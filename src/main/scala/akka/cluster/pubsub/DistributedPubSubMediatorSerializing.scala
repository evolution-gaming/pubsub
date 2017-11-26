package akka.cluster.pubsub

import akka.Done
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, DeadLetter, Deploy, ExtendedActorSystem, Props}
import akka.cluster.Cluster
import akka.cluster.pubsub.DistributedPubSubMediator.Internal.Topic
import akka.cluster.pubsub.DistributedPubSubMediator.SendToAll
import akka.dispatch.Dispatchers
import akka.routing.RoutingLogic
import akka.serialization.SerializationExtension
import akka.stream.scaladsl.{Sink, Source, SourceQueueWithComplete}
import akka.stream.{ActorMaterializer, OverflowStrategy, QueueOfferResult}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

/**
  * Override to be able to serialize/deserialize msgs on PubSub level to offload akka remoting transport.
  * Serialization performed if there are remote subscribers, once per msg, disregarding the number of subscribers
  * Deserialization performed on every remote node once per message, disregarding the number of subscribers as well.
  */
class DistributedPubSubMediatorSerializing(
  settings: DistributedPubSubSettings,
  serialize: String => Boolean) extends DistributedPubSubMediator(settings)
  with DistributedPubSubMediatorSerializing.StreamHelper {

  import DistributedPubSubMediatorSerializing._
  import context.dispatcher

  private val selfAddress = Cluster(context.system).selfAddress
  private lazy val serialization = SerializationExtension(context.system)
  private lazy val queue = {
    val strategy = OverflowStrategy.backpressure
    val maxSubstreams = Parallelism * 3
    Source
      .queue[SerializationTask](Int.MaxValue, strategy)
      .groupBy(maxSubstreams, elem => math.abs(elem.topic.hashCode) % maxSubstreams)
      .buffer(Int.MaxValue, strategy)
      .mapAsync(Parallelism)(x => Future(x.serialize() -> x.sender))
      .to(selfSink)
      .run()(ActorMaterializer(namePrefix = Some("serialization")))
  }

  override def publish(path: String, msg: Any, allButSelf: Boolean) = {

    val refs = for {
      (address, bucket) <- registry
      if !(allButSelf && address == selfAddress) // if we should skip sender() node and current address == self address => skip
      valueHolder <- bucket.content.get(path)
      ref <- valueHolder.ref
    } yield (ref, address)

    def forwardMsg() = refs foreach { case (ref, _) => ref forward msg }

    if (refs.isEmpty) {
      ignoreOrSendToDeadLetters(msg)
    } else if (refs exists { case (_, address) => address != selfAddress }) {

      val topic = toTopic(path)

      val msgAndSerializer = msg match {
        case _: PublishBytes                 => None
        case msg: AnyRef if serialize(topic) => Some(msg -> serialization.findSerializerFor(msg))
        case _                               => None
      }

      msgAndSerializer match {
        case None                    => forwardMsg()
        case Some((msg, serializer)) =>
          val serialize = () => {
            val msgOrBytes = try {
              val bytes = serializer.toBinary(msg)
              PublishBytes(serializer.identifier, bytes)
            } catch {
              case NonFatal(failure) =>
                log.error(s"Failed to serialize ${ msg.getClass.getName } at $topic, sending as is", failure)
                msg
            }

            SendToAll(path, msgOrBytes, allButSelf = true)
          }

          def errorMsg = s"Failed to enqueue ${ msg.getClass.getName } at $topic"

          queue.offerAndLog(SerializationTask(topic, sender(), serialize), errorMsg)

          refs foreach { case (ref, address) => if (address == selfAddress) ref forward msg }
      }
    } else {
      forwardMsg()
    }
  }

  override def newTopicActor(encTopic: String) = {
    val props = TopicSerializing.props(settings)
    val ref = context.actorOf(props, name = encTopic)
    registerTopic(ref)
    ref
  }

  private def ignoreOrSendToDeadLetters(msg: Any) =
    if (settings.sendToDeadLettersWhenNoSubscribers) context.system.deadLetters ! DeadLetter(msg, sender(), context.self)

  case class SerializationTask(topic: String, sender: ActorRef, serialize: () => SendToAll)
}

object DistributedPubSubMediatorSerializing {

  private val Parallelism = Runtime.getRuntime.availableProcessors

  def props(
    settings: DistributedPubSubSettings,
    serialize: String => Boolean): Props = {

    def actor = new DistributedPubSubMediatorSerializing(settings, serialize)
    Props(actor).withDeploy(Deploy.local)
  }

  def apply(
    system: ActorSystem,
    serialize: String => Boolean,
    name: String = "distributedPubSubMediatorOverride"): ActorRef = {

    val settings = DistributedPubSubSettings(system)
    val dispatcher = system.settings.config.getString("akka.cluster.pub-sub.use-dispatcher") match {
      case "" => Dispatchers.DefaultDispatcherId
      case id => id
    }

    val props = this.props(settings, serialize).withDispatcher(dispatcher)
    system.asInstanceOf[ExtendedActorSystem].systemActorOf(props, name)
  }


  private def toTopic(path: String) = path.split("/").last


  class TopicSerializing(emptyTimeToLive: FiniteDuration, routingLogic: RoutingLogic)
    extends Topic(emptyTimeToLive, routingLogic) with ActorLogging with StreamHelper {

    import context.dispatcher

    private lazy val serialization = SerializationExtension(context.system)
    private lazy val queue = {
      Source
        .queue[DeserializationTask](Int.MaxValue, OverflowStrategy.backpressure)
        .mapAsync(Parallelism)(x => Future(x.deserialize() map { _ -> x.sender }))
        .collect { case Some(x) => x }
        .to(selfSink)
        .run()(ActorMaterializer(namePrefix = Some("deserialization")))
    }

    def rcvBytes: Receive = {
      case PublishBytes(identifier, bytes) =>

        def topic = toTopic(self.path.toStringWithoutAddress)

        val deserialize = () => try {
          val bytesCopy = CopyArray(bytes) // otherwise kryo corrupts original bytes array
          serialization.deserialize[AnyRef](bytesCopy, identifier, None) match {
            case Success(msg)     => Some(msg)
            case Failure(failure) =>
              log.error(failure, s"Failed to deserialize msg at $topic")
              None
          }
        } catch {
          case NonFatal(failure) =>
            log.error(failure, s"Failed to deserialize msg at $topic")
            None
        }

        def errorMsg = s"Failed to enqueue msg at $topic"

        queue.offerAndLog(DeserializationTask(sender(), deserialize), errorMsg)
    }

    override def receive = rcvBytes orElse super.receive

    private case class DeserializationTask(sender: ActorRef, deserialize: () => Option[AnyRef])
  }

  object TopicSerializing {
    def props(settings: DistributedPubSubSettings): Props = {
      def actor = new TopicSerializing(settings.removedTimeToLive, settings.routingLogic)
      Props(actor)
    }
  }


  trait StreamHelper extends Actor with ActorLogging {

    def selfSink[T]: Sink[(T, ActorRef), Future[Done]] = {
      Sink.foreach[(T, ActorRef)] { case (x, sender) => self.tell(x, sender) }
    }

    implicit class SourceQueueWithCompleteOps[T](self: SourceQueueWithComplete[T]) {

      def offerAndLog(elem: T, errorMsg: => String)(implicit ec: ExecutionContext): Unit = {
        self.offer(elem) onComplete {
          case Success(QueueOfferResult.Enqueued)         =>
          case Success(QueueOfferResult.Failure(failure)) => log.error(failure, errorMsg)
          case Success(failure)                           => log.error(s"$errorMsg $failure")
          case Failure(failure)                           => log.error(failure, errorMsg)
        }
      }
    }
  }
}

case class PublishBytes(identifier: Int, bytes: Array[Byte])
