package akka.cluster.pubsub

import akka.Done
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, DeadLetter, Deploy, ExtendedActorSystem, Props}
import akka.cluster.Cluster
import akka.cluster.pubsub.DistributedPubSubMediator.Internal.Topic
import akka.cluster.pubsub.DistributedPubSubMediator.SendToAll
import akka.dispatch.Dispatchers
import akka.routing.RoutingLogic
import akka.stream.scaladsl.{Sink, Source, SourceQueueWithComplete}
import akka.stream.{Materializer, OverflowStrategy, QueueOfferResult}
import cats.Id
import com.evolutiongaming.cluster.pubsub.{PubSub, PubSubMsg}
import com.evolutiongaming.serialization.{SerializedMsg, SerializedMsgExt}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
  * Override to be able to serialize/deserialize msgs on PubSub level to offload akka remoting transport.
  * Serialization performed if there are remote subscribers, once per msg, disregarding the number of subscribers
  * Deserialization performed on every remote node once per message, disregarding the number of subscribers as well.
  */
class DistributedPubSubMediatorSerializing(
  settings: DistributedPubSubSettings,
  serialize: String => Boolean,
  metrics: PubSub.Metrics[Id]
) extends DistributedPubSubMediator(settings) with DistributedPubSubMediatorSerializing.StreamHelper {

  import DistributedPubSubMediatorSerializing._
  import context.dispatcher

  private val selfAddress = Cluster(context.system).selfAddress
  private val serializedMsgExt = SerializedMsgExt(context.system)

  private lazy val queue = {
    val strategy = OverflowStrategy.backpressure
    val maxSubstreams = Parallelism
    val bufferSize = Int.MaxValue
    Source
      .queue[SerializationTask](bufferSize, strategy)
      .groupBy(maxSubstreams, elem => math.abs(elem.topic.hashCode % maxSubstreams))
      .buffer(bufferSize, strategy)
      .mapAsync(1)(_.serialize)
      .to(selfSink)
      .run()(Materializer(context.system))
  }

  override def publish(path: String, msg: Any, allButSelf: Boolean) = {

    val refs = for {
      (address, bucket) <- registry
      if !(allButSelf && address == selfAddress) // if we should skip sender() node and current address == self address => skip
      valueHolder <- bucket.content.get(path)
      ref <- valueHolder.ref
    } yield (ref, address)

    def forward(): Unit = refs foreach { case (ref, _) => ref forward msg }

    if (refs.isEmpty) {
      ignoreOrSendToDeadLetters(msg)
    } else if (refs exists { case (_, address) => address != selfAddress }) {

      val topic = toTopic(path)

      def serializeAndForward(msg: AnyRef): Unit = {
        val sender = this.sender()

        def result(msg: AnyRef) = {
          val sendToAll = SendToAll(path, msg, allButSelf = true)
          (sendToAll, sender)
        }

        val serialize = Future {
          val serializedMsg = serializedMsgExt.toMsg(msg)
          metrics.toBytes(topic, serializedMsg.bytes.length)
          val pubSubMsg = PubSubMsg(serializedMsg, System.currentTimeMillis())
          result(pubSubMsg)
        } recover { case failure =>
          log.error(failure, s"Failed to serialize ${ msg.getClass.getName } at $topic, sending as is")
          result(msg)
        }

        val task = SerializationTask(topic, serialize)
        queue.offerAndLog(task, s"Failed to enqueue ${ msg.getClass.getName } at $topic")

        refs foreach { case (ref, address) => if (address == selfAddress) ref forward msg }
      }

      msg match {
        case _: PubSubMsg                  => forward()
        case _: SerializedMsg              => forward()
        case x: AnyRef if serialize(topic) => serializeAndForward(x)
        case _                             => forward()
      }
    } else {
      forward()
    }
  }

  override def newTopicActor(encTopic: String): ActorRef = {
    val props = TopicSerializing.props(settings, metrics)
    val ref = context.actorOf(props, name = encTopic)
    registerTopic(ref)
    ref
  }

  private def ignoreOrSendToDeadLetters(msg: Any) =
    if (settings.sendToDeadLettersWhenNoSubscribers) context.system.deadLetters ! DeadLetter(msg, sender(), context.self)

  case class SerializationTask(topic: String, serialize: Future[(SendToAll, ActorRef)])
}

object DistributedPubSubMediatorSerializing {

  private val Parallelism = (Runtime.getRuntime.availableProcessors max 1) * 2

  def props(
    settings: DistributedPubSubSettings,
    serialize: String => Boolean,
    metrics: PubSub.Metrics[Id]
  ): Props = {

    def actor = new DistributedPubSubMediatorSerializing(settings, serialize, metrics)

    Props(actor).withDeploy(Deploy.local)
  }

  def apply(
    system: ActorSystem,
    serialize: String => Boolean,
    metrics: PubSub.Metrics[Id],
    name: String = "distributedPubSubMediatorOverride"
  ): ActorRef = {

    val settings = DistributedPubSubSettings(system)
    val dispatcher = system.settings.config.getString("akka.cluster.pub-sub.use-dispatcher") match {
      case "" => Dispatchers.DefaultDispatcherId
      case id => id
    }

    val props = this.props(settings, serialize, metrics).withDispatcher(dispatcher)
    system.asInstanceOf[ExtendedActorSystem].systemActorOf(props, name)
  }


  private def toTopic(path: String) = path.split("/").last


  class TopicSerializing(
    emptyTimeToLive: FiniteDuration,
    routingLogic: RoutingLogic,
    metrics: PubSub.Metrics[Id],
  ) extends Topic(emptyTimeToLive, routingLogic) with ActorLogging with StreamHelper {

    import context.dispatcher

    private val serializedMsgExt = SerializedMsgExt(context.system)

    private val topic = toTopic(self.path.toStringWithoutAddress)

    private lazy val queue = {
      Source
        .queue[Future[Option[(AnyRef, ActorRef)]]](Int.MaxValue, OverflowStrategy.backpressure)
        .mapAsync(1)(identity)
        .collect { case Some(x) => x }
        .to(selfSink)
        .run()(Materializer(context.system))
    }

    override def receive = rcvBytes orElse super.receive

    def rcvBytes: Receive = {
      case PubSubMsg(serializedMsg, timestamp) => onBytes(serializedMsg, timestamp)
    }

    private def onBytes(serializedMsg: SerializedMsg, timestamp: Long) = {
      val latency = System.currentTimeMillis() - timestamp
      metrics.latency(topic, latency)
      val sender = this.sender()
      val deserialize = Future {
        val length = serializedMsg.bytes.length.toLong
        metrics.fromBytes(topic, length)
        val msg = serializedMsgExt.fromMsg(serializedMsg).get
        val result = (msg, sender)
        Some(result)
      } recover { case failure =>
        log.error(failure, s"Failed to deserialize msg at $topic")
        None
      }

      queue.offerAndLog(deserialize, s"Failed to enqueue msg at $topic")
    }
  }

  object TopicSerializing {

    def props(settings: DistributedPubSubSettings, metrics: PubSub.Metrics[Id]): Props = {
      def actor = new TopicSerializing(settings.removedTimeToLive, settings.routingLogic, metrics)

      Props(actor)
    }
  }


  trait StreamHelper extends Actor with ActorLogging {

    def selfSink[A]: Sink[(A, ActorRef), Future[Done]] = {
      Sink.foreach[(A, ActorRef)] { case (x, sender) => self.tell(x, sender) }
    }

    implicit class SourceQueueWithCompleteOps[A](self: SourceQueueWithComplete[A]) {

      def offerAndLog(elem: A, errorMsg: => String)(implicit ec: ExecutionContext): Unit = {
        self.offer(elem) onComplete {
          case Success(QueueOfferResult.Enqueued)         =>
          case Success(QueueOfferResult.Failure(failure)) => log.error(failure, errorMsg)
          case Success(failure)                           => log.error(s"$errorMsg $failure")
          case Failure(failure)                           => log.error(failure, s"$errorMsg $failure")
        }
      }
    }
  }
}