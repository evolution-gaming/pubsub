package akka.cluster.pubsub

import akka.Done
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, DeadLetter, Deploy, ExtendedActorSystem, Props}
import akka.cluster.Cluster
import akka.cluster.pubsub.DistributedPubSubMediator.Internal.Topic
import akka.cluster.pubsub.DistributedPubSubMediator.SendToAll
import akka.dispatch.Dispatchers
import akka.routing.RoutingLogic
import akka.serialization.{SerializationExtension, SerializerWithStringManifest}
import akka.stream.scaladsl.{Sink, Source, SourceQueueWithComplete}
import akka.stream.{ActorMaterializer, OverflowStrategy, QueueOfferResult}
import com.codahale.metrics.MetricRegistry
import com.evolutiongaming.metrics.MetricName
import com.evolutiongaming.safeakka.actor.Sender

import scala.compat.Platform
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
  metricRegistry: MetricRegistry) extends DistributedPubSubMediator(settings)
  with DistributedPubSubMediatorSerializing.StreamHelper {

  import DistributedPubSubMediatorSerializing._
  import context.dispatcher

  private val selfAddress = Cluster(context.system).selfAddress
  private val serialization = SerializationExtension(context.system)
  private lazy val queue = {
    val strategy = OverflowStrategy.backpressure
    val maxSubstreams = Parallelism
    val bufferSize = Int.MaxValue
    Source
      .queue[SerializationTask](bufferSize, strategy)
      .groupBy(maxSubstreams, elem => math.abs(elem.topic.hashCode) % maxSubstreams)
      .buffer(bufferSize, strategy)
      .mapAsync(Parallelism)(_.serialize)
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
          val serializer = serialization.findSerializerFor(msg)
          val bytes = serializer.toBinary(msg)
          val name = MetricName(topic)
          metricRegistry.meter(s"$name.toBytes").mark(bytes.length.toLong)
          val manifest = serializer match {
            case serializer: SerializerWithStringManifest => serializer.manifest(msg)
            case _ if serializer.includeManifest          => msg.getClass.getName
            case _                                        => ""
          }
          val timestamp = Platform.currentTime
          val msgBytes = MsgBytes(serializer.identifier, timestamp, manifest, bytes)
          result(msgBytes)
        } recover { case failure =>
          log.error(s"Failed to serialize ${ msg.getClass.getName } at $topic, sending as is", failure)
          result(msg)
        }

        val task = SerializationTask(topic, serialize)
        queue.offerAndLog(task, s"Failed to enqueue ${ msg.getClass.getName } at $topic")

        refs foreach { case (ref, address) => if (address == selfAddress) ref forward msg }
      }

      msg match {
        case _: MsgBytes                     => forward()
        case msg: AnyRef if serialize(topic) => serializeAndForward(msg)
        case _                               => forward()
      }
    } else {
      forward()
    }
  }

  override def newTopicActor(encTopic: String): ActorRef = {
    val props = TopicSerializing.props(settings, metricRegistry)
    val ref = context.actorOf(props, name = encTopic)
    registerTopic(ref)
    ref
  }

  private def ignoreOrSendToDeadLetters(msg: Any) =
    if (settings.sendToDeadLettersWhenNoSubscribers) context.system.deadLetters ! DeadLetter(msg, sender(), context.self)

  case class SerializationTask(topic: String, serialize: Future[(SendToAll, Sender)])
}

object DistributedPubSubMediatorSerializing {

  private val Parallelism = Runtime.getRuntime.availableProcessors

  def props(
    settings: DistributedPubSubSettings,
    serialize: String => Boolean,
    metricRegistry: MetricRegistry): Props = {

    def actor = new DistributedPubSubMediatorSerializing(settings, serialize, metricRegistry)

    Props(actor).withDeploy(Deploy.local)
  }

  def apply(
    system: ActorSystem,
    serialize: String => Boolean,
    metricRegistry: MetricRegistry,
    name: String = "distributedPubSubMediatorOverride"): ActorRef = {

    val settings = DistributedPubSubSettings(system)
    val dispatcher = system.settings.config.getString("akka.cluster.pub-sub.use-dispatcher") match {
      case "" => Dispatchers.DefaultDispatcherId
      case id => id
    }

    val props = this.props(settings, serialize, metricRegistry).withDispatcher(dispatcher)
    system.asInstanceOf[ExtendedActorSystem].systemActorOf(props, name)
  }


  private def toTopic(path: String) = path.split("/").last


  class TopicSerializing(
    emptyTimeToLive: FiniteDuration,
    routingLogic: RoutingLogic,
    metricRegistry: MetricRegistry) extends Topic(emptyTimeToLive, routingLogic) with ActorLogging with StreamHelper {

    import context.dispatcher

    private val serialization = SerializationExtension(context.system)
    private val topic = toTopic(self.path.toStringWithoutAddress)

    private lazy val queue = {
      Source
        .queue[Future[Option[(AnyRef, Sender)]]](Int.MaxValue, OverflowStrategy.backpressure)
        .mapAsync(Parallelism)(identity)
        .collect { case Some(x) => x }
        .to(selfSink)
        .run()(ActorMaterializer(namePrefix = Some("deserialization")))
    }

    def rcvBytes: Receive = {
      case MsgBytes(identifier, timestamp, manifest, bytes) =>
        val latency = Platform.currentTime - timestamp
        val name = MetricName(topic)
        metricRegistry.histogram(s"$name.latency").update(latency)

        val sender = this.sender()
        val deserialize = Future {
          metricRegistry.meter(s"$name.fromBytes").mark(bytes.length.toLong)
          val msg =
            if (manifest.nonEmpty) serialization.deserialize(bytes, identifier, manifest).get
            else serialization.deserialize(bytes, identifier, None).get
          val result = (msg, sender)
          Some(result)
        } recover { case failure =>
          log.error(failure, s"Failed to deserialize msg at $topic")
          None
        }

        queue.offerAndLog(deserialize, s"Failed to enqueue msg at $topic")
    }

    override def receive = rcvBytes orElse super.receive
  }

  object TopicSerializing {
    def props(settings: DistributedPubSubSettings, metricRegistry: MetricRegistry): Props = {
      def actor = new TopicSerializing(settings.removedTimeToLive, settings.routingLogic, metricRegistry)

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
          case Failure(failure)                           => log.error(failure, s"$errorMsg $failure")
        }
      }
    }
  }
}

case class MsgBytes(identifier: Int, timestamp: Long, manifest: String, bytes: Array[Byte])
