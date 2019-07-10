package com.evolutiongaming.cluster.pubsub

import akka.actor.{ActorPath, ActorRef, ActorRefFactory, ActorSystem}
import akka.cluster.Cluster
import akka.cluster.pubsub.{DistributedPubSubMediatorSerializing, DistributedPubSubMediator => Mediator}
import akka.pattern._
import akka.util.Timeout
import com.codahale.metrics.MetricRegistry
import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.metrics.MetricName
import com.evolutiongaming.safeakka.actor._
import com.evolutiongaming.serialization.ToBytesAble

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.util.control.NonFatal

trait PubSub {
  import PubSub._

  def publish[A: Topic : ToBytes](
    msg: A,
    sender: Option[ActorRef] = None,
    sendToEachGroup: Boolean = false
  ): Unit

  def subscribe[A: Topic : FromBytes : ClassTag](
    group: Option[String] = None)(
    onMsg: OnMsg[A]
  ): Unsubscribe

  def topics(timeout: FiniteDuration = 3.seconds): Future[Set[String]]
}

object PubSub {

  type OnMsg[-A] = (A, ActorPath) => Unit

  type Unsubscribe = () => Unit

  object Unsubscribe {
    val Empty: Unsubscribe = () => ()
  }


  def empty: PubSub = new PubSub {

    def publish[A: Topic : ToBytes](msg: A, sender: Option[ActorRef] = None, sendToEachGroup: Boolean = false) = {}

    def subscribe[A: Topic : FromBytes : ClassTag](group: Option[String] = None)(onMsg: OnMsg[A]) = {
      Unsubscribe.Empty
    }

    def topics(timeout: FiniteDuration) = Future.successful(Set.empty)
  }


  def apply(
    system: ActorSystem,
    metrics: Metrics,
    serialize: String => Boolean = _ => false
  ): PubSub = {

    val ref = if (system hasExtension Cluster) {
      DistributedPubSubMediatorSerializing(system, serialize, metrics)
    } else {
      system.actorOf(LocalPubSub.props)
    }
    val log = ActorLog(system, classOf[PubSub])
    apply(ref, log, system)
  }


  def apply(pubSub: ActorRef, log: ActorLog, factory: ActorRefFactory): PubSub = {

    def publishRaw[A](msg: A, sender: Option[ActorRef], sendToEachGroup: Boolean)
      (implicit topic: Topic[A]): Unit = {

      val publish = Mediator.Publish(topic.name, msg, sendToEachGroup)
      log.debug(s"publish $publish")
      pubSub.tell(publish, sender getOrElse ActorRef.noSender)
    }

    def subscribeRaw[A](
      group: Option[String])(
      onMsg: OnMsg[A])(implicit
      topic: Topic[A],
      tag: ClassTag[A]
    ): Unsubscribe = {

      import Subscription.In

      def subscribe(log: ActorLog) = {

        val setup: SetupActor[In[A]] = ctx => {
          val behavior = Behavior.stateless[In[A]] {
            case Signal.Msg(msg, sender) => msg match {
              case In.Subscribed   => log.debug(s"subscribed ${ ctx.self }")
              case In.Unsubscribed => log.debug(s"unsubscribed ${ ctx.self }")
              case In.Msg(msg)     =>
                log.debug(s"receive $msg")
                try onMsg(msg, sender.path) catch {
                  case NonFatal(failure) => log.error(s"failure $failure", failure)
                }
            }
            case Signal.PostStop         =>
              val unsubscribe = Mediator.Unsubscribe(topic.name, group, ctx.self)
              log.debug(s"unsubscribe $unsubscribe")
              pubSub.tell(unsubscribe, ctx.self)
            case _                       =>
          }

          val logListener = ActorLog(ctx.system, PubSub.getClass) prefixed topic.name
          (behavior, logListener)
        }

        val ref = SafeActorRef(setup)(factory, Subscription.In.unapplyOf[A])
        val subscribe = Mediator.Subscribe(topic.name, group, ref.unsafe)
        log.debug(s"subscribe $subscribe")
        pubSub.tell(subscribe, ref.unsafe)
        () => factory.stop(ref.unsafe)
      }

      subscribe(log.prefixed(topic.name))
    }

    new PubSub {

      def publish[A](msg: A, sender: Option[ActorRef] = None, sendToEachGroup: Boolean = false)
        (implicit topic: Topic[A], toBytes: ToBytes[A]): Unit = {

        val toBytesAble = ToBytesAble(msg)(toBytes.apply)
        implicit val topicFinal = Topic[ToBytesAble](topic.name)
        publishRaw(toBytesAble, sender, sendToEachGroup)
      }

      def subscribe[A](group: Option[String] = None)(onMsg: OnMsg[A])
        (implicit topic: Topic[A], fromBytes: FromBytes[A], tag: ClassTag[A]): Unsubscribe = {

        implicit val topicFinal = Topic[ToBytesAble](topic.name)

        val onToBytesAble: OnMsg[ToBytesAble] = (msg: ToBytesAble, sender: ActorPath) => {
          msg match {
            case ToBytesAble.Bytes(bytes)  => onMsg(fromBytes(bytes), sender)
            case ToBytesAble.Raw(tag(msg)) => onMsg(msg, sender)
            case ToBytesAble.Raw(msg)      => log.warn(s"$topic: receive unexpected $msg")
          }
        }

        subscribeRaw(group)(onToBytesAble)
      }

      def topics(timeout: FiniteDuration): Future[Set[String]] = {
        implicit val executor = CurrentThreadExecutionContext
        implicit val timeout1 = Timeout(timeout)
        pubSub.ask(Mediator.GetTopics).mapTo[Mediator.CurrentTopics] map { _.topics }
      }
    }
  }


  def proxy(ref: ActorRef): PubSub = new PubSub {

    def publish[A: Topic : ToBytes](msg: A, sender: Option[ActorRef] = None, sendToEachGroup: Boolean = false) = {
      ref.tell(msg, sender getOrElse ActorRef.noSender)
    }

    def subscribe[A: Topic : FromBytes : ClassTag](group: Option[String] = None)(onMsg: OnMsg[A]) = {
      Unsubscribe.Empty
    }

    def topics(timeout: FiniteDuration) = {
      implicit val executor = CurrentThreadExecutionContext
      implicit val timeout1 = Timeout(timeout)
      ref.ask(Mediator.GetTopics).mapTo[Mediator.CurrentTopics] map { _.topics }
    }
  }


  object Subscription {

    sealed trait In[+A]

    object In {

      def unapplyOf[A](implicit tag: ClassTag[A]): Unapply[In[A]] = Unapply.pf[In[A]] {
        case In.Msg(tag(x))             => In.Msg(x)
        case _: Mediator.SubscribeAck   => In.Subscribed
        case _: Mediator.UnsubscribeAck => In.Unsubscribed
        case In.Subscribed              => In.Subscribed
        case In.Unsubscribed            => In.Unsubscribed
        case tag(x)                     => In.Msg(x)
      }

      final case class Msg[+A](msg: A) extends In[A]
      case object Subscribed extends In[Nothing]
      case object Unsubscribed extends In[Nothing]
    }
  }


  trait Metrics {

    def subscribe(topic: String): Unit

    def unsubscribe(topic: String): Unit

    def publish(topic: String): Unit

    def toBytes(topic: String, size: Long): Unit

    def fromBytes(topic: String, size: Long): Unit

    def latency(topic: String, latencyMs: Long): Unit
  }

  object Metrics {

    def empty: Metrics = new Metrics {

      def subscribe(topic: String) = {}

      def unsubscribe(topic: String) = {}

      def publish(topic: String) = {}

      def toBytes(topic: String, size: Long) = {}

      def fromBytes(topic: String, size: Long) = {}

      def latency(topic: String, latencyMs: Long) = {}
    }
    

    def codahale(registry: MetricRegistry): Metrics = {

      def nameOf(topic: String) = MetricName(topic)

      val toBytesMeter = registry.meter("toBytes")

      val fromBytesMeter = registry.meter("fromBytes")

      val latencyHistogram = registry.histogram("latency")

      new Metrics {

        def subscribe(topic: String) = {
          val name = nameOf(topic)
          registry.counter(s"$name.subscriptions").inc()
        }

        def unsubscribe(topic: String) = {
          val name = nameOf(topic)
          registry.counter(s"$name.subscriptions").dec()
        }

        def publish(topic: String) = {
          val name = nameOf(topic)
          registry.meter(s"$name.publish").mark()
        }

        def toBytes(topic: String, size: Long) = {
          val name = nameOf(topic)
          toBytesMeter.mark(size)
          registry.meter(s"$name.toBytes").mark(size)
        }

        def fromBytes(topic: String, size: Long) = {
          val name = nameOf(topic)
          fromBytesMeter.mark(size)
          registry.meter(s"$name.fromBytes").mark(size)
        }

        def latency(topic: String, latencyMs: Long) = {
          val name = nameOf(topic)
          latencyHistogram.update(latencyMs)
          registry.histogram(s"$name.latency").update(latencyMs)
        }
      }
    }
  }


  implicit class PubSubOps(val self: PubSub) extends AnyVal {

    def withOptimiseSubscribe(optimiseSubscribe: OptimiseSubscribe): PubSub = {
      new PubSub {

        def publish[A: Topic : ToBytes](msg: A, sender: Option[Sender], sendToEachGroup: Boolean) = {
          self.publish(msg, sender, sendToEachGroup)
        }

        def subscribe[A: Topic : FromBytes : ClassTag](group: Option[String])(onMsg: OnMsg[A]) = {
          optimiseSubscribe[A](onMsg) { onMsg =>
            self.subscribe[A](group)(onMsg)
          }
        }

        def topics(timeout: FiniteDuration) = self.topics(timeout)
      }
    }


    def withMetrics(pubSub: PubSub, metrics: Metrics): PubSub = {

      new PubSub {

        def publish[A](
          msg: A,
          sender: Option[ActorRef] = None,
          sendToEachGroup: Boolean = false)(implicit
          topic: Topic[A],
          toBytes: ToBytes[A]
        ) = {
          metrics.publish(topic.name)
          pubSub.publish(msg, sender, sendToEachGroup)
        }

        def subscribe[A](
          group: Option[String] = None)(
          onMsg: OnMsg[A])(implicit
          topic: Topic[A],
          fromBytes: FromBytes[A],
          tag: ClassTag[A]
        ) = {
          val unsubscribe = pubSub.subscribe(group)(onMsg)
          metrics.subscribe(topic.name)
          () => {
            metrics.unsubscribe(topic.name)
            unsubscribe()
          }
        }

        def topics(timeout: FiniteDuration) = pubSub.topics(timeout)
      }
    }
  }
}
