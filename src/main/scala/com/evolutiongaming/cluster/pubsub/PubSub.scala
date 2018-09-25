package com.evolutiongaming.cluster.pubsub

import akka.actor.{ActorRef, ActorRefFactory, ActorSystem}
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
  import PubSub.{OnMsg, Unsubscribe}

  def publishAny[T: Topic](msg: T, sender: Option[ActorRef] = None, sendToEachGroup: Boolean = false): Unit

  def subscribeAny[T: Topic: ClassTag](ref: ActorRef, group: Option[String] = None): Unsubscribe

  def subscribeAny[T: Topic: ClassTag](factory: ActorRefFactory)(onMsg: OnMsg[T]): Unsubscribe

  def subscribeAny[T: Topic: ClassTag](factory: ActorRefFactory, group: Option[String])(onMsg: OnMsg[T]): Unsubscribe

  def publish[T: Topic: ToBytes](msg: T, sender: Option[ActorRef] = None, sendToEachGroup: Boolean = false): Unit

  def subscribe[T: Topic: FromBytes: ClassTag](factory: ActorRefFactory, group: Option[String] = None)(onMsg: OnMsg[T]): Unsubscribe

  def unsubscribe[T: Topic](ref: ActorRef, group: Option[String] = None): Unit

  def topics(timeout: FiniteDuration = 3.seconds): Future[Set[String]]
}

object PubSub {

  type OnMsg[-T] = (T, Sender) => Unit

  type Unsubscribe = () => Unit

  object Unsubscribe {
    val Empty: Unsubscribe = () => ()
  }


  def apply(
    system: ActorSystem,
    registry: MetricRegistry,
    serialize: String => Boolean = _ => false): PubSub = {

    val ref = if (system hasExtension Cluster) {
      DistributedPubSubMediatorSerializing(system, serialize, registry)
    } else {
      system.actorOf(LocalPubSub.props)
    }
    val log = ActorLog(system, classOf[PubSub])
    apply(ref, log)
  }


  def apply(pubSub: ActorRef, log: ActorLog): PubSub = new PubSub {

    def publishAny[T](msg: T, sender: Option[ActorRef], sendToEachGroup: Boolean = false)
      (implicit topic: Topic[T]): Unit = {

      val publish = Mediator.Publish(topic.name, msg, sendToEachGroup)
      log.debug(s"publish $publish")
      pubSub.tell(publish, sender getOrElse ActorRef.noSender)
    }

    def subscribeAny[T](factory: ActorRefFactory)(onMsg: OnMsg[T])
      (implicit topic: Topic[T], tag: ClassTag[T]): Unsubscribe = {

      subscribeAny(factory, None)(onMsg)
    }

    def subscribeAny[T](ref: ActorRef, group: Option[String] = None)
      (implicit topic: Topic[T], tag: ClassTag[T]): Unsubscribe = {

      val subscribe = Mediator.Subscribe(topic.name, group, ref)
      log.debug(s"subscribe $subscribe")
      pubSub.tell(subscribe, ref)
      () => unsubscribe(ref, group)
    }

    def subscribeAny[T](factory: ActorRefFactory, group: Option[String])(onMsg: OnMsg[T])
      (implicit topic: Topic[T], tag: ClassTag[T]): Unsubscribe = {

      import Subscription.In

      def subscribe(log: ActorLog) = {

        val setup: SetupActor[In[T]] = ctx => {
          val behavior = Behavior.stateless[In[T]] {
            case Signal.Msg(msg, sender) => msg match {
              case In.Subscribed => log.debug(s"subscribed ${ ctx.self }")
              case In.Msg(msg)   =>
                log.debug(s"receive $msg")
                try onMsg(msg, sender) catch {
                  case NonFatal(failure) => log.error(s"failure $failure", failure)
                }
            }
            case Signal.PostStop         => unsubscribe(ctx.self)
            case _                       =>
          }

          val logListener = ActorLog(ctx.system, PubSub.getClass) prefixed topic.name
          (behavior, logListener)
        }

        val unapply = Unapply.pf[In[T]] {
          case _: Mediator.SubscribeAck => In.Subscribed
          case In.Msg(tag(x))           => In.Msg(x)
          case In.Subscribed            => In.Subscribed
          case tag(x)                   => In.Msg(x)
        }

        val ref = SafeActorRef(setup)(factory, unapply)
        subscribeAny(ref.unsafe, group)
      }

      subscribe(log.prefixed(topic.name))
    }

    def publish[T](msg: T, sender: Option[ActorRef] = None, sendToEachGroup: Boolean = false)
      (implicit topic: Topic[T], toBytes: ToBytes[T]): Unit = {

      val toBytesAble = ToBytesAble(msg)(toBytes.apply)
      implicit val topicFinal = Topic[ToBytesAble](topic.name)
      publishAny(toBytesAble, sender, sendToEachGroup)
    }

    def subscribe[T](factory: ActorRefFactory, group: Option[String] = None)(onMsg: OnMsg[T])
      (implicit topic: Topic[T], fromBytes: FromBytes[T], tag: ClassTag[T]): Unsubscribe = {

      implicit val topicFinal = Topic[ToBytesAble](topic.name)

      val onToBytesAble: OnMsg[ToBytesAble]  = (msg: ToBytesAble, sender: ActorRef) => {
        msg match {
          case ToBytesAble.Bytes(bytes)     => onMsg(fromBytes(bytes), sender)
          case ToBytesAble.Raw(tag(msg), _) => onMsg(msg, sender)
          case ToBytesAble.Raw(msg, _)      => log.warn(s"$topic: receive unexpected $msg")
        }
      }

      subscribeAny(factory, group)(onToBytesAble)
    }

    def unsubscribe[T](ref: ActorRef, group: Option[String] = None)(implicit topic: Topic[T]): Unit = {
      val unsubscribe = Mediator.Unsubscribe(topic.name, group, ref)
      log.debug(s"unsubscribe $unsubscribe")
      pubSub.tell(unsubscribe, ref)
    }

    def topics(timeout: FiniteDuration): Future[Set[String]] = {
      implicit val ec = CurrentThreadExecutionContext
      implicit val t = Timeout(timeout)
      pubSub.ask(Mediator.GetTopics).mapTo[Mediator.CurrentTopics] map { _.topics }
    }
  }


  def apply(pubSub: PubSub, registry: MetricRegistry): PubSub = new PubSub {

    def publishAny[T](msg: T, sender: Option[ActorRef], sendToEachGroup: Boolean = false)
      (implicit topic: Topic[T]): Unit = {

      markPublish(topic)
      pubSub.publishAny(msg, sender, sendToEachGroup)
    }

    def subscribeAny[T](ref: ActorRef, group: Option[String])
      (implicit topic: Topic[T], tag: ClassTag[T]): Unsubscribe = {

      incSubscriptions(topic)
      pubSub.subscribeAny(ref, group)
    }

    def subscribeAny[T](factory: ActorRefFactory)(onMsg: OnMsg[T])
      (implicit topic: Topic[T], tag: ClassTag[T]): Unsubscribe = {

      incSubscriptions(topic)
      pubSub.subscribeAny(factory)(onMsg)
    }

    def subscribeAny[T](factory: ActorRefFactory, group: Option[String])(onMsg: OnMsg[T])
      (implicit topic: Topic[T], tag: ClassTag[T]): Unsubscribe = {

      incSubscriptions(topic)
      pubSub.subscribeAny(factory, group)(onMsg)
    }

    def publish[T](msg: T, sender: Option[ActorRef] = None, sendToEachGroup: Boolean = false)
      (implicit topic: Topic[T], toBytes: ToBytes[T]): Unit = {

      markPublish(topic)
      pubSub.publish(msg, sender, sendToEachGroup)
    }

    def subscribe[T](factory: ActorRefFactory, group: Option[String] = None)(onMsg: OnMsg[T])
      (implicit topic: Topic[T], fromBytes: FromBytes[T], tag: ClassTag[T]): Unsubscribe = {

      incSubscriptions(topic)
      pubSub.subscribe(factory, group)(onMsg)
    }

    def unsubscribe[T](ref: ActorRef, group: Option[String])(implicit topic: Topic[T]): Unit = {
      val name = MetricName(topic.name)
      registry.counter(s"$name.subscriptions").dec()
      pubSub.unsubscribe(ref, group)
    }

    def topics(timeout: FiniteDuration): Future[Set[String]] = {
      pubSub.topics(timeout)
    }

    private def incSubscriptions(topic: Topic[_]): Unit = {
      val name = MetricName(topic.name)
      registry.counter(s"$name.subscriptions").inc()
    }

    private def markPublish(topic: Topic[_]): Unit = {
      val name = MetricName(topic.name)
      registry.meter(s"$name.publish").mark()
    }
  }


  def apply(pubSub: PubSub, optimiseSubscribe: OptimiseSubscribe): PubSub = new PubSub {

    def publishAny[T: Topic](msg: T, sender: Option[Sender], sendToEachGroup: Boolean) = {
      pubSub.publishAny(msg, sender, sendToEachGroup)
    }

    def subscribeAny[T: Topic : ClassTag](ref: Sender, group: Option[String]) = {
      pubSub.subscribeAny(ref, group)
    }

    def subscribeAny[T: Topic : ClassTag](factory: ActorRefFactory)(onMsg: OnMsg[T]) = {
      pubSub.subscribeAny(factory)(onMsg)
    }

    def subscribeAny[T: Topic : ClassTag](factory: ActorRefFactory, group: Option[String])(onMsg: OnMsg[T]) = {

      pubSub.subscribeAny(factory, group)(onMsg)
    }

    def publish[T: Topic : ToBytes](msg: T, sender: Option[Sender], sendToEachGroup: Boolean) = {
      pubSub.publish(msg, sender, sendToEachGroup)
    }

    def subscribe[T](factory: ActorRefFactory, group: Option[String])(onMsg: OnMsg[T])
      (implicit topic: Topic[T], fromBytes: FromBytes[T], tag: ClassTag[T]) = {

      optimiseSubscribe[T](factory, onMsg) { (factory, onMsg) =>
        pubSub.subscribe[T](factory, group)(onMsg)
      }
    }

    def unsubscribe[T: Topic](ref: Sender, group: Option[String]) = pubSub.unsubscribe(ref, group)

    def topics(timeout: FiniteDuration) = pubSub.topics(timeout)
  }


  val Empty: PubSub = new PubSub {

    def publishAny[T: Topic](msg: T, sender: Option[ActorRef], sendToEachGroup: Boolean = false) = {}

    def subscribeAny[T: Topic: ClassTag](ref: ActorRef, group: Option[String] = None) = {
      Unsubscribe.Empty
    }

    def subscribeAny[T: Topic: ClassTag](factory: ActorRefFactory)(onMsg: OnMsg[T]) = {
      Unsubscribe.Empty
    }

    def subscribeAny[T: Topic: ClassTag](factory: ActorRefFactory, group: Option[String])
      (onMsg: OnMsg[T]) = {

      Unsubscribe.Empty
    }

    def publish[T: Topic: ToBytes](msg: T, sender: Option[ActorRef] = None, sendToEachGroup: Boolean = false) = {}

    def subscribe[T: Topic: FromBytes: ClassTag](factory: ActorRefFactory, group: Option[String] = None)
      (onMsg: OnMsg[T]) = {

      Unsubscribe.Empty
    }

    def unsubscribe[T: Topic](ref: ActorRef, group: Option[String] = None) = {}

    def topics(timeout: FiniteDuration) = Future.successful(Set.empty)
  }


  def proxy(ref: ActorRef): PubSub = new PubSub {

    def publishAny[T: Topic](msg: T, sender: Option[ActorRef], sendToEachGroup: Boolean = false) = {
      ref.tell(msg, sender getOrElse ActorRef.noSender)
    }

    def subscribeAny[T: Topic: ClassTag](ref: ActorRef, group: Option[String] = None) = {
      Unsubscribe.Empty
    }

    def subscribeAny[T: Topic: ClassTag](factory: ActorRefFactory)(onMsg: OnMsg[T]) = {
      Unsubscribe.Empty
    }

    def subscribeAny[T: Topic: ClassTag](factory: ActorRefFactory, group: Option[String])(onMsg: OnMsg[T]) = {
      Unsubscribe.Empty
    }

    def publish[T: Topic: ToBytes](msg: T, sender: Option[ActorRef] = None, sendToEachGroup: Boolean = false) = {
      ref.tell(msg, sender getOrElse ActorRef.noSender)
    }

    def subscribe[T: Topic: FromBytes: ClassTag](factory: ActorRefFactory, group: Option[String] = None)
      (onMsg: OnMsg[T]) = {

      Unsubscribe.Empty
    }

    def unsubscribe[T: Topic](ref: ActorRef, group: Option[String] = None) = {}

    def topics(timeout: FiniteDuration) = {
      implicit val ec = CurrentThreadExecutionContext
      implicit val t = Timeout(timeout)
      ref.ask(Mediator.GetTopics).mapTo[Mediator.CurrentTopics] map { _.topics }
    }
  }


  object Subscription {

    sealed trait In[+T]

    object In {
      final case class Msg[T](msg: T) extends In[T]
      case object Subscribed extends In[Nothing]
    }
  }
}
