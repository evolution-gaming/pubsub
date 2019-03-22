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

  def publishAny[A: Topic](msg: A, sender: Option[ActorRef] = None, sendToEachGroup: Boolean = false): Unit

  def subscribeAny[A: Topic: ClassTag](ref: ActorRef, group: Option[String] = None): Unsubscribe

  def subscribeAny[A: Topic: ClassTag](factory: ActorRefFactory)(onMsg: OnMsg[A]): Unsubscribe

  def subscribeAny[A: Topic: ClassTag](factory: ActorRefFactory, group: Option[String])(onMsg: OnMsg[A]): Unsubscribe

  def publish[A: Topic: ToBytes](msg: A, sender: Option[ActorRef] = None, sendToEachGroup: Boolean = false): Unit

  def subscribe[A: Topic: FromBytes: ClassTag](factory: ActorRefFactory, group: Option[String] = None)(onMsg: OnMsg[A]): Unsubscribe

  def unsubscribe[A: Topic](ref: ActorRef, group: Option[String] = None): Unit

  def topics(timeout: FiniteDuration = 3.seconds): Future[Set[String]]
}

object PubSub {

  type OnMsg[-A] = (A, Sender) => Unit

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

    def publishAny[A](msg: A, sender: Option[ActorRef], sendToEachGroup: Boolean = false)
      (implicit topic: Topic[A]): Unit = {

      val publish = Mediator.Publish(topic.name, msg, sendToEachGroup)
      log.debug(s"publish $publish")
      pubSub.tell(publish, sender getOrElse ActorRef.noSender)
    }

    def subscribeAny[A](factory: ActorRefFactory)(onMsg: OnMsg[A])
      (implicit topic: Topic[A], tag: ClassTag[A]): Unsubscribe = {

      subscribeAny(factory, None)(onMsg)
    }

    def subscribeAny[A](ref: ActorRef, group: Option[String] = None)
      (implicit topic: Topic[A], tag: ClassTag[A]): Unsubscribe = {

      val subscribe = Mediator.Subscribe(topic.name, group, ref)
      log.debug(s"subscribe $subscribe")
      pubSub.tell(subscribe, ref)
      () => unsubscribe(ref, group)
    }

    def subscribeAny[A](factory: ActorRefFactory, group: Option[String])(onMsg: OnMsg[A])
      (implicit topic: Topic[A], tag: ClassTag[A]): Unsubscribe = {

      import Subscription.In

      def subscribe(log: ActorLog) = {

        val setup: SetupActor[In[A]] = ctx => {
          val behavior = Behavior.stateless[In[A]] {
            case Signal.Msg(msg, sender) => msg match {
              case In.Subscribed   => log.debug(s"subscribed ${ ctx.self }")
              case In.Unsubscribed => log.debug(s"unsubscribed ${ ctx.self }")
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

        val ref = SafeActorRef(setup)(factory, Subscription.In.unapplyOf[A])
        subscribeAny(ref.unsafe, group)
      }

      subscribe(log.prefixed(topic.name))
    }

    def publish[A](msg: A, sender: Option[ActorRef] = None, sendToEachGroup: Boolean = false)
      (implicit topic: Topic[A], toBytes: ToBytes[A]): Unit = {

      val toBytesAble = ToBytesAble(msg)(toBytes.apply)
      implicit val topicFinal = Topic[ToBytesAble](topic.name)
      publishAny(toBytesAble, sender, sendToEachGroup)
    }

    def subscribe[A](factory: ActorRefFactory, group: Option[String] = None)(onMsg: OnMsg[A])
      (implicit topic: Topic[A], fromBytes: FromBytes[A], tag: ClassTag[A]): Unsubscribe = {

      implicit val topicFinal = Topic[ToBytesAble](topic.name)

      val onToBytesAble: OnMsg[ToBytesAble]  = (msg: ToBytesAble, sender: ActorRef) => {
        msg match {
          case ToBytesAble.Bytes(bytes)  => onMsg(fromBytes(bytes), sender)
          case ToBytesAble.Raw(tag(msg)) => onMsg(msg, sender)
          case ToBytesAble.Raw(msg)      => log.warn(s"$topic: receive unexpected $msg")
        }
      }

      subscribeAny(factory, group)(onToBytesAble)
    }

    def unsubscribe[A](ref: ActorRef, group: Option[String] = None)(implicit topic: Topic[A]): Unit = {
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

    def publishAny[A](msg: A, sender: Option[ActorRef], sendToEachGroup: Boolean = false)
      (implicit topic: Topic[A]): Unit = {

      markPublish(topic)
      pubSub.publishAny(msg, sender, sendToEachGroup)
    }

    def subscribeAny[A](ref: ActorRef, group: Option[String])
      (implicit topic: Topic[A], tag: ClassTag[A]): Unsubscribe = {

      incSubscriptions(topic)
      pubSub.subscribeAny(ref, group)
    }

    def subscribeAny[A](factory: ActorRefFactory)(onMsg: OnMsg[A])
      (implicit topic: Topic[A], tag: ClassTag[A]): Unsubscribe = {

      incSubscriptions(topic)
      pubSub.subscribeAny(factory)(onMsg)
    }

    def subscribeAny[A](factory: ActorRefFactory, group: Option[String])(onMsg: OnMsg[A])
      (implicit topic: Topic[A], tag: ClassTag[A]): Unsubscribe = {

      incSubscriptions(topic)
      pubSub.subscribeAny(factory, group)(onMsg)
    }

    def publish[A](msg: A, sender: Option[ActorRef] = None, sendToEachGroup: Boolean = false)
      (implicit topic: Topic[A], toBytes: ToBytes[A]): Unit = {

      markPublish(topic)
      pubSub.publish(msg, sender, sendToEachGroup)
    }

    def subscribe[A](factory: ActorRefFactory, group: Option[String] = None)(onMsg: OnMsg[A])
      (implicit topic: Topic[A], fromBytes: FromBytes[A], tag: ClassTag[A]): Unsubscribe = {

      incSubscriptions(topic)
      pubSub.subscribe(factory, group)(onMsg)
    }

    def unsubscribe[A](ref: ActorRef, group: Option[String])(implicit topic: Topic[A]): Unit = {
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

    def publishAny[A: Topic](msg: A, sender: Option[Sender], sendToEachGroup: Boolean) = {
      pubSub.publishAny(msg, sender, sendToEachGroup)
    }

    def subscribeAny[A: Topic : ClassTag](ref: Sender, group: Option[String]) = {
      pubSub.subscribeAny(ref, group)
    }

    def subscribeAny[A: Topic : ClassTag](factory: ActorRefFactory)(onMsg: OnMsg[A]) = {
      pubSub.subscribeAny(factory)(onMsg)
    }

    def subscribeAny[A: Topic : ClassTag](factory: ActorRefFactory, group: Option[String])(onMsg: OnMsg[A]) = {

      pubSub.subscribeAny(factory, group)(onMsg)
    }

    def publish[A: Topic : ToBytes](msg: A, sender: Option[Sender], sendToEachGroup: Boolean) = {
      pubSub.publish(msg, sender, sendToEachGroup)
    }

    def subscribe[A](factory: ActorRefFactory, group: Option[String])(onMsg: OnMsg[A])
      (implicit topic: Topic[A], fromBytes: FromBytes[A], tag: ClassTag[A]) = {

      optimiseSubscribe[A](factory, onMsg) { (factory, onMsg) =>
        pubSub.subscribe[A](factory, group)(onMsg)
      }
    }

    def unsubscribe[A: Topic](ref: Sender, group: Option[String]) = pubSub.unsubscribe(ref, group)

    def topics(timeout: FiniteDuration) = pubSub.topics(timeout)
  }


  val Empty: PubSub = new PubSub {

    def publishAny[A: Topic](msg: A, sender: Option[ActorRef], sendToEachGroup: Boolean = false) = {}

    def subscribeAny[A: Topic: ClassTag](ref: ActorRef, group: Option[String] = None) = {
      Unsubscribe.Empty
    }

    def subscribeAny[A: Topic: ClassTag](factory: ActorRefFactory)(onMsg: OnMsg[A]) = {
      Unsubscribe.Empty
    }

    def subscribeAny[A: Topic: ClassTag](factory: ActorRefFactory, group: Option[String])
      (onMsg: OnMsg[A]) = {

      Unsubscribe.Empty
    }

    def publish[A: Topic: ToBytes](msg: A, sender: Option[ActorRef] = None, sendToEachGroup: Boolean = false) = {}

    def subscribe[A: Topic: FromBytes: ClassTag](factory: ActorRefFactory, group: Option[String] = None)
      (onMsg: OnMsg[A]) = {

      Unsubscribe.Empty
    }

    def unsubscribe[A: Topic](ref: ActorRef, group: Option[String] = None) = {}

    def topics(timeout: FiniteDuration) = Future.successful(Set.empty)
  }


  def proxy(ref: ActorRef): PubSub = new PubSub {

    def publishAny[A: Topic](msg: A, sender: Option[ActorRef], sendToEachGroup: Boolean = false) = {
      ref.tell(msg, sender getOrElse ActorRef.noSender)
    }

    def subscribeAny[A: Topic: ClassTag](ref: ActorRef, group: Option[String] = None) = {
      Unsubscribe.Empty
    }

    def subscribeAny[A: Topic: ClassTag](factory: ActorRefFactory)(onMsg: OnMsg[A]) = {
      Unsubscribe.Empty
    }

    def subscribeAny[A: Topic: ClassTag](factory: ActorRefFactory, group: Option[String])(onMsg: OnMsg[A]) = {
      Unsubscribe.Empty
    }

    def publish[A: Topic: ToBytes](msg: A, sender: Option[ActorRef] = None, sendToEachGroup: Boolean = false) = {
      ref.tell(msg, sender getOrElse ActorRef.noSender)
    }

    def subscribe[A: Topic: FromBytes: ClassTag](factory: ActorRefFactory, group: Option[String] = None)
      (onMsg: OnMsg[A]) = {

      Unsubscribe.Empty
    }

    def unsubscribe[A: Topic](ref: ActorRef, group: Option[String] = None) = {}

    def topics(timeout: FiniteDuration) = {
      implicit val ec = CurrentThreadExecutionContext
      implicit val t = Timeout(timeout)
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
}
