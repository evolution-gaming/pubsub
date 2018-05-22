package com.evolutiongaming.cluster.pubsub

import akka.actor.{Actor, ActorLogging, ActorRef, ActorRefFactory, ActorSystem, Props}
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
  import PubSub.Unsubscribe

  def publishAny[T](msg: T, sender: Option[ActorRef] = None, sendToEachGroup: Boolean = false)
    (implicit topic: Topic[T]): Unit

  def subscribeAny[T](ref: ActorRef, group: Option[String] = None)
    (implicit topic: Topic[T], tag: ClassTag[T]): Unsubscribe

  def subscribeAny[T](factory: ActorRefFactory)(onMsg: (T, ActorRef) => Unit)
    (implicit topic: Topic[T], tag: ClassTag[T]): Unsubscribe

  def publish[T](msg: T, sender: Option[ActorRef] = None, sendToEachGroup: Boolean = false)
    (implicit topic: Topic[T], toBytes: ToBytes[T]): Unit

  def subscribe[T](factory: ActorRefFactory)(onMsg: (T, ActorRef) => Unit)
    (implicit topic: Topic[T], fromBytes: FromBytes[T], tag: ClassTag[T]): Unsubscribe

  def unsubscribe[T](ref: ActorRef, group: Option[String] = None)(implicit topic: Topic[T]): Unit

  def topics(timeout: FiniteDuration = 3.seconds): Future[Set[String]]
}

object PubSub {

  type Unsubscribe = () => Unit

  object Unsubscribe {
    lazy val Empty: Unsubscribe = () => ()
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
    val pubSub = apply(ref, log)
    apply(pubSub, registry)
  }

  def apply(pubSub: ActorRef, log: ActorLog): PubSub = new PubSub {

    def publishAny[T](msg: T, sender: Option[ActorRef], sendToEachGroup: Boolean = false)
      (implicit topic: Topic[T]): Unit = {

      val publish = Mediator.Publish(topic.name, msg, sendToEachGroup)
      log.debug(s"publish $publish")
      pubSub.tell(publish, sender getOrElse ActorRef.noSender)
    }

    def subscribeAny[T](ref: ActorRef, group: Option[String] = None)
      (implicit topic: Topic[T], tag: ClassTag[T]): Unsubscribe = {

      val subscribe = Mediator.Subscribe(topic.name, group, ref)
      log.debug(s"subscribe $subscribe")
      pubSub.tell(subscribe, ref)
      () => unsubscribe(ref, group)
    }

    def subscribeAny[T](factory: ActorRefFactory)(onMsg: (T, ActorRef) => Unit)
      (implicit topic: Topic[T], tag: ClassTag[T]): Unsubscribe = {

      import Listener.In

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

          val logListener = ActorLog(ctx.system, Listener.getClass) prefixed topic.name
          (behavior, logListener)
        }

        val unapply = Unapply.pf[In[T]] {
          case _: Mediator.SubscribeAck => In.Subscribed
          case In.Msg(tag(x))           => In.Msg(x)
          case In.Subscribed            => In.Subscribed
          case tag(x)                   => In.Msg(x)
        }

        val ref = SafeActorRef(setup)(factory, unapply)
        subscribeAny(ref.unsafe)
      }

      subscribe(log.prefixed(topic.name))
    }

    def publish[T](msg: T, sender: Option[ActorRef] = None, sendToEachGroup: Boolean = false)
      (implicit topic: Topic[T], toBytes: ToBytes[T]): Unit = {

      val toBytesAble = ToBytesAble(msg)(toBytes.apply)
      implicit val topicFinal = Topic[ToBytesAble](topic.name)
      publishAny(toBytesAble, sender, sendToEachGroup)
    }

    def subscribe[T](factory: ActorRefFactory)(onMsg: (T, ActorRef) => Unit)
      (implicit topic: Topic[T], fromBytes: FromBytes[T], tag: ClassTag[T]): Unsubscribe = {

      implicit val topicFinal = Topic[ToBytesAble](topic.name)

      def onMsgFinal(msg: ToBytesAble, sender: ActorRef): Unit = {
        msg match {
          case ToBytesAble.Bytes(bytes)     => onMsg(fromBytes(bytes), sender)
          case ToBytesAble.Raw(tag(msg), _) => onMsg(msg, sender)
          case ToBytesAble.Raw(msg, _)      => log.warn(s"$topic: receive unexpected $msg")
        }
      }

      subscribeAny(factory)(onMsgFinal)
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

    def subscribeAny[T](factory: ActorRefFactory)(onMsg: (T, ActorRef) => Unit)
      (implicit topic: Topic[T], tag: ClassTag[T]): Unsubscribe = {

      incSubscriptions(topic)
      pubSub.subscribeAny(factory)(onMsg)
    }

    def publish[T](msg: T, sender: Option[ActorRef] = None, sendToEachGroup: Boolean = false)
      (implicit topic: Topic[T], toBytes: ToBytes[T]): Unit = {

      markPublish(topic)
      pubSub.publish(msg, sender, sendToEachGroup)
    }

    def subscribe[T](factory: ActorRefFactory)(onMsg: (T, ActorRef) => Unit)
      (implicit topic: Topic[T], fromBytes: FromBytes[T], tag: ClassTag[T]): Unsubscribe = {

      incSubscriptions(topic)
      pubSub.subscribe(factory)(onMsg)
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


  object Empty extends PubSub {

    def publishAny[T](msg: T, sender: Option[ActorRef], sendToEachGroup: Boolean = false)
      (implicit topic: Topic[T]): Unit = {}

    def subscribeAny[T](ref: ActorRef, group: Option[String] = None)
      (implicit topic: Topic[T], tag: ClassTag[T]): Unsubscribe = {
      Unsubscribe.Empty
    }

    def subscribeAny[T](factory: ActorRefFactory)(onMsg: (T, ActorRef) => Unit)
      (implicit topic: Topic[T], tag: ClassTag[T]): Unsubscribe = {
      Unsubscribe.Empty
    }

    def publish[T](msg: T, sender: Option[ActorRef] = None, sendToEachGroup: Boolean = false)
      (implicit topic: Topic[T], toBytes: ToBytes[T]): Unit = {}

    def subscribe[T](factory: ActorRefFactory)(onMsg: (T, ActorRef) => Unit)
      (implicit topic: Topic[T], fromBytes: FromBytes[T], tag: ClassTag[T]): Unsubscribe = {
      Unsubscribe.Empty
    }

    def unsubscribe[T](ref: ActorRef, group: Option[String] = None)(implicit topic: Topic[T]): Unit = {}

    def topics(timeout: FiniteDuration): Future[Set[String]] = Future.successful(Set.empty)
  }


  class Proxy(ref: ActorRef) extends PubSub {

    def publishAny[T](msg: T, sender: Option[ActorRef], sendToEachGroup: Boolean = false)(implicit topic: Topic[T]): Unit = {
      ref.tell(msg, sender getOrElse ActorRef.noSender)
    }

    def subscribeAny[T](ref: ActorRef, group: Option[String] = None)
      (implicit topic: Topic[T], tag: ClassTag[T]): Unsubscribe = {
      Unsubscribe.Empty
    }

    def subscribeAny[T](factory: ActorRefFactory)(onMsg: (T, ActorRef) => Unit)
      (implicit topic: Topic[T], tag: ClassTag[T]): Unsubscribe = {
      Unsubscribe.Empty
    }

    def publish[T](msg: T, sender: Option[ActorRef] = None, sendToEachGroup: Boolean = false)
      (implicit topic: Topic[T], toBytes: ToBytes[T]): Unit = {
      ref.tell(msg, sender getOrElse ActorRef.noSender)
    }

    def subscribe[T](factory: ActorRefFactory)(onMsg: (T, ActorRef) => Unit)
      (implicit topic: Topic[T], fromBytes: FromBytes[T], tag: ClassTag[T]): Unsubscribe = {
      Unsubscribe.Empty
    }

    def unsubscribe[T](ref: ActorRef, group: Option[String] = None)(implicit topic: Topic[T]): Unit = {}

    def topics(timeout: FiniteDuration): Future[Set[String]] = {
      implicit val ec = CurrentThreadExecutionContext
      implicit val t = Timeout(timeout)
      ref.ask(Mediator.GetTopics).mapTo[Mediator.CurrentTopics] map { _.topics }
    }
  }

  object Proxy {
    def apply(ref: ActorRef): Proxy = new Proxy(ref)
  }


  abstract class Listener[T](pubSub: PubSub)(implicit topic: Topic[T], tag: ClassTag[T]) extends Actor with ActorLogging {

    override def preStart() = {
      super.preStart()
      pubSub.subscribeAny[T](self)
    }

    def apply(msg: T, sender: ActorRef): Unit

    def receive = {
      case x: Mediator.SubscribeAck => log.debug("{}: subscribed {}", topic, x.subscribe.ref)
      case tag(msg)                 => onExpected(msg)
      case msg                      => onUnexpected(msg)
    }

    private def onExpected(msg: T) = {
      log.debug("{}: receive {}", topic, msg)
      try apply(msg, sender()) catch {
        case NonFatal(failure) => onFailure(failure)
      }
    }

    private def onUnexpected(msg: Any) = log.warning(s"{}: receive unexpected {}", topic, msg)

    private def onFailure(failure: Throwable) = log.error(failure, s"$topic: failure $failure")

    override def postStop() = {
      pubSub.unsubscribe[T](self)
      super.postStop()
    }
  }

  object Listener {

    def props[T](pubSub: PubSub)(onMsg: (T, ActorRef) => Unit)(implicit topic: Topic[T], tag: ClassTag[T]): Props = {
      def actor() = new Listener(pubSub) {
        def apply(msg: T, sender: ActorRef): Unit = onMsg(msg, sender)
      }

      Props(actor())
    }

    sealed trait In[+T]
    object In {
      case class Msg[T](msg: T) extends In[T]
      case object Subscribed extends In[Nothing]
    }
  }
}
