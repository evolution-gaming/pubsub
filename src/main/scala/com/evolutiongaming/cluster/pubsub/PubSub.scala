package com.evolutiongaming.cluster.pubsub

import akka.actor.{Actor, ActorLogging, ActorRef, ActorRefFactory, ActorSystem, Props}
import akka.cluster.Cluster
import akka.cluster.pubsub.{DistributedPubSubMediatorSerializing, DistributedPubSubMediator => Mediator}
import com.codahale.metrics.MetricRegistry
import com.evolutiongaming.metrics.MetricName
import com.evolutiongaming.safeakka.actor._

import scala.reflect.ClassTag
import scala.util.control.NonFatal

trait PubSub {
  import PubSub.Unsubscribe

  def publish[T](msg: T, sender: Option[ActorRef] = None, sendToEachGroup: Boolean = false)
    (implicit topic: Topic[T]): Unit

  def subscribe[T](ref: ActorRef, group: Option[String] = None)(implicit topic: Topic[T]): Unsubscribe

  def subscribe[T](factory: ActorRefFactory)(f: (T, ActorRef) => Unit)(implicit topic: Topic[T]): Unsubscribe

  def unsubscribe[T](ref: ActorRef, group: Option[String] = None)(implicit topic: Topic[T]): Unit

  def topics(sender: ActorRef): Unit
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

    def publish[T](msg: T, sender: Option[ActorRef], sendToEachGroup: Boolean = false)
      (implicit topic: Topic[T]): Unit = {

      val publish = Mediator.Publish(topic.str, msg, sendToEachGroup)
      log.debug(s"publish $publish")
      pubSub.tell(publish, sender getOrElse ActorRef.noSender)
    }

    def subscribe[T](ref: ActorRef, group: Option[String] = None)(implicit topic: Topic[T]): Unsubscribe = {
      val subscribe = Mediator.Subscribe(topic.str, group, ref)
      log.debug(s"subscribe $subscribe")
      pubSub.tell(subscribe, ref)
      () => unsubscribe(ref, group)
    }

    def subscribe[T](factory: ActorRefFactory)(onMsg: (T, ActorRef) => Unit)(implicit topic: Topic[T]): Unsubscribe = {
      val setup = Listener.setup(this)(onMsg)
      val ref = Listener.safeActorRef(setup, factory)
      subscribe(ref.unsafe)
    }

    def unsubscribe[T](ref: ActorRef, group: Option[String] = None)(implicit topic: Topic[T]): Unit = {
      val unsubscribe = Mediator.Unsubscribe(topic.str, group, ref)
      log.debug(s"unsubscribe $unsubscribe")
      pubSub.tell(unsubscribe, ref)
    }

    def topics(sender: ActorRef): Unit = {
      log.debug(s"topics $sender")
      pubSub.tell(Mediator.GetTopics, sender)
    }
  }

  def apply(pubSub: PubSub, registry: MetricRegistry): PubSub = new PubSub {

    def publish[T](msg: T, sender: Option[ActorRef], sendToEachGroup: Boolean = false)(implicit topic: Topic[T]): Unit = {
      val name = MetricName(topic.str)
      registry.meter(s"$name.publish").mark()
      pubSub.publish(msg, sender, sendToEachGroup)
    }

    def subscribe[T](ref: ActorRef, group: Option[String])(implicit topic: Topic[T]): Unsubscribe = {
      val name = MetricName(topic.str)
      registry.counter(s"$name.subscriptions").inc()
      pubSub.subscribe(ref, group)
    }

    def subscribe[T](factory: ActorRefFactory)(f: (T, ActorRef) => Unit)(implicit topic: Topic[T]): Unsubscribe = {
      val name = MetricName(topic.str)
      registry.counter(s"$name.subscriptions").inc()
      pubSub.subscribe(factory)(f)
    }

    def unsubscribe[T](ref: ActorRef, group: Option[String])(implicit topic: Topic[T]): Unit = {
      val name = MetricName(topic.str)
      registry.counter(s"$name.subscriptions").dec()
      pubSub.unsubscribe(ref, group)
    }

    def topics(sender: ActorRef): Unit = {
      pubSub.topics(sender)
    }
  }


  object Empty extends PubSub {

    def publish[T](msg: T, sender: Option[ActorRef], sendToEachGroup: Boolean = false)(implicit topic: Topic[T]): Unit = {}

    def subscribe[T](ref: ActorRef, group: Option[String] = None)(implicit topic: Topic[T]): Unsubscribe = {
      Unsubscribe.Empty
    }

    def subscribe[T](factory: ActorRefFactory)(f: (T, ActorRef) => Unit)(implicit topic: Topic[T]): Unsubscribe = {
      Unsubscribe.Empty
    }

    def unsubscribe[T](ref: ActorRef, group: Option[String] = None)(implicit topic: Topic[T]): Unit = {}

    def topics(ref: ActorRef): Unit = {}
  }


  class Proxy(ref: ActorRef) extends PubSub {

    def publish[T](msg: T, sender: Option[ActorRef], sendToEachGroup: Boolean = false)(implicit topic: Topic[T]): Unit = {
      ref.tell(msg, sender getOrElse ActorRef.noSender)
    }

    def subscribe[T](ref: ActorRef, group: Option[String] = None)(implicit topic: Topic[T]): Unsubscribe = {
      Unsubscribe.Empty
    }

    def subscribe[T](factory: ActorRefFactory)(f: (T, ActorRef) => Unit)(implicit topic: Topic[T]): Unsubscribe = {
      Unsubscribe.Empty
    }

    def unsubscribe[T](ref: ActorRef, group: Option[String] = None)(implicit topic: Topic[T]): Unit = {}

    def topics(ref: ActorRef): Unit = {}
  }

  object Proxy {
    def apply(ref: ActorRef): Proxy = new Proxy(ref)
  }


  abstract class Listener[T](pubSub: PubSub)(implicit topic: Topic[T]) extends Actor with ActorLogging {
    private val tag: ClassTag[T] = topic.tag

    override def preStart() = {
      super.preStart()
      pubSub.subscribe[T](self)
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

    private def onUnexpected(msg: Any) = log.debug(s"{}: receive unexpected {}", topic, msg)

    private def onFailure(failure: Throwable) = log.error(failure, s"$topic: failure $failure")

    override def postStop() = {
      pubSub.unsubscribe[T](self)
      super.postStop()
    }
  }

  object Listener {

    def props[T](pubSub: PubSub)(f: (T, ActorRef) => Unit)(implicit topic: Topic[T]): Props = {
      Props(new Imp(pubSub, f))
    }

    def setup[T](pubSub: PubSub)(onMsg: (T, ActorRef) => Unit)(implicit topic: Topic[T]): SetupActor[In[T]] = ctx => {
      val log = ActorLog(ctx.system, Listener.getClass) prefixed topic.toString

      val behavior = Behavior.stateless[In[T]] {
        case Signal.Msg(msg, sender) => msg match {
          case In.Ack(subscribe) => log.debug(s"$topic: subscribed ${ subscribe.ref }")
          case In.Msg(msg)       => try onMsg(msg, sender) catch {
            case NonFatal(failure) => log.error(s"$topic: failure $failure", failure)
          }
        }
        case Signal.PostStop         => pubSub.unsubscribe(ctx.self)
        case _                       =>
      }
      (behavior, log)
    }

    def safeActorRef[T](setup: SetupActor[In[T]], factory: ActorRefFactory)(implicit topic: Topic[T]): SafeActorRef[In[T]] = {
      val tag = topic.tag
      val unapply = Unapply.pf[In[T]] {
        case Mediator.SubscribeAck(x) => In.Ack(x)
        case In.Msg(tag(x))           => In.Msg(x)
        case in: In.Ack               => in
        case tag(x)                   => In.Msg(x)
      }
      SafeActorRef(setup)(factory, unapply)
    }


    class Imp[T](pubSub: PubSub, f: (T, ActorRef) => Unit)(implicit topic: Topic[T]) extends Listener[T](pubSub) {
      def apply(msg: T, sender: ActorRef): Unit = f(msg, sender)
    }


    sealed trait In[+T]
    object In {
      case class Msg[T](msg: T) extends In[T]
      case class Ack(subscribe: Mediator.Subscribe) extends In[Nothing]
    }
  }
}
