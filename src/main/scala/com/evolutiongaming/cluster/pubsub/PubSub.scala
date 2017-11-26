package com.evolutiongaming.cluster.pubsub

import akka.actor.{Actor, ActorLogging, ActorRef, ActorRefFactory, ActorSystem, Props, Terminated}
import akka.cluster.Cluster
import akka.cluster.pubsub.{DistributedPubSubMediatorSerializing, DistributedPubSubMediator => Mediator}
import com.codahale.metrics.{Counter, MetricRegistry}
import com.evolutiongaming.metrics.MetricName
import com.evolutiongaming.safeakka.actor._
import com.typesafe.scalalogging.LazyLogging

import scala.reflect.ClassTag
import scala.util.control.NonFatal

trait PubSub {

  def publish[T](msg: WithSender[T], sendToEachGroup: Boolean = false)(implicit topic: Topic[T]): Unit

  def subscribe[T](ref: ActorRef, group: Option[String] = None)(implicit topic: Topic[T]): Unit

  def subscribe[T](factory: ActorRefFactory)(f: (T, ActorRef) => Unit)(implicit topic: Topic[T]): Unit

  def unsubscribe[T](ref: ActorRef, group: Option[String] = None)(implicit topic: Topic[T]): Unit

  def topics(sender: ActorRef): Unit
}

object PubSub {

  def apply(
    system: ActorSystem,
    registry: MetricRegistry,
    name: String = "PubSub",
    serialize: String => Boolean = _ => false): PubSub = {

    val props = this.props(system, registry, serialize)
    val ref = system.actorOf(props, name)
    new MeteredImpl(ref, registry)
  }

  def props(system: ActorSystem, registry: MetricRegistry, serialize: String => Boolean = _ => false): Props = {
    val impl = if (system hasExtension Cluster) {
      DistributedPubSubMediatorSerializing(system, serialize)
    } else {
      system.actorOf(LocalPubSub.props)
    }

    val subscriptions = registry.counter("cluster.pubsub.subscriptions")
    MeteredActor.props(impl, subscriptions)
  }


  class Impl(pubSub: ActorRef) extends PubSub with LazyLogging {

    def publish[T](msg: WithSender[T], sendToEachGroup: Boolean = false)(implicit topic: Topic[T]): Unit = {

      val publish = Mediator.Publish(topic.str, msg.msg, sendToEachGroup)
      logger.debug(s"publish $publish")
      pubSub.tell(publish, msg.senderOrNot)
    }

    def subscribe[T](ref: ActorRef, group: Option[String] = None)(implicit topic: Topic[T]): Unit = {
      val subscribe = Mediator.Subscribe(topic.str, group, ref)
      logger.debug(s"subscribe $subscribe")
      pubSub.tell(subscribe, ref)
    }

    def subscribe[T](factory: ActorRefFactory)(onMsg: (T, ActorRef) => Unit)(implicit topic: Topic[T]): Unit = {
      val setup = Listener.setup(this)(onMsg)
      Listener.safeActorRef(setup, factory)
    }

    def unsubscribe[T](ref: ActorRef, group: Option[String] = None)(implicit topic: Topic[T]): Unit = {
      val unsubscribe = Mediator.Unsubscribe(topic.str, group, ref)
      logger.debug(s"unsubscribe $unsubscribe")
      pubSub.tell(unsubscribe, ref)
    }

    def topics(sender: ActorRef): Unit = {
      logger.debug(s"topics $sender")
      pubSub.tell(Mediator.GetTopics, sender)
    }
  }

  class MeteredImpl(pubSub: ActorRef, registry: MetricRegistry) extends Impl(pubSub) {

    override def publish[T](msg: WithSender[T], sendToEachGroup: Boolean)(implicit topic: Topic[T]) = {

      val name = MetricName(topic.str)
      registry.meter("cluster.pubsub.messages").mark()
      registry.meter(s"cluster.pubsub.messages.$name").mark()

      super.publish(msg, sendToEachGroup)
    }
  }


  object Empty extends PubSub {

    def publish[T](msg: WithSender[T], sendToEachGroup: Boolean = false)(implicit topic: Topic[T]): Unit = {}

    def subscribe[T](ref: ActorRef, group: Option[String] = None)(implicit topic: Topic[T]): Unit = {}

    def subscribe[T](factory: ActorRefFactory)(f: (T, ActorRef) => Unit)(implicit topic: Topic[T]): Unit = {}

    def unsubscribe[T](ref: ActorRef, group: Option[String] = None)(implicit topic: Topic[T]): Unit = {}

    def topics(ref: ActorRef): Unit = {}
  }


  class Proxy(singleReceiver: ActorRef) extends PubSub {

    def publish[T](msg: WithSender[T], sendToEachGroup: Boolean = false)(implicit topic: Topic[T]): Unit = {
      singleReceiver.tell(msg.msg, msg.senderOrNot)
    }

    def subscribe[T](ref: ActorRef, group: Option[String] = None)(implicit topic: Topic[T]): Unit = {}

    def subscribe[T](factory: ActorRefFactory)(f: (T, ActorRef) => Unit)(implicit topic: Topic[T]): Unit = {}

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

      pubSub.subscribe[T](ctx.self)

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

  class MeteredActor(impl: ActorRef, subscriptionsCounter: Counter) extends Actor {

    private var subscriptionCounts: Map[ActorRef, Int] = Map().withDefaultValue(0)

    def receive = {
      case x: Mediator.Publish =>
        impl forward x

      case x: Mediator.Subscribe =>
        subscriptionsCounter.inc()
        subscriptionCounts = subscriptionCounts.updated(x.ref, subscriptionCounts(x.ref) + 1)
        context.watch(x.ref)
        impl forward x

      case x: Mediator.Unsubscribe =>
        subscriptionsCounter.dec()
        subscriptionCounts = subscriptionCounts.updated(x.ref, subscriptionCounts(x.ref) - 1)
        impl forward x

      case Terminated(ref) =>
        subscriptionsCounter.dec(subscriptionCounts(ref).toLong)
        subscriptionCounts -= ref
        context unwatch ref

      case x => impl forward x
    }
  }

  object MeteredActor {
    def props(impl: ActorRef, subscriptionsCounter: Counter): Props = {
      Props(new MeteredActor(impl, subscriptionsCounter))
    }
  }
}
