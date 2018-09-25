package com.evolutiongaming.cluster.pubsub

import akka.actor.{Actor, ActorRefFactory, ActorSystem, Props}
import com.evolutiongaming.cluster.pubsub.PubSub.{OnMsg, Unsubscribe}
import com.evolutiongaming.concurrent.AvailableProcessors
import com.evolutiongaming.concurrent.sequentially.{MapDirective, SequentialMap, Sequentially}
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.safeakka.actor.ActorLog

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

trait OptimiseSubscribe {

  def apply[A: Topic](
    factory: ActorRefFactory,
    onMsg: OnMsg[A])(
    subscribe: (ActorRefFactory, OnMsg[A]) => Unsubscribe): Unsubscribe
}

object OptimiseSubscribe {

  val Empty: OptimiseSubscribe = new OptimiseSubscribe {
    def apply[A: Topic](
      factory: ActorRefFactory,
      onMsg: OnMsg[A])(
      subscribe: (ActorRefFactory, OnMsg[A]) => Unsubscribe) = subscribe(factory, onMsg)
  }

  type Listener = OnMsg[Any]


  def apply(system: ActorSystem): OptimiseSubscribe = {
    val sequentially = Sequentially(system, None, AvailableProcessors())
    val log = ActorLog(system, classOf[OptimiseSubscribe])
    apply(sequentially, log, system)(system.dispatcher)
  }

  def apply(sequentially: Sequentially[String], log: ActorLog, factory: ActorRefFactory)(implicit ec: ExecutionContext): OptimiseSubscribe = {
    val map = SequentialMap[String, Subscription](sequentially)
    apply(map, log, factory)
  }

  def apply(map: SequentialMap[String, Subscription], log: ActorLog, factory: ActorRefFactory)
    (implicit ec: ExecutionContext): OptimiseSubscribe = {

    def optimise[A](
      onMsg: OnMsg[A])(
      subscribe: (ActorRefFactory, OnMsg[A]) => Unsubscribe)(implicit
      topic: Topic[A]) = {

      val listener = onMsg.asInstanceOf[Listener]

      val result = map.updateUnit(topic.name) { subscription =>
        val updated = subscription match {
          case Some(subscription) => subscription + listener
          case None               =>
            val tmp: OnMsg[A] = (msg: A, sender) => {
              for {
                subscription <- map.getNow(topic.name)
                listener <- subscription.listeners
              } try {
                listener(msg, sender)
              } catch {
                case NonFatal(failure) => log.error(s"onMsg failed for $topic: $failure", failure)
              }
            }

            val unsubscribe = subscribe(factory, tmp)
            Subscription(unsubscribe, listener :: Nel)
        }
        MapDirective.update(updated)
      }

      result.failed.foreach { failure =>
        log.error(s"failed to subscribe to $topic, $failure", failure)
      }

      () => {
        val unsubscribe = for {
          _ <- result
          _ <- map.updateUnit(topic.name) {
            case None               => MapDirective.ignore
            case Some(subscription) => subscription - listener match {
              case Some(subscription) => MapDirective.update(subscription)
              case None               =>
                subscription.unsubscribe()
                MapDirective.remove
            }
          }
        } yield {}

        unsubscribe.failed.foreach { failure =>
          log.error(s"failed to unsubscribe from $topic, $failure", failure)
        }
      }
    }

    new OptimiseSubscribe {

      def apply[A: Topic](
        factory: ActorRefFactory,
        onMsg: OnMsg[A])(
        subscribe: (ActorRefFactory, OnMsg[A]) => Unsubscribe) = {

        val unsubscribe = optimise(onMsg)(subscribe)

        def actor() = new Actor {
          def receive = PartialFunction.empty
          override def postStop() = unsubscribe()
        }

        factory.actorOf(Props(actor))
        unsubscribe
      }
    }
  }


  final case class Subscription(unsubscribe: Unsubscribe, listeners: Nel[Listener]) {
    self =>

    def +(listener: Listener): Subscription = copy(listeners = listener :: listeners)

    def -(listener: Listener): Option[Subscription] = {
      val listeners = self.listeners.filter(_ != listener)
      Nel.opt(listeners).map(listeners => copy(listeners = listeners))
    }
  }
}
