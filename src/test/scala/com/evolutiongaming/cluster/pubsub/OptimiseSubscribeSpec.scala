package com.evolutiongaming.cluster.pubsub

import akka.actor.{Actor, ActorRefFactory, Props}
import com.evolutiongaming.cluster.pubsub.PubSub.{OnMsg, Unsubscribe}
import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.concurrent.sequentially.Sequentially
import com.evolutiongaming.safeakka.actor.ActorLog
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.duration._
import scala.concurrent.{Await, Promise}

class OptimiseSubscribeSpec extends WordSpec with ActorSpec with Matchers {

  "OptimiseSubscribe" should {

    "subscribe once and dispatch msgs to all subscribers" in {

      val optimiseSubscribe = OptimiseSubscribe(
        Sequentially.now,
        ActorLog.empty,
        system)(CurrentThreadExecutionContext)

      type Msg = String

      var msgs = List.empty[Msg]

      val sender = Actor.noSender

      implicit val topic = Topic[String]

      var listeners = List.empty[OnMsg[Msg]]

      def publish(msg: Msg) = for {
        onMsg <- listeners
      } onMsg(msg, sender)

      val subscribe: (ActorRefFactory, OnMsg[Msg]) => Unsubscribe = (_, onMsg: OnMsg[Msg]) => {
        listeners = onMsg :: listeners
        () => listeners = listeners.filter(_ != onMsg)
      }

      def onMsg(prefix: String): OnMsg[Msg] = (msg, _) => msgs = s"$prefix-$msg" :: msgs

      listeners.size shouldEqual 0
      publish("1")
      msgs shouldEqual Nil

      val onMsg1 = onMsg("1")
      val unsubscribe1 = optimiseSubscribe[Msg](system, onMsg1)(subscribe)
      listeners.size shouldEqual 1

      publish("2")
      msgs shouldEqual List("1-2")

      val onMsg2 = onMsg("2")
      val unsubscribe2 = optimiseSubscribe[Msg](system, onMsg2)(subscribe)
      listeners.size shouldEqual 1

      publish("3")
      msgs shouldEqual List("1-3", "2-3", "1-2")

      unsubscribe1()
      listeners.size shouldEqual 1

      unsubscribe2()
      listeners.size shouldEqual 0

      def actor(prefix: String) = {
        val subscribed = Promise[Unsubscribe]()
        val stopped = Promise[Unit]()

        def actor() = new Actor {

          override def preStart() = {
            val unsubscribe = optimiseSubscribe[Msg](context, onMsg(prefix))(subscribe)
            subscribed.success(unsubscribe)
          }

          def receive = PartialFunction.empty

          override def postStop() = {
            stopped.success(())
          }
        }

        val ref = system.actorOf(Props(actor()))
        Await.result(subscribed.future, 3.seconds)

        () => {
          system.stop(ref)
          Await.result(stopped.future, 3.seconds)
        }
      }

      val stop1 = actor("3")
      val stop2 = actor("4")
      listeners.size shouldEqual 1

      publish("4")
      msgs shouldEqual List("3-4", "4-4", "1-3", "2-3", "1-2")

      stop1()
      listeners.size shouldEqual 1

      publish("5")
      msgs shouldEqual List("4-5", "3-4", "4-4", "1-3", "2-3", "1-2")

      stop2()
      listeners.size shouldEqual 0
    }
  }
}
