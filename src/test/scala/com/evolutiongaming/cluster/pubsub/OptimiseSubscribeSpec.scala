package com.evolutiongaming.cluster.pubsub

import akka.testkit.TestActors
import com.evolutiongaming.cluster.pubsub.PubSub.{OnMsg, Unsubscribe}
import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.concurrent.sequentially.Sequentially
import com.evolutiongaming.safeakka.actor.ActorLog
import org.scalatest.{Matchers, WordSpec}

class OptimiseSubscribeSpec extends WordSpec with ActorSpec with Matchers {

  "OptimiseSubscribe" should {

    "subscribe once and dispatch msgs to all subscribers" in {

      val optimiseSubscribe = OptimiseSubscribe(
        Sequentially.now,
        ActorLog.empty)(
        CurrentThreadExecutionContext)

      type Msg = String

      var msgs = List.empty[Msg]

      val sender = system.actorOf(TestActors.blackholeProps)

      implicit val topic = Topic[String]

      var listeners = List.empty[OnMsg[Msg]]

      def publish(msg: Msg) = for {
        onMsg <- listeners
      } onMsg(msg, sender.path)

      val subscribe: OnMsg[Msg] => Unsubscribe = (onMsg: OnMsg[Msg]) => {
        listeners = onMsg :: listeners
        () => listeners = listeners.filter(_ != onMsg)
      }

      def onMsg(prefix: String): OnMsg[Msg] = (msg, _) => msgs = s"$prefix-$msg" :: msgs

      listeners.size shouldEqual 0
      publish("1")
      msgs shouldEqual Nil

      val onMsg1 = onMsg("1")
      val unsubscribe1 = optimiseSubscribe[Msg](onMsg1)(subscribe)
      listeners.size shouldEqual 1

      publish("2")
      msgs shouldEqual List("1-2")

      val onMsg2 = onMsg("2")
      val unsubscribe2 = optimiseSubscribe[Msg](onMsg2)(subscribe)
      listeners.size shouldEqual 1

      publish("3")
      msgs shouldEqual List("1-3", "2-3", "1-2")

      unsubscribe1()
      listeners.size shouldEqual 1

      unsubscribe2()
      listeners.size shouldEqual 0
    }
  }
}
