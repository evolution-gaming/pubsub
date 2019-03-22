package com.evolutiongaming.cluster.pubsub

import akka.cluster.pubsub.{DistributedPubSubMediator => Mediator}
import akka.testkit.TestProbe
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.serialization.ToBytesAble
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.Await

class PubSubSpec extends WordSpec with ActorSpec with Matchers {

  val topic = "topic"
  val msg = "msg"
  type Msg = String
  implicit val MsgTopic: Topic[Msg] = Topic[Msg](topic)

  "PubSub" should {

    for {
      group <- List(Some("group"), None)
    } {
      s"subscribeAny ActorRef, group: $group" in new Scope {
        pubSub.subscribeAny[Msg](ref, group)
        expectMsg(Mediator.Subscribe(topic, group, ref))
        lastSender shouldEqual probe.ref
      }

      s"subscribeAny, group: $group" in new Scope {
        val unsubscribe = pubSub.subscribeAny[Msg](system, group) { (_: Msg, _) => }
        val subscriber = expectMsgPF() { case Mediator.Subscribe(`topic`, `group`, ref) => ref }
        unsubscribe()
        expectMsg(Mediator.Unsubscribe(topic, group, subscriber))
      }

      s"subscribe, group: $group" in new Scope {
        val unsubscribe = pubSub.subscribe[Msg](system, group) { (_: Msg, _) => }
        val subscriber = expectMsgPF() { case Mediator.Subscribe(`topic`, `group`, ref) => ref }
        unsubscribe()
        expectMsg(Mediator.Unsubscribe(topic, group, subscriber))
      }

      s"unsubscribe, group: $group" in new Scope {
        pubSub.unsubscribe[Msg](ref, group)
        expectMsg(Mediator.Unsubscribe(topic, group, ref))
        lastSender shouldEqual probe.ref
      }
    }

    for {
      sendToEachGroup <- List(true, false)
    } {
      s"publishAny, sendToEachGroup: $sendToEachGroup" in new Scope {
        pubSub.publishAny(msg, sendToEachGroup = sendToEachGroup)
        expectMsg(Mediator.Publish(topic, msg, sendToEachGroup))
      }

      s"publish, sendToEachGroup: $sendToEachGroup" in new Scope {
        pubSub.publish(msg, sendToEachGroup = sendToEachGroup)
        expectMsgPF() { case Mediator.Publish(`topic`, ToBytesAble.Raw(`msg`), `sendToEachGroup`) => msg }
      }
    }

    "topics" in new Scope {
      val future = pubSub.topics()
      expectMsg(Mediator.GetTopics)
      val topics = Set("topic")
      lastSender ! Mediator.CurrentTopics(topics)
      Await.result(future, timeout.duration) shouldEqual topics
    }
  }

  private trait Scope extends ActorScope {
    val probe = TestProbe()
    def ref = probe.ref
    val pubSub = PubSub(testActor, ActorLog.empty)
  }
}