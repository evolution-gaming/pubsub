package com.evolutiongaming.cluster.pubsub

import akka.cluster.pubsub.{DistributedPubSubMediator => Mediator}
import akka.testkit.TestProbe
import com.evolutiongaming.safeakka.actor.ActorLog
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.Await

class PubSubSpec extends WordSpec with ActorSpec with Matchers {

  "PubSub" should {

    "subscribeAny" in new Scope {
      pubSub.subscribeAny[Msg.type](ref, Some(group))
      expectMsg(Mediator.Subscribe(topic, Some(group), ref))
      lastSender shouldEqual probe.ref
    }

    "publishAny" in new Scope {
      pubSub publishAny Msg
      expectMsg(Mediator.Publish(topic, Msg))
    }

    "unsubscribe" in new Scope {
      pubSub.unsubscribe[Msg.type](ref, Some(group))
      expectMsg(Mediator.Unsubscribe(topic, Some(group), ref))
      lastSender shouldEqual probe.ref
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
    val group = "group"
    val topic = "topic"
    val probe = TestProbe()
    def ref = probe.ref
    val pubSub = PubSub(testActor, ActorLog.empty)

    case object Msg

    implicit val MsgTopic: Topic[Msg.type] = Topic[Msg.type](topic)
  }
}