package com.evolutiongaming.cluster.pubsub

import akka.cluster.pubsub.{DistributedPubSubMediator => Mediator}
import akka.testkit.TestProbe
import com.evolutiongaming.safeakka.actor.{ActorLog, WithSender}
import org.scalatest.{Matchers, WordSpec}

class PubSubSpec extends WordSpec with ActorSpec with Matchers {

  "PubSub" should {

    "subscribe" in new Scope {
      pubSub.subscribe[Msg.type](ref, Some(group))
      expectMsg(Mediator.Subscribe(topic, Some(group), ref))
      lastSender shouldEqual probe.ref
    }

    "publish" in new Scope {
      pubSub publish WithSender(Msg)
      expectMsg(Mediator.Publish(topic, Msg))
    }

    "unsubscribe" in new Scope {
      pubSub.unsubscribe[Msg.type](ref, Some(group))
      expectMsg(Mediator.Unsubscribe(topic, Some(group), ref))
      lastSender shouldEqual probe.ref
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