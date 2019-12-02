package com.evolutiongaming.cluster.pubsub

import akka.cluster.pubsub.{DistributedPubSubMediator => Mediator}
import akka.testkit.{TestActorRef, TestProbe}
import com.evolutiongaming.cluster.pubsub.LocalPubSub._
import org.scalatest.concurrent.Eventually

import scala.concurrent.duration._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LocalPubSubSpec extends AnyFlatSpec with ActorSpec with Matchers with Eventually {

  "LocalPubSub" should "receive Subscribe" in new Scope {
    actor ! Mediator.Subscribe(topic1, probe1.ref)
    state() shouldEqual Map(topic1 -> Set(probe1.ref))

    actor ! Mediator.Subscribe(topic2, probe2.ref)
    state() shouldEqual Map(topic1 -> Set(probe1.ref), topic2 -> Set(probe2.ref))

    actor ! Mediator.Subscribe(topic1, probe2.ref)
    state() shouldEqual Map(topic1 -> Set(probe1.ref, probe2.ref), topic2 -> Set(probe2.ref))

    actor ! Mediator.Subscribe(topic2, probe1.ref)
    state() shouldEqual Map(topic1 -> Set(probe1.ref, probe2.ref), topic2 -> Set(probe1.ref, probe2.ref))
  }

  it should "receive Unsubscribe" in new Scope {
    actor ! Mediator.Subscribe(topic1, probe1.ref)
    actor ! Mediator.Unsubscribe(topic2, probe1.ref)
    actor ! Mediator.Unsubscribe(topic1, probe2.ref)
    state() shouldEqual Map(topic1 -> Set(probe1.ref))

    actor ! Mediator.Unsubscribe(topic1, probe1.ref)
    state() shouldEqual Map()

    actor ! Mediator.Subscribe(topic1, probe1.ref)
    actor ! Mediator.Subscribe(topic1, probe2.ref)
    state() shouldEqual Map(topic1 -> Set(probe1.ref, probe2.ref))

    actor ! Mediator.Unsubscribe(topic1, probe2.ref)
    state() shouldEqual Map(topic1 -> Set(probe1.ref))
  }

  it should "receive Publish" in new Scope {
    actor ! Mediator.Subscribe(topic1, probe1.ref)
    actor ! Mediator.Publish(topic1, msg)
    probe1 expectMsg msg
    probe1.lastSender shouldEqual testActor

    actor ! Mediator.Publish(topic2, msg)
    probe1.expectNoMessage(500.millis)

    actor ! Mediator.Subscribe(topic1, probe2.ref)
    actor ! Mediator.Publish(topic1, msg)

    probe1 expectMsg msg
    probe1.lastSender shouldEqual testActor

    probe2 expectMsg msg
    probe2.lastSender shouldEqual testActor

    actor ! Mediator.Subscribe(topic2, probe1.ref)
    actor ! Mediator.Publish(topic2, msg)
    probe1 expectMsg msg
    probe2.expectNoMessage(500.millis)
  }

  it should "receive Terminated" in new Scope {
    actor ! Mediator.Subscribe(topic1, probe1.ref)
    actor ! Mediator.Subscribe(topic1, probe2.ref)
    actor ! Mediator.Publish(topic1, msg)
    probe1 expectMsg msg
    probe2 expectMsg msg
    state() shouldEqual Map(topic1 -> Set(probe1.ref, probe2.ref))

    watch(probe1.ref)
    system stop probe1.ref
    expectTerminated(probe1.ref)

    actor ! Mediator.Publish(topic1, msg)
    probe2 expectMsg msg

    eventually {
      state() shouldEqual Map(topic1 -> Set(probe2.ref))
    }
  }

  it should "receive Subscribe and reply with Subscribed" in new Scope {
    actor ! Mediator.Subscribe(topic1, Some(LocalPubSub.Ack), probe1.ref)
    probe1.expectMsg(Subscribed(topic1))

    actor ! Mediator.Subscribe(topic2, Some(LocalPubSub.Ack), probe1.ref)
    probe1.expectMsg(Subscribed(topic2))

    state() shouldEqual Map(topic1 -> Set(probe1.ref), topic2 -> Set(probe1.ref))
  }

  it should "receive GetTopics and reply with CurrentTopics" in new Scope {
    actor ! Mediator.GetTopics
    expectMsg(Mediator.CurrentTopics(Set.empty))

    actor ! Mediator.Subscribe(topic1, Some(LocalPubSub.Ack), testActor)
    expectMsg(Subscribed(topic1))

    actor ! Mediator.GetTopics
    expectMsg(Mediator.CurrentTopics(Set(topic1)))
  }

  private trait Scope extends ActorScope {
    val topic1 = "topic1"
    val topic2 = "topic2"
    val actor = TestActorRef(LocalPubSub.props)
    lazy val probe1 = TestProbe()
    lazy val probe2 = TestProbe()
    val msg = "msg"

    def state() = {
      actor ! GetState
      expectMsgType[State].value
    }
  }
}
