package com.evolutiongaming.cluster.pubsub

import akka.cluster.pubsub.DistributedPubSubMediator.{Subscribe, Unsubscribe}
import akka.testkit.{TestActorRef, TestProbe}
import com.codahale.metrics.Counter
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Matchers, WordSpec}

class PubSubMeteredActorSpec extends WordSpec with ActorSpec with Matchers with MockitoSugar {

  "LocalPub" should {
    "forward Subscribe" in new Scope {
      expectForwarded(Subscribe(topic, probe))
      verify(counter).inc()
    }

    "forward Unsubscribe" in new Scope {
      expectForwarded(Unsubscribe(topic, probe))
      verify(counter).dec()
    }

    "forward any" in new Scope {
      expectForwarded("any")
    }

    "receive Terminated" in new Scope {
      expectForwarded(Subscribe(topic, probe))
      expectForwarded(Subscribe("topic2", probe))

      watch(probe)
      system stop probe
      expectTerminated(probe)

      expectForwarded("delay")

      verify(counter, times(2)).inc()
      verify(counter).dec(2)
    }
  }

  private trait Scope extends ActorScope {
    val topic = "topic"
    val counter = mock[Counter]

    val actor = TestActorRef(PubSub.MeteredActor.props(testActor, counter))
    val probe = TestProbe().ref

    def expectForwarded(msg: Any) = {
      actor ! msg
      expectMsg(msg)
      lastSender shouldEqual testActor
    }
  }
}
