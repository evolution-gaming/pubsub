package com.evolutiongaming.cluster.pubsub

import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.duration._

class PubSubGroupWithinSpec extends WordSpec with ActorSpec with Matchers {

  "PubSubGroupWithin" should {

    "group msgs until given number reached" in new Scope(1.minute, 2) {
      pubSubGroupWithin.publish("1", Some(testActor))
      pubSubGroupWithin.publish("2", Some(testActor))
      expectMsg("12")
      lastSender shouldEqual testActor

      pubSubGroupWithin.publish("3", Some(testActor))
      pubSubGroupWithin.publish("4", Some(testActor))
      expectMsg("34")
      lastSender shouldEqual testActor
    }

    "group msgs until given timeout reached" in new Scope(300.millis, 100) {
      pubSubGroupWithin.publish("1", Some(testActor))
      pubSubGroupWithin.publish("2", Some(testActor))
      expectMsg("12")
      lastSender shouldEqual testActor
    }
  }

  private abstract class Scope(duration: FiniteDuration, size: Int) extends ActorScope {
    val pubSub = PubSub.Proxy(testActor)
    implicit val topic = Topic[String]("test")
    val pubSubGroupWithin = PubSubGroupWithin[String](duration, size, pubSub, system) { _.reduceLeft { _ + _ } }
  }
}
