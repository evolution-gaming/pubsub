package com.evolutiongaming.cluster.pubsub

import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.duration.{FiniteDuration, _}

class GroupWithinSpec extends WordSpec with ActorSpec with Matchers {

  "GroupWithin" should {

    "group msgs until given number reached" in new Scope(1.minute, 2) {
      groupWithin("1")
      groupWithin("2")
      expectMsg("12")

      groupWithin("3")
      groupWithin("4")
      expectMsg("34")
    }

    "group msgs until given timeout reached" in new Scope(300.millis, 100) {
      groupWithin("1")
      groupWithin("2")
      expectMsg("12")
    }
  }

  private abstract class Scope(delay: FiniteDuration, size: Int) extends ActorScope {
    val groupWithin = {
      val settings = GroupWithin.Settings(delay, size)
      GroupWithin[String](settings, system) { xs =>
        val x = xs.reduceLeft { _ + _ }
        testActor ! x
      }
    }
  }
}

