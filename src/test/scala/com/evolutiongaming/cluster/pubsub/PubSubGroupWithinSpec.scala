package com.evolutiongaming.cluster.pubsub

import com.evolutiongaming.nel.Nel
import com.evolutiongaming.safeakka.actor.ActorLog
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.Future

class PubSubGroupWithinSpec extends WordSpec with ActorSpec with Matchers {

  "PubSubGroupWithin" should {

    "proxy pubsub when folded" in new Scope {
      pubSubGroupWithin("msg", Some(testActor))
      expectMsg("msg")
      lastSender shouldEqual testActor
    }
  }

  private trait Scope extends ActorScope {
    val pubSub = PubSub.proxy(testActor)
    implicit val topic = Topic[String]("test")

    val createGroupWithin: GroupWithin.Create = new GroupWithin.Create {
      def apply[A](fold: Nel[A] => Unit) = new GroupWithin[A] {
        def apply(value: A): Future[Unit] = Future.successful(fold(Nel(value)))
        def stop(): Unit = {}
      }
    }
    val pubSubGroupWithin = PublishGroupWithin[String](pubSub, createGroupWithin, ActorLog.empty) { _.reduceLeft { _ + _ } }
  }
}
