package com.evolutiongaming.cluster.pubsub

import akka.cluster.pubsub.{DistributedPubSubMediator => Mediator}
import akka.testkit.TestProbe
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits._
import com.evolutiongaming.catshelper.CatsHelper._
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.serialization.ToBytesAble

import scala.concurrent.Await
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.mutable

class PubSubSpec extends AnyWordSpec with ActorSpec with Matchers {

  val topic = "topic"
  val msg = "msg"
  type Msg = String
  implicit val MsgTopic: Topic[Msg] = Topic[Msg](topic)

  "PubSub" should {

    for {
      group <- List(Some("group"), None)
    } {
      s"subscribe, group: $group" in new Scope {
        val msgs = mutable.ArrayBuffer[String]()
        val (_, unsubscribe) = pubSub.subscribe[Msg](group) { (msg: Msg, _) => IO(msgs.addOne(msg)) }.allocated.toTry.get
        val subscriber = expectMsgPF() { case Mediator.Subscribe(`topic`, `group`, ref) => ref }

        subscriber ! ToBytesAble.Raw("msg1")(ToBytes.StrToBytes.apply)
        Thread.sleep(100)
        msgs.toVector shouldBe Vector("msg1")

        subscriber ! ToBytesAble.Raw("msg2")(ToBytes.StrToBytes.apply)
        Thread.sleep(100)
        msgs.toVector shouldBe Vector("msg1", "msg2")

        unsubscribe.toTry.get
        expectMsg(Mediator.Unsubscribe(topic, group, subscriber))
      }
    }

    for {
      sendToEachGroup <- List(true, false)
    } {
      s"publish, sendToEachGroup: $sendToEachGroup" in new Scope {
        pubSub.publish(msg, sendToEachGroup = sendToEachGroup).toFuture
        expectMsgPF() { case Mediator.Publish(`topic`, ToBytesAble.Raw(`msg`), `sendToEachGroup`) => msg }
      }
    }

    "topics" in new Scope {
      val future = pubSub.topics().toFuture
      expectMsg(Mediator.GetTopics)
      val topics = Set("topic")
      lastSender ! Mediator.CurrentTopics(topics)
      Await.result(future, timeout.duration) shouldEqual topics
    }
  }

  private trait Scope extends ActorScope {
    val probe = TestProbe()
    def ref = probe.ref
    val pubSub = PubSub[IO](testActor, ActorLog.empty, system)
  }
}