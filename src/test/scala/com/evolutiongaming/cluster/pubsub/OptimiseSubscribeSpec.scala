package com.evolutiongaming.cluster.pubsub

import akka.actor.ActorPath
import akka.testkit.TestActors
import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, IO, Resource, Sync}
import cats.implicits._
import cats.temp.par._
import com.evolutiongaming.cluster.pubsub.IOSuite._
import com.evolutiongaming.cluster.pubsub.PubSub.OnMsg
import org.scalatest.{AsyncFunSuite, Matchers}

class OptimiseSubscribeSpec extends AsyncFunSuite with ActorSpec with Matchers {

  test("subscribe once and dispatch msgs to all subscribers") {
    `subscribe once and dispatch msgs to all subscribers`[IO].run()
  }

  private def `subscribe once and dispatch msgs to all subscribers`[F[_] : Concurrent : Par] = {

    type Msg = String

    implicit val topic = Topic[String]

    val sender = Resource.make {
      Sync[F].delay { system.actorOf(TestActors.blackholeProps) }
    } { actorRef =>
      Sync[F].delay { system.stop(actorRef) }
    }

    sender.use { sender =>
      for {
        optimiseSubscribe <- OptimiseSubscribe.of[F]
        msgsRef           <- Ref[F].of(Set.empty[Msg])
        listenersRef      <- Ref[F].of(List.empty[OnMsg[F, Msg]])
        publish            = (msg: Msg) => for {
          listeners <- listenersRef.get
          _         <- listeners.foldMapM { onMsg => onMsg(msg, sender.path) }

        } yield {}

        subscribe          = (prefix: String) => {
          val onMsg = (msg: Msg, _: ActorPath) => msgsRef.update { _ + s"$prefix-$msg" }
          val subscribe = (onMsg: OnMsg[F, Msg]) => {
            val result = for {
              _ <- listenersRef.update { onMsg :: _ }
            } yield {
              val release = listenersRef.update { _.filter(_ != onMsg) }
              ((), release)
            }
            Resource(result)
          }

          for {
            a <- optimiseSubscribe[Msg](onMsg)(subscribe).allocated
          } yield {
            val (_, release) = a
            release
          }
        }

        listeners0        <- listenersRef.get
        _                 <- publish("0")
        msgs0             <- msgsRef.get
        unsubscribe0      <- subscribe("1")
        listeners1        <- listenersRef.get
        _                 <- publish("1")
        msgs1             <- msgsRef.get
        unsubscribe1      <- subscribe("2")
        listeners2        <- listenersRef.get
         _                <- publish("2")
        msgs2             <- msgsRef.get
        _                 <- unsubscribe0
        listeners3        <- listenersRef.get
        _                 <- unsubscribe1
        listeners4        <- listenersRef.get
      } yield {
        listeners0.size shouldEqual 0
        msgs0 shouldEqual Set.empty[String]
        listeners1.size shouldEqual 1
        msgs1 shouldEqual Set("1-1")
        listeners2.size shouldEqual 1
        msgs2 shouldEqual Set("1-2", "2-2", "1-1")
        listeners3.size shouldEqual 1
        listeners4.size shouldEqual 0
      }
    }
  }
}
