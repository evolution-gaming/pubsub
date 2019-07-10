package com.evolutiongaming.cluster.pubsub

import akka.actor.ActorRefFactory
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, OverflowStrategy, QueueOfferResult}
import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.nel.Nel

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

trait GroupWithin[-A] {

  def apply(value: A): Future[Unit]
  
  def stop(): Unit
}

object GroupWithin {

  def empty[A]: GroupWithin[A] = new GroupWithin[A] {

    def apply(value: A) = Future.unit

    def stop(): Unit = {}
  }


  def apply[A](
    settings: Settings,
    factory: ActorRefFactory)(
    fold: Nel[A] => Unit
  ): GroupWithin[A] = {

    val folded = (msgs: Seq[A]) => {
      Nel.opt(msgs) foreach { msgs => fold(msgs) }
    }

    implicit val materializer = ActorMaterializer(namePrefix = Some("GroupWithin"))(factory)
    val queue = Source
      .queue[A](settings.buffer, OverflowStrategy.backpressure)
      .groupedWithin(settings.size, settings.delay)
      .to(Sink.foreach(folded))
      .run()

    implicit val ec = CurrentThreadExecutionContext

    new GroupWithin[A] {

      def apply(value: A) = {

        def errorMsg = s"Failed to enqueue msg $value"

        def failed[B](message: String, cause: Option[Throwable]) = {
          Future.failed[B](new QueueException(message, cause))
        }

        queue.offer(value) flatMap {
          case QueueOfferResult.Enqueued         => Future.unit
          case QueueOfferResult.Failure(failure) => failed(errorMsg, Some(failure))
          case failure                           => failed(s"$errorMsg $failure", None)
        } recoverWith { case failure =>
          failed(errorMsg, Some(failure))
        }
      }

      def stop() = queue.complete()
    }
  }


  class QueueException(message: String, cause: Option[Throwable] = None) extends RuntimeException(message, cause.orNull)


  final case class Settings(delay: FiniteDuration, size: Int, buffer: Int)

  object Settings {
    def apply(delay: FiniteDuration, size: Int): Settings = {
      Settings(delay, size = size, buffer = size * 100)
    }
  }


  trait Create {
    def apply[A](fold: Nel[A] => Unit): GroupWithin[A]
  }

  object Create {

    def empty: Create = const(GroupWithin.empty)

    def const(value: GroupWithin[Any]): Create = new Create {
      def apply[A](fold: Nel[A] => Unit) = value
    }
  }
}
