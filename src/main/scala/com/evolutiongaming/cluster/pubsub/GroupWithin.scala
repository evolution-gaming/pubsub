package com.evolutiongaming.cluster.pubsub

import akka.actor.ActorRefFactory
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, OverflowStrategy, QueueOfferResult}
import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.nel.Nel

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

trait GroupWithin[-T] {
  def apply(value: T): Future[Unit]
  def stop(): Unit
}

object GroupWithin {

  type Fold[T] = Nel[T] => Unit


  private val futureUnit = Future.successful(())


  private lazy val Empty = new GroupWithin[Any] {
    def apply(value: Any): Future[Unit] = futureUnit
    def stop(): Unit = {}
  }

  def empty[T]: GroupWithin[T] = Empty


  def apply[T](
    settings: Settings,
    factory: ActorRefFactory)(
    fold: Fold[T]): GroupWithin[T] = {

    val folded = (msgs: Seq[T]) => {
      Nel.opt(msgs) foreach { msgs => fold(msgs) }
    }

    implicit val materializer = ActorMaterializer(namePrefix = Some("GroupWithin"))(factory)
    val queue = Source
      .queue[T](settings.buffer, OverflowStrategy.backpressure)
      .groupedWithin(settings.size, settings.delay)
      .to(Sink.foreach(folded))
      .run()

    implicit val ec = CurrentThreadExecutionContext

    new GroupWithin[T] {

      def apply(value: T): Future[Unit] = {

        def errorMsg = s"Failed to enqueue msg $value"

        def failed(message: String, cause: Option[Throwable]) = {
          Future.failed(new QueueException(message, cause))
        }

        queue.offer(value) flatMap {
          case QueueOfferResult.Enqueued         => futureUnit
          case QueueOfferResult.Failure(failure) => failed(errorMsg, Some(failure))
          case failure                           => failed(s"$errorMsg $failure", None)
        } recover {
          case failure => failed(errorMsg, Some(failure))
        }
      }

      def stop(): Unit = queue.complete()
    }
  }


  class QueueException(message: String, cause: Option[Throwable] = None) extends RuntimeException(message, cause.orNull)


  case class Settings(delay: FiniteDuration, size: Int, buffer: Int)

  object Settings {
    def apply(delay: FiniteDuration, size: Int): Settings = {
      Settings(delay, size = size, buffer = size * 100)
    }
  }


  trait Create {
    def apply[T](fold: Fold[T]): GroupWithin[T]
  }

  object Create {
    lazy val Empty: Create = new Create {
      def apply[T](fold: Fold[T]): GroupWithin[T] = GroupWithin.Empty
    }
  }
}
