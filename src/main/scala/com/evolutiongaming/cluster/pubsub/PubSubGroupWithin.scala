package com.evolutiongaming.cluster.pubsub

import akka.actor.{ActorRef, ActorRefFactory}
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, OverflowStrategy, QueueOfferResult}
import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.safeakka.actor.WithSender
import com.typesafe.scalalogging.LazyLogging

import scala.collection.immutable.Seq
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}

trait PubSubGroupWithin[-T] {
  def publish(msg: T, sender: Option[ActorRef]): Unit
}


object PubSubGroupWithin {

  def apply[T](
    timeout: FiniteDuration,
    size: Int,
    pubSub: PubSub,
    factory: ActorRefFactory)
    (group: Nel[T] => T)
    (implicit topic: Topic[T]): PubSubGroupWithin[T] = {

    def publish(msgs: Seq[WithSender[T]]): Unit = {
      Nel.opt(msgs) foreach { msgs =>
        val msg = group(msgs map { _.msg })
        val sender = msgs.head.sender
        pubSub.publish(msg, sender)(topic)
      }
    }

    val bufferSize = size * 100
    val materializer = ActorMaterializer(namePrefix = Some("PubSubGroupWithin"))(factory)
    val queue = Source
      .queue[WithSender[T]](bufferSize, OverflowStrategy.backpressure)
      .groupedWithin(size, timeout)
      .to(Sink.foreach(publish))
      .run()(materializer)

    implicit val ec = CurrentThreadExecutionContext

    new PubSubGroupWithin[T] with LazyLogging {

      def publish(msg: T, sender: Option[ActorRef]): Unit = {

        def errorMsg = s"Failed to enqueue msg ${ msg.getClass.getName } at: ${ topic.str }"

        val withSender = WithSender(msg, sender)
        queue.offer(withSender) onComplete {
          case Success(QueueOfferResult.Enqueued)         =>
          case Success(QueueOfferResult.Failure(failure)) => logger.error(errorMsg, failure)
          case Success(failure)                           => logger.error(s"$errorMsg $failure")
          case Failure(failure)                           => logger.error(errorMsg, failure)
        }
      }
    }
  }


  def empty[T]: PubSubGroupWithin[T] = Empty


  class Proxy[-T](pubSub: PubSub)(implicit topic: Topic[T]) extends PubSubGroupWithin[T] {
    def publish(msg: T, sender: Option[ActorRef]): Unit = pubSub.publish(msg, sender)
  }

  object Proxy {

    def apply[T](ref: ActorRef)(implicit topic: Topic[T]): Proxy[T] = Proxy[T](PubSub.Proxy(ref))

    def apply[T](pubSub: PubSub)(implicit topic: Topic[T]): Proxy[T] = new Proxy[T](pubSub)
  }


  private object Empty extends PubSubGroupWithin[Any] {
    def publish(msg: Any, sender: Option[ActorRef]): Unit = {}
  }
}
