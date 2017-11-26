package com.evolutiongaming.cluster.pubsub

import akka.actor.{ActorRef, ActorRefFactory}
import akka.stream.scaladsl.{Sink, Source, SourceQueueWithComplete}
import akka.stream.{ActorMaterializer, OverflowStrategy, QueueOfferResult}
import com.evolutiongaming.safeakka.actor.WithSender
import com.evolutiongaming.nel.Nel
import com.typesafe.scalalogging.LazyLogging

import scala.collection.immutable.Seq
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}

trait PubSubGroupWithin[-T] {
  def publish(msg: WithSender[T]): Unit
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
        pubSub.publish(WithSender(msg, sender))(topic)
      }
    }

    val bufferSize = size * 100
    val materializer = ActorMaterializer(namePrefix = Some("PubSubGroupWithin"))(factory)
    val queue = Source
      .queue[WithSender[T]](bufferSize, OverflowStrategy.backpressure)
      .groupedWithin(size, timeout)
      .to(Sink.foreach(publish))
      .run()(materializer)

    new Impl(group, queue, topic)(factory.dispatcher)
  }

  def empty[T]: PubSubGroupWithin[T] = Empty


  class Impl[-T](
    group: Nel[T] => T,
    queue: SourceQueueWithComplete[WithSender[T]],
    topic: Topic[T])(implicit ec: ExecutionContext) extends PubSubGroupWithin[T] with LazyLogging {

    def publish(msg: WithSender[T]): Unit = {
      
      def errorMsg = s"Failed to enqueue msg ${ msg.getClass.getName } at: ${ topic.str }"

      queue.offer(msg) onComplete {
        case Success(QueueOfferResult.Enqueued)         =>
        case Success(QueueOfferResult.Failure(failure)) => logger.error(errorMsg, failure)
        case Success(failure)                           => logger.error(s"$errorMsg $failure")
        case Failure(failure)                           => logger.error(errorMsg, failure)
      }
    }
  }


  class Proxy[-T](pubSub: PubSub)(implicit topic: Topic[T]) extends PubSubGroupWithin[T] {
    def publish(msg: WithSender[T]): Unit = pubSub.publish(msg)
  }

  object Proxy {

    def apply[T](ref: ActorRef)(implicit topic: Topic[T]): Proxy[T] = Proxy[T](PubSub.Proxy(ref))

    def apply[T](pubSub: PubSub)(implicit topic: Topic[T]): Proxy[T] = new Proxy[T](pubSub)
  }


  private object Empty extends PubSubGroupWithin[Any] {
    def publish(msg: WithSender[Any]): Unit = {}
  }
}
