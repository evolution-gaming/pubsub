package com.evolutiongaming.cluster.pubsub

import akka.actor.ActorRef
import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.safeakka.actor.{ActorLog, WithSender}

trait PublishGroupWithin[-T] {
  def apply(msg: T, sender: Option[ActorRef] = None): Unit
}


object PublishGroupWithin {

  def apply[T](
    pubSub: PubSub,
    createGroupWithin: GroupWithin.Create,
    log: ActorLog)(
    fold: Nel[T] => T)(
    implicit topic: Topic[T], toBytes: ToBytes[T]): PublishGroupWithin[T] = {

    val groupWithin = createGroupWithin[WithSender[T]] { msgs =>
      val msg = fold(msgs map { _.msg })
      val sender = msgs.head.sender
      pubSub.publish(msg, sender)
    }

    apply(groupWithin, log, topic)
  }

  def any[T](
    pubSub: PubSub,
    createGroupWithin: GroupWithin.Create,
    log: ActorLog)(
    fold: Nel[T] => T)(
    implicit topic: Topic[T]): PublishGroupWithin[T] = {

    val groupWithin = createGroupWithin[WithSender[T]] { msgs =>
      val msg = fold(msgs map { _.msg })
      val sender = msgs.head.sender
      pubSub.publishAny(msg, sender)
    }

    apply(groupWithin, log, topic)
  }

  private def apply[T](groupWithin: GroupWithin[WithSender[T]], log: ActorLog, topic: Topic[T]): PublishGroupWithin[T] = {
    implicit val ec = CurrentThreadExecutionContext
    new PublishGroupWithin[T] {
      def apply(msg: T, sender: Option[ActorRef]): Unit = {
        val withSender = WithSender(msg, sender)
        groupWithin(withSender).failed.foreach { failure =>
          log.error(s"Failed to enqueue msg ${ msg.getClass.getName } at ${ topic.name } $failure", failure)
        }
      }
    }
  }


  def empty[T]: PublishGroupWithin[T] = Empty


  class Proxy[-T](pubSub: PubSub)(implicit topic: Topic[T]) extends PublishGroupWithin[T] {
    def apply(msg: T, sender: Option[ActorRef]): Unit = pubSub.publishAny(msg, sender)
  }

  object Proxy {

    def apply[T](ref: ActorRef)(implicit topic: Topic[T]): Proxy[T] = Proxy[T](PubSub.Proxy(ref))

    def apply[T](pubSub: PubSub)(implicit topic: Topic[T]): Proxy[T] = new Proxy[T](pubSub)
  }


  private object Empty extends PublishGroupWithin[Any] {
    def apply(msg: Any, sender: Option[ActorRef]): Unit = {}
  }
}