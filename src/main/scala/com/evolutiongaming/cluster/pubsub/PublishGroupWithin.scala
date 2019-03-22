package com.evolutiongaming.cluster.pubsub

import akka.actor.ActorRef
import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.safeakka.actor.{ActorLog, WithSender}

trait PublishGroupWithin[-T] {
  def apply(msg: T, sender: Option[ActorRef] = None): Unit
}


object PublishGroupWithin {

  def apply[A](
    pubSub: PubSub,
    createGroupWithin: GroupWithin.Create,
    log: ActorLog)(
    fold: Nel[A] => A)(
    implicit topic: Topic[A], toBytes: ToBytes[A]): PublishGroupWithin[A] = {

    val groupWithin = createGroupWithin[WithSender[A]] { msgs =>
      val msg = fold(msgs map { _.msg })
      val sender = msgs.head.sender
      pubSub.publish(msg, sender)
    }

    apply(groupWithin, log, topic)
  }

  def any[A](
    pubSub: PubSub,
    createGroupWithin: GroupWithin.Create,
    log: ActorLog)(
    fold: Nel[A] => A)(
    implicit topic: Topic[A]): PublishGroupWithin[A] = {

    val groupWithin = createGroupWithin[WithSender[A]] { msgs =>
      val msg = fold(msgs map { _.msg })
      val sender = msgs.head.sender
      pubSub.publishAny(msg, sender)
    }

    apply(groupWithin, log, topic)
  }

  def apply[A](groupWithin: GroupWithin[WithSender[A]], log: ActorLog, topic: Topic[A]): PublishGroupWithin[A] = {
    implicit val ec = CurrentThreadExecutionContext
    new PublishGroupWithin[A] {
      def apply(msg: A, sender: Option[ActorRef]): Unit = {
        val withSender = WithSender(msg, sender)
        groupWithin(withSender).failed.foreach { failure =>
          log.error(s"Failed to enqueue msg ${ msg.getClass.getName } at ${ topic.name } $failure", failure)
        }
      }
    }
  }


  def empty[A]: PublishGroupWithin[A] = Empty


  class Proxy[-A](pubSub: PubSub)(implicit topic: Topic[A]) extends PublishGroupWithin[A] {
    def apply(msg: A, sender: Option[ActorRef]): Unit = pubSub.publishAny(msg, sender)
  }

  object Proxy {

    def apply[A](ref: ActorRef)(implicit topic: Topic[A]): Proxy[A] = Proxy[A](PubSub.proxy(ref))

    def apply[A](pubSub: PubSub)(implicit topic: Topic[A]): Proxy[A] = new Proxy[A](pubSub)
  }


  private object Empty extends PublishGroupWithin[Any] {
    def apply(msg: Any, sender: Option[ActorRef]): Unit = {}
  }
}