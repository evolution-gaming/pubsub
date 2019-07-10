package com.evolutiongaming.cluster.pubsub

import akka.actor.ActorRef
import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.safeakka.actor.{ActorLog, WithSender}

trait PublishGroupWithin[-A] {

  def apply(msg: A, sender: Option[ActorRef] = None): Unit
}


object PublishGroupWithin {

  def empty[A]: PublishGroupWithin[A] = new PublishGroupWithin[Any] {
    def apply(msg: Any, sender: Option[ActorRef]): Unit = {}
  }


  def proxy[A: Topic : ToBytes](pubSub: PubSub): PublishGroupWithin[A] = {
    new PublishGroupWithin[A] {
      def apply(msg: A, sender: Option[ActorRef]) = pubSub.publish(msg, sender)
    }
  }


  def apply[A](
    pubSub: PubSub,
    createGroupWithin: GroupWithin.Create,
    log: ActorLog)(
    fold: Nel[A] => A)(implicit
    topic: Topic[A],
    toBytes: ToBytes[A]
  ): PublishGroupWithin[A] = {

    val groupWithin = createGroupWithin[WithSender[A]] { msgs =>
      val msg = fold(msgs map { _.msg })
      val sender = msgs.head.sender
      pubSub.publish(msg, sender)
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
}