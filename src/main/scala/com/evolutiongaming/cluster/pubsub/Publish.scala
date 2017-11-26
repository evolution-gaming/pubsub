package com.evolutiongaming.cluster.pubsub

import akka.actor.ActorRef
import com.evolutiongaming.safeakka.actor.WithSender

trait Publish {
  def apply[T](msg: T)(implicit topic: Topic[T]): Unit
}

object Publish {

  def empty: Publish = Empty

  def apply(sender: ActorRef, pubSub: PubSub): Publish = new Publish {
    def apply[T](msg: T)(implicit topic: Topic[T]): Unit = pubSub.publish(WithSender(msg, sender))
  }

  private object Empty extends Publish {
    def apply[T](msg: T)(implicit topic: Topic[T]): Unit = {}
  }
}
