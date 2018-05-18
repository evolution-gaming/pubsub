package com.evolutiongaming.cluster.pubsub

import akka.actor.ActorRef

trait Publish {
  def apply[T](msg: T)(implicit topic: Topic[T]): Unit
}

object Publish {

  lazy val Empty: Publish = new Publish {
    def apply[T](msg: T)(implicit topic: Topic[T]): Unit = {}
  }

  def apply(sender: Option[ActorRef], pubSub: PubSub): Publish = new Publish {
    def apply[T](msg: T)(implicit topic: Topic[T]): Unit = pubSub.publish(msg, sender)
  }
}
