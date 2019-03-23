package com.evolutiongaming.cluster.pubsub

import scala.reflect.ClassTag

final case class Topic[-A](name: String)

object Topic {
  def apply[A](implicit tag: ClassTag[A]): Topic[A] = apply(tag.runtimeClass.getName)
}