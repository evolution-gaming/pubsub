package com.evolutiongaming.cluster.pubsub

import scala.reflect.ClassTag

final case class Topic[-T](name: String)

object Topic {
  def apply[T](implicit tag: ClassTag[T]): Topic[T] = apply(tag.runtimeClass.getName)
}