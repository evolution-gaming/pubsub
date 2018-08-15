package com.evolutiongaming.cluster.pubsub

import scala.reflect.ClassTag

trait Topic[-T] {
  def name: String
}

object Topic {

  def apply[T](implicit tag: ClassTag[T]): Topic[T] = apply(tag.runtimeClass.getName)

  def apply[T](name: String): Topic[T] = Impl(name)

  private final case class Impl[T](name: String) extends Topic[T] {
    override def productPrefix = s"Topic($name)"
  }
}