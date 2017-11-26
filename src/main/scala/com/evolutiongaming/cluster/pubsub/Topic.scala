package com.evolutiongaming.cluster.pubsub

import scala.annotation.unchecked.uncheckedVariance
import scala.reflect.ClassTag

trait Topic[-T] {
  def str: String
  def tag: ClassTag[T @uncheckedVariance]
}

object Topic {

  case class Str[T](str: String, tag: ClassTag[T]) extends Topic[T] {
    override def productPrefix = "Topic.Str"
  }

  case class Tag[T](tag: ClassTag[T]) extends Topic[T] {
    def str: String = tag.runtimeClass.getName
    override def productPrefix = "Topic.Tag"
  }

  def apply[T](implicit tag: ClassTag[T]): Topic[T] = Tag(tag)

  def apply[T](str: String)(implicit tag: ClassTag[T]): Topic[T] = Str(str, tag)
}