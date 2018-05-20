package com.evolutiongaming.cluster.pubsub

import com.evolutiongaming.serialization.SerializerHelper._

trait ToBytes[T] {
  def apply(value: T): Array[Byte]
}

object ToBytes {
  implicit val StrToBytes: ToBytes[String] = new ToBytes[String] {
    def apply(value: String): Array[Byte] = value.getBytes(Utf8)
  }
}


trait FromBytes[T] {
  def apply(bytes: Array[Byte]): T
}

object FromBytes {
  implicit val StrFromBytes: FromBytes[String] = new FromBytes[String] {
    def apply(bytes: Array[Byte]): String = new String(bytes, Utf8)
  }
}
