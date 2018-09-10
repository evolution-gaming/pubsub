package com.evolutiongaming.cluster.pubsub

import com.evolutiongaming.serialization.SerializerHelper._

trait ToBytes[-T] {
  def apply(value: T): Bytes
}

object ToBytes {
  implicit val StrToBytes: ToBytes[String] = new ToBytes[String] {
    def apply(value: String): Bytes = value.getBytes(Utf8)
  }

  implicit val BytesToBytes: ToBytes[Bytes] = new ToBytes[Bytes] {
    def apply(value: Bytes): Bytes = value
  }
}


trait FromBytes[T] {
  def apply(bytes: Bytes): T
}

object FromBytes {

  implicit val StrFromBytes: FromBytes[String] = new FromBytes[String] {
    def apply(bytes: Bytes): String = new String(bytes, Utf8)
  }

  implicit val BytesFromBytes: FromBytes[Bytes] = new FromBytes[Bytes] {
    def apply(bytes: Bytes): Bytes = bytes
  }
}
