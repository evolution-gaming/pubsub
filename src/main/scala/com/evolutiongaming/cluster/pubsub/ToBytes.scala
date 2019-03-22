package com.evolutiongaming.cluster.pubsub

import scodec.bits.ByteVector

trait ToBytes[-A] {
  def apply(value: A): ByteVector
}

object ToBytes {
  implicit val StrToBytes: ToBytes[String] = (value: String) => {
    ByteVector.encodeUtf8(value).fold(throw _, identity)
  }

  implicit val BytesToBytes: ToBytes[ByteVector] = (value: ByteVector) => value
}


trait FromBytes[A] {
  def apply(bytes: ByteVector): A
}

object FromBytes {

  implicit val StrFromBytes: FromBytes[String] = (bytes: ByteVector) => {
    bytes.decodeUtf8.fold(throw _, identity)
  }

  implicit val BytesFromBytes: FromBytes[ByteVector] = (bytes: ByteVector) => bytes
}
