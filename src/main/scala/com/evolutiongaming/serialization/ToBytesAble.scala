package com.evolutiongaming.serialization

/**
  * provides ability to have compile time check for serialization presence for remote messages
  * and use passed serialization during remoting
  */
sealed trait ToBytesAble extends Product with Serializable {
  def bytes(): Array[Byte]
}

object ToBytesAble {

  type ToBytes[T] = T => Array[Byte]

  def apply[T](msg: T)(toBytes: ToBytes[T]): ToBytesAble = Raw(msg, toBytes)


  case class Raw[T](msg: T, toBytes: ToBytes[T]) extends ToBytesAble {
    def bytes(): Array[Byte] = toBytes(msg)
  }

  case class Bytes(value: Array[Byte]) extends ToBytesAble {
    def bytes(): Array[Byte] = value
  }
}