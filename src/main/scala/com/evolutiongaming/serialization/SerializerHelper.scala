package com.evolutiongaming.serialization

import java.nio.ByteBuffer
import java.nio.charset.{Charset, StandardCharsets}

object SerializerHelper {

  type Bytes = Array[Byte]

  val Utf8: Charset = StandardCharsets.UTF_8


  implicit class ByteBufferOps(val self: ByteBuffer) extends AnyVal {

    def readBytes: Bytes = {
      val length = self.getInt()
      val bytes = new Bytes(length)
      self.get(bytes)
      bytes
    }

    def writeBytes(bytes: Bytes): Unit = {
      self.putInt(bytes.length)
      self.put(bytes)
    }

    def readString: String = {
      new String(readBytes, Utf8)
    }

    def writeString(value: String): Unit = {
      val bytes = value.getBytes(Utf8)
      writeBytes(bytes)
    }
  }
}
