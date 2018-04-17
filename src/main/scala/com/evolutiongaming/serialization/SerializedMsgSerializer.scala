package com.evolutiongaming.serialization

import java.io.NotSerializableException
import java.lang.{Integer => JInt}
import java.nio.ByteBuffer

import akka.serialization.SerializerWithStringManifest
import com.evolutiongaming.serialization.SerializerHelper._

class SerializedMsgSerializer extends SerializerWithStringManifest {

  private val Manifest = "A"

  def identifier: Int = 1403526138

  def manifest(x: AnyRef): String = x match {
    case _: SerializedMsg => Manifest
    case _                => illegalArgument(s"Cannot serialize message of ${ x.getClass } in ${ getClass.getName }")
  }

  def toBinary(x: AnyRef): Bytes = {
    x match {
      case x: SerializedMsg => SerializedMsgSerializer.toBinary(x)
      case _                => illegalArgument(s"Cannot serialize message of ${ x.getClass } in ${ getClass.getName }")
    }
  }

  def fromBinary(bytes: Bytes, manifest: String): AnyRef = {
    manifest match {
      case Manifest => SerializedMsgSerializer.fromBinary(bytes)
      case _        => notSerializable(s"Cannot deserialize message for manifest $manifest in ${ getClass.getName }")
    }
  }

  private def notSerializable(msg: String) = throw new NotSerializableException(msg)

  private def illegalArgument(msg: String) = throw new IllegalArgumentException(msg)
}

object SerializedMsgSerializer {

  def toBinary(x: SerializedMsg): Bytes = {
    val manifest = x.manifest.getBytes(Utf8)
    val bytes = x.bytes
    val buffer = ByteBuffer.allocate(JInt.BYTES + JInt.BYTES + manifest.length + JInt.BYTES + bytes.length)
    buffer.putInt(x.identifier)
    buffer.writeBytes(manifest)
    buffer.writeBytes(bytes)
    buffer.array()
  }

  def fromBinary(bytes: Bytes): SerializedMsg = {
    val buffer = ByteBuffer.wrap(bytes)
    val identifier = buffer.getInt
    val manifest = buffer.readString
    val msgBytes = buffer.readBytes
    SerializedMsg(identifier, manifest, msgBytes)
  }
}
