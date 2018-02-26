package com.evolutiongaming.cluster.pubsub

import java.nio.ByteBuffer

import akka.cluster.pubsub.MsgBytes
import akka.serialization.SerializerWithStringManifest

class PubSubSerializer extends SerializerWithStringManifest {

  def identifier: Int = 314261278

  private val MsgBytesManifest = "MsgBytes"

  def manifest(x: AnyRef): String = {
    x match {
      case _: MsgBytes => MsgBytesManifest
      case _           => sys.error(s"Cannot serialize message of ${ x.getClass } in ${ getClass.getName }")
    }
  }

  def toBinary(x: AnyRef): Array[Byte] = {
    x match {
      case x: MsgBytes => msgBytesToBinary(x)
      case _           => sys.error(s"Cannot serialize message of ${ x.getClass } in ${ getClass.getName }")
    }
  }

  def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = {
    manifest match {
      case MsgBytesManifest => msgBytesFromBinary(bytes)
      case _                => sys.error(s"Cannot deserialize message for manifest $manifest in ${ getClass.getName }")
    }
  }

  private def msgBytesFromBinary(bytes: Array[Byte]) = {
    val buffer = ByteBuffer.wrap(bytes)
    val identifier = buffer.getInt
    val timestamp = buffer.getLong
    val manifest = readStr(buffer)
    val msgBytes = new Array[Byte](buffer.remaining())
    buffer.get(msgBytes)
    MsgBytes(identifier, timestamp, manifest, msgBytes)
  }

  private def msgBytesToBinary(x: MsgBytes) = {
    val manifestBytes = x.manifest.getBytes("UTF-8")
    val manifestLength = manifestBytes.length
    val buffer = ByteBuffer.allocate(4 + 8 + 4 + manifestLength + x.bytes.length)
    buffer.putInt(x.identifier)
    buffer.putLong(x.timestamp)
    buffer.putInt(manifestLength)
    buffer.put(manifestBytes)
    buffer.put(x.bytes)
    buffer.array()
  }

  private def readStr(buffer: ByteBuffer) = {
    val length = buffer.getInt()
    val bytes = new Array[Byte](length)
    buffer.get(bytes)
    new String(bytes, "UTF-8")
  }
}
