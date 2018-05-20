package com.evolutiongaming.cluster.pubsub

import java.io.NotSerializableException
import java.lang.{Integer => JInt, Long => JLong}
import java.nio.ByteBuffer

import akka.cluster.pubsub.PubSubMsg
import akka.serialization.SerializerWithStringManifest
import com.evolutiongaming.serialization.SerializedMsg
import com.evolutiongaming.serialization.SerializerHelper._

class PubSubSerializer extends SerializerWithStringManifest {

  private val MsgManifest = "A"

  def identifier: Int = 314261278

  def manifest(x: AnyRef): String = {
    x match {
      case _: PubSubMsg => MsgManifest
      case _            => illegalArgument(s"Cannot serialize message of ${ x.getClass } in ${ getClass.getName }")
    }
  }

  def toBinary(x: AnyRef): Bytes = {
    x match {
      case x: PubSubMsg => msgToBinary(x)
      case _            => illegalArgument(s"Cannot serialize message of ${ x.getClass } in ${ getClass.getName }")
    }
  }

  def fromBinary(bytes: Bytes, manifest: String): AnyRef = {
    manifest match {
      case MsgManifest => msgFromBinary(bytes)
      case _           => notSerializable(s"Cannot deserialize message for manifest $manifest in ${ getClass.getName }")
    }
  }

  private def msgFromBinary(bytes: Bytes) = {
    val buffer = ByteBuffer.wrap(bytes)
    val identifier = buffer.getInt
    val timestamp = buffer.getLong
    val manifest = buffer.readString
    val msgBytes = buffer.readBytes
    PubSubMsg(SerializedMsg(identifier, manifest, msgBytes), timestamp)
  }

  private def msgToBinary(x: PubSubMsg) = {
    val serializedMsg = x.serializedMsg
    val manifest = serializedMsg.manifest.getBytes(Utf8)
    val bytes = serializedMsg.bytes
    val buffer = ByteBuffer.allocate(JInt.BYTES + JLong.BYTES + JInt.BYTES + manifest.length + JInt.BYTES + bytes.length)
    buffer.putInt(serializedMsg.identifier)
    buffer.putLong(x.timestamp)
    buffer.writeBytes(manifest)
    buffer.writeBytes(bytes)
    buffer.array()
  }

  private def notSerializable(msg: String) = throw new NotSerializableException(msg)

  private def illegalArgument(msg: String) = throw new IllegalArgumentException(msg)
}