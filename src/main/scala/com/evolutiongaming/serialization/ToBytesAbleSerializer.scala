package com.evolutiongaming.serialization

import java.io.NotSerializableException

import akka.serialization.SerializerWithStringManifest
import com.evolutiongaming.serialization.SerializerHelper._


class ToBytesAbleSerializer extends SerializerWithStringManifest {

  private val Manifest = "A"

  def identifier: Int = 59156265

  def manifest(x: AnyRef): String = x match {
    case _: ToBytesAble => Manifest
    case _              => illegalArgument(s"Cannot serialize message of ${ x.getClass } in ${ getClass.getName }")
  }

  def toBinary(x: AnyRef): Bytes = {
    x match {
      case x: ToBytesAble => x.bytes()
      case _              => illegalArgument(s"Cannot serialize message of ${ x.getClass } in ${ getClass.getName }")
    }
  }

  def fromBinary(bytes: Bytes, manifest: String): AnyRef = {
    manifest match {
      case Manifest => ToBytesAble.Bytes(bytes)
      case _        => notSerializable(s"Cannot deserialize message for manifest $manifest in ${ getClass.getName }")
    }
  }

  private def notSerializable(msg: String) = throw new NotSerializableException(msg)

  private def illegalArgument(msg: String) = throw new IllegalArgumentException(msg)
}