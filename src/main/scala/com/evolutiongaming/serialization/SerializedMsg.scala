package com.evolutiongaming.serialization

import akka.actor.{ExtendedActorSystem, Extension, ExtensionId}
import akka.serialization.{Serialization, SerializationExtension, SerializerWithStringManifest}
import com.evolutiongaming.serialization.SerializerHelper.Bytes

import scala.util.Try

final case class SerializedMsg(identifier: Int, manifest: String, bytes: Bytes)


trait SerializedMsgConverter extends Extension {
  def toMsg(msg: AnyRef): SerializedMsg
  def fromMsg(msg: SerializedMsg): Try[AnyRef]
}

object SerializedMsgConverter {
  def apply(serialization: Serialization): SerializedMsgConverter = {
    new SerializedMsgConverter {

      def toMsg(msg: AnyRef): SerializedMsg = {
        msg match {
          case msg: SerializedMsg => msg
          case _             =>
            val serializer = serialization.findSerializerFor(msg)
            val bytes = serializer.toBinary(msg)
            val manifest = serializer match {
              case serializer: SerializerWithStringManifest => serializer.manifest(msg)
              case _ if serializer.includeManifest          => msg.getClass.getName
              case _                                        => ""
            }
            SerializedMsg(serializer.identifier, manifest, bytes)
        }
      }

      def fromMsg(msg: SerializedMsg) = {
        import msg._
        if (manifest.isEmpty) serialization.deserialize(bytes, identifier, None)
        else serialization.deserialize(bytes, identifier, manifest)
      }
    }
  }
}


object SerializedMsgExt extends ExtensionId[SerializedMsgConverter] {

  def createExtension(system: ExtendedActorSystem): SerializedMsgConverter = {
    val serialization = SerializationExtension(system)
    SerializedMsgConverter(serialization)
  }
}