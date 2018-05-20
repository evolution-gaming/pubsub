package com.evolutiongaming.cluster.pubsub

import akka.cluster.pubsub.PubSubMsg
import com.evolutiongaming.serialization.SerializedMsg
import org.scalatest.{FunSuite, Matchers}

class PubSubSerializerSpec extends FunSuite with Matchers {

  private val serializer = new PubSubSerializer()

  test("toBinary & fromBinary for PubSubMsg") {
    val expected = PubSubMsg(SerializedMsg(1, "manifest", Array(3, 4)), 2L)
    val actual = toAndFromBinary(expected)
    actual.timestamp shouldEqual expected.timestamp
    actual.serializedMsg.identifier shouldEqual expected.serializedMsg.identifier
    actual.serializedMsg.manifest shouldEqual expected.serializedMsg.manifest
    actual.serializedMsg.bytes.toList shouldEqual expected.serializedMsg.bytes.toList
  }

  test("manifest for PubSubMsg") {
    val msg = PubSubMsg(SerializedMsg(1, "manifest", Array(3, 4)), 2L)
    serializer.manifest(msg) shouldEqual "A"
  }

  def toAndFromBinary[T <: AnyRef](msg: T): T = {
    val manifest = serializer.manifest(msg)
    val bytes = serializer.toBinary(msg)
    val deserialized = serializer.fromBinary(bytes, manifest)
    deserialized.asInstanceOf[T]
  }
}