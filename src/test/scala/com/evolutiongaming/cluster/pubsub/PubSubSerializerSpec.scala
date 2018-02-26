package com.evolutiongaming.cluster.pubsub

import akka.cluster.pubsub.MsgBytes
import org.scalatest.{FunSuite, Matchers}

class PubSubSerializerSpec extends FunSuite with Matchers {

  private val serializer = new PubSubSerializer()

  test("toBinary & fromBinary for MsgBytes") {
    val expected = MsgBytes(1, 2L, "manifest", Array(3, 4))
    val bytes = serializer.toBinary(expected)
    val manifest = serializer.manifest(expected)
    val actual = serializer.fromBinary(bytes, manifest).asInstanceOf[MsgBytes]
    actual.timestamp shouldEqual expected.timestamp
    actual.identifier shouldEqual expected.identifier
    actual.bytes.toList shouldEqual expected.bytes.toList
  }

  test("manifest for MsgBytes") {
    val msgBytes = MsgBytes(1, 2L, "manifest", Array.empty)
    serializer.manifest(msgBytes) shouldEqual "MsgBytes"
  }
}