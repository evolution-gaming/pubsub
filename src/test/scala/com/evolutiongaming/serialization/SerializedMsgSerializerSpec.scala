package com.evolutiongaming.serialization

import com.evolutiongaming.serialization.SerializerHelper._
import org.scalatest.{FunSuite, Matchers}

class SerializedMsgSerializerSpec extends FunSuite with Matchers {

  test("toBinary & fromBinary for SerializedMsg") {
    val bytes = "bytes"
    val expected = SerializedMsg(1, "manifest", bytes.getBytes(Utf8))
    val actual = toAndFromBinary(expected)
    actual.identifier shouldEqual actual.identifier
    actual.manifest shouldEqual actual.manifest
    new String(actual.bytes, Utf8) shouldEqual bytes
  }

  def toAndFromBinary(msg: SerializedMsg) = {
    val bytes = SerializedMsgSerializer.toBinary(msg)
    SerializedMsgSerializer.fromBinary(bytes)
  }
}
