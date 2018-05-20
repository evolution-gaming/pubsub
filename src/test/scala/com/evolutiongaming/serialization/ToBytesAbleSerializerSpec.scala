package com.evolutiongaming.serialization

import com.evolutiongaming.serialization.SerializerHelper.Utf8
import org.scalatest.{FunSuite, Matchers}

class ToBytesAbleSerializerSpec extends FunSuite with Matchers {

  private val serializer = new ToBytesAbleSerializer()

  test("toBinary & fromBinary for ToBytesAble.Raw") {
    val str = "value"
    val expected = ToBytesAble(str)(_.getBytes(Utf8))
    val actual = toAndFromBinary(expected)
    new String(actual.bytes(), Utf8) shouldEqual str
  }

  test("toBinary & fromBinary for ToBytesAble.Bytes") {
    val str = "value"
    val expected = ToBytesAble.Bytes(str.getBytes(Utf8))
    val actual = toAndFromBinary(expected)
    new String(actual.bytes(), Utf8) shouldEqual str
  }

  def toAndFromBinary[T <: AnyRef](msg: T): T = {
    val manifest = serializer.manifest(msg)
    val bytes = serializer.toBinary(msg)
    val deserialized = serializer.fromBinary(bytes, manifest)
    deserialized.asInstanceOf[T]
  }
}