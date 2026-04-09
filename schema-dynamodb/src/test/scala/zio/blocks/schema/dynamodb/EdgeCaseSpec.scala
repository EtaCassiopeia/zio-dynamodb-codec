package zio.blocks.schema.dynamodb

import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import zio.blocks.schema.*
import zio.test.*

object EdgeCaseSpec extends ZIOSpecDefault:

  case class SimpleRec(x: String, y: Int) derives Schema

  def spec = suite("EdgeCaseSpec")(
    suite("Primitives")(
      test("String round-trip") {
        val codec = DynamoDB.codec[String]
        val av    = codec.encodeValue("hello")
        assertTrue(av.s() == "hello", codec.decodeValue(av) == Right("hello"))
      },
      test("Int round-trip") {
        val codec = DynamoDB.codec[Int]
        val av    = codec.encodeValue(42)
        assertTrue(av.n() == "42", codec.decodeValue(av) == Right(42))
      },
      test("Long round-trip") {
        val codec = DynamoDB.codec[Long]
        val av    = codec.encodeValue(123456789L)
        assertTrue(av.n() == "123456789", codec.decodeValue(av) == Right(123456789L))
      },
      test("Double round-trip") {
        val codec = DynamoDB.codec[Double]
        val av    = codec.encodeValue(3.14)
        assertTrue(av.n() == "3.14", codec.decodeValue(av) == Right(3.14))
      },
      test("Boolean round-trip") {
        val codec = DynamoDB.codec[Boolean]
        val av    = codec.encodeValue(true)
        assertTrue(av.bool() == true, codec.decodeValue(av) == Right(true))
      }
    ),
    suite("Records")(
      test("simple record round-trip") {
        val codec = DynamoDB.codec[SimpleRec]
        val value = SimpleRec("hello", 42)
        val map   = new java.util.HashMap[String, AttributeValue]()
        codec.encode(value, map)
        val back = codec.decode(map)
        assertTrue(back == Right(value))
      },
      test("missing field returns error") {
        val codec = DynamoDB.codec[SimpleRec]
        val map   = new java.util.HashMap[String, AttributeValue]()
        map.put("x", AttributeValue.builder().s("hello").build())
        val back = codec.decode(map)
        assertTrue(back.isLeft)
      }
    )
  )
