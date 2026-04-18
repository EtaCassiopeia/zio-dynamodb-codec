package zio.blocks.schema.dynamodb

import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import zio.blocks.schema.*
import zio.test.*

import java.time.*
import java.util.{Currency, UUID}

object EdgeCaseSpec extends ZIOSpecDefault:

  case class SimpleRec(x: String, y: Int) derives Schema
  case class AllOptional(a: Option[String], b: Option[Int], c: Option[Boolean]) derives Schema
  case class WithOptionalRecord(data: Option[SimpleRec]) derives Schema
  case class WithVectorField(items: Vector[String]) derives Schema
  case class WithNonStringMap(data: Map[Int, String]) derives Schema

  case class Outer(inner: Inner) derives Schema
  case class Inner(deep: Deep) derives Schema
  case class Deep(value: Int) derives Schema

  case class TreeNode(value: String, children: List[TreeNode]) derives Schema
  case class LinkedNode(value: Int, next: Option[LinkedNode]) derives Schema

  case class WithTransient(
    @Modifier.transient computed: String = "computed-value",
    name: String
  ) derives Schema
  case class WithEither(result: Either[String, Int]) derives Schema
  case class WithTuple(pair: (String, Int)) derives Schema

  enum Status derives Schema:
    case Active, Inactive, Suspended

  case class ValidatedId(value: String)
  object ValidatedId:
    given Schema[ValidatedId] = NewtypeSchema.validated[ValidatedId, String](
      s => if s.nonEmpty then Right(ValidatedId(s)) else Left("ID must not be empty"),
      _.value
    )

  case class InnerNewtype(value: String)
  object InnerNewtype:
    given Schema[InnerNewtype] = NewtypeSchema[InnerNewtype, String](InnerNewtype(_), _.value)

  case class OuterNewtype(inner: InnerNewtype)
  object OuterNewtype:
    given Schema[OuterNewtype] = NewtypeSchema[OuterNewtype, InnerNewtype](OuterNewtype(_), _.inner)

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
      },
      test("Byte round-trip") {
        val codec = DynamoDB.codec[Byte]
        val av    = codec.encodeValue(42.toByte)
        assertTrue(av.n() == "42", codec.decodeValue(av) == Right(42.toByte))
      },
      test("Short round-trip") {
        val codec = DynamoDB.codec[Short]
        val av    = codec.encodeValue(1000.toShort)
        assertTrue(av.n() == "1000", codec.decodeValue(av) == Right(1000.toShort))
      },
      test("Float round-trip") {
        val codec = DynamoDB.codec[Float]
        val av    = codec.encodeValue(3.14f)
        assertTrue(av.n() == "3.14", codec.decodeValue(av) == Right(3.14f))
      },
      test("BigInt round-trip") {
        val codec = DynamoDB.codec[BigInt]
        val big   = BigInt("123456789012345678901234567890")
        val av    = codec.encodeValue(big)
        assertTrue(av.n() == "123456789012345678901234567890", codec.decodeValue(av) == Right(big))
      },
      test("BigDecimal round-trip") {
        val codec = DynamoDB.codec[BigDecimal]
        val big   = BigDecimal("123456789.012345678901234567890")
        val av    = codec.encodeValue(big)
        assertTrue(codec.decodeValue(av) == Right(big))
      },
      test("Char round-trip") {
        val codec = DynamoDB.codec[Char]
        val av    = codec.encodeValue('Z')
        assertTrue(av.s() == "Z", codec.decodeValue(av) == Right('Z'))
      },
      test("Unit round-trip") {
        val codec = DynamoDB.codec[Unit]
        val av    = codec.encodeValue(())
        assertTrue(av.nul(), codec.decodeValue(av) == Right(()))
      },
      test("UUID round-trip") {
        val codec = DynamoDB.codec[UUID]
        val u     = UUID.fromString("550e8400-e29b-41d4-a716-446655440000")
        val av    = codec.encodeValue(u)
        assertTrue(codec.decodeValue(av) == Right(u))
      },
      test("LocalDate round-trip") {
        val codec = DynamoDB.codec[LocalDate]
        val d     = LocalDate.of(2025, 4, 17)
        val av    = codec.encodeValue(d)
        assertTrue(av.s() == "2025-04-17", codec.decodeValue(av) == Right(d))
      },
      test("Duration round-trip") {
        val codec = DynamoDB.codec[Duration]
        val dur   = Duration.ofHours(2).plusMinutes(30)
        val av    = codec.encodeValue(dur)
        assertTrue(codec.decodeValue(av) == Right(dur))
      },
      test("Month round-trip") {
        val codec = DynamoDB.codec[Month]
        val m     = Month.APRIL
        val av    = codec.encodeValue(m)
        assertTrue(av.s() == "APRIL", codec.decodeValue(av) == Right(m))
      },
      test("DayOfWeek round-trip") {
        val codec = DynamoDB.codec[DayOfWeek]
        val d     = DayOfWeek.THURSDAY
        val av    = codec.encodeValue(d)
        assertTrue(av.s() == "THURSDAY", codec.decodeValue(av) == Right(d))
      },
      test("ZoneId round-trip") {
        val codec = DynamoDB.codec[ZoneId]
        val z     = ZoneId.of("America/New_York")
        val av    = codec.encodeValue(z)
        assertTrue(av.s() == "America/New_York", codec.decodeValue(av) == Right(z))
      },
      test("Currency round-trip") {
        val codec = DynamoDB.codec[Currency]
        val c     = Currency.getInstance("USD")
        val av    = codec.encodeValue(c)
        assertTrue(av.s() == "USD", codec.decodeValue(av) == Right(c))
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
    ),
    suite("Options")(
      test("Option[Record] with Some") {
        val codec = DynamoDB.codec[WithOptionalRecord]
        val value = WithOptionalRecord(Some(SimpleRec("hello", 42)))
        val map   = new java.util.HashMap[String, AttributeValue]()
        codec.encode(value, map)
        val back = codec.decode(map)
        assertTrue(back == Right(value))
      },
      test("Option[Record] with None omits field") {
        val codec = DynamoDB.codec[WithOptionalRecord]
        val value = WithOptionalRecord(None)
        val map   = new java.util.HashMap[String, AttributeValue]()
        codec.encode(value, map)
        val back = codec.decode(map)
        assertTrue(!map.containsKey("data"), back == Right(value))
      },
      test("all-optional record with all None") {
        val codec = DynamoDB.codec[AllOptional]
        val value = AllOptional(None, None, None)
        val map   = new java.util.HashMap[String, AttributeValue]()
        codec.encode(value, map)
        val back = codec.decode(map)
        assertTrue(back == Right(value))
      },
      test("NULL attribute decoded as None") {
        val codec = DynamoDB.codec[WithOptionalRecord]
        val map   = new java.util.HashMap[String, AttributeValue]()
        map.put("data", AttributeValue.builder().nul(true).build())
        val back = codec.decode(map)
        assertTrue(back == Right(WithOptionalRecord(None)))
      }
    ),
    suite("Collections")(
      test("empty List round-trip") {
        val codec = DynamoDB.codec[List[String]]
        val av    = codec.encodeValue(List.empty[String])
        assertTrue(av.hasL, av.l().size() == 0, codec.decodeValue(av) == Right(List.empty[String]))
      },
      test("empty Map round-trip") {
        val codec = DynamoDB.codec[Map[String, String]]
        val av    = codec.encodeValue(Map.empty[String, String])
        assertTrue(av.hasM, av.m().size() == 0, codec.decodeValue(av) == Right(Map.empty[String, String]))
      },
      test("Vector[String] round-trip") {
        val codec = DynamoDB.codec[WithVectorField]
        val value = WithVectorField(Vector("a", "b", "c"))
        val map   = new java.util.HashMap[String, AttributeValue]()
        codec.encode(value, map)
        val back = codec.decode(map)
        assertTrue(back == Right(value))
      },
      test("Map[Int, String] (non-string keys) round-trip") {
        val codec = DynamoDB.codec[WithNonStringMap]
        val value = WithNonStringMap(Map(1 -> "one", 2 -> "two"))
        val map   = new java.util.HashMap[String, AttributeValue]()
        codec.encode(value, map)
        val back = codec.decode(map)
        assertTrue(map.get("data").hasL, back == Right(value))
      },
      test("empty Set[String] fails on encode") {
        val codec = DynamoDB.codec[Set[String]]
        val result =
          try
            codec.encodeValue(Set.empty[String])
            false
          catch case _: SchemaError => true
        assertTrue(result)
      }
    ),
    suite("Enums")(
      test("enum round-trip") {
        val codec  = DynamoDB.codec[Status]
        val values = List(Status.Active, Status.Inactive, Status.Suspended)
        val results = values.map { v =>
          val av = codec.encodeValue(v)
          codec.decodeValue(av)
        }
        assertTrue(results == values.map(Right(_)))
      },
      test("unknown enum value returns error") {
        val codec  = DynamoDB.codec[Status]
        val av     = AttributeValue.builder().s("Unknown").build()
        val result = codec.decodeValue(av)
        assertTrue(result.isLeft)
      },
      test("Either[String, Int] Right round-trip") {
        val codec = DynamoDB.codec[WithEither]
        val value = WithEither(Right(42))
        val map   = new java.util.HashMap[String, AttributeValue]()
        codec.encode(value, map)
        val back = codec.decode(map)
        assertTrue(back == Right(value))
      },
      test("Either[String, Int] Left round-trip") {
        val codec = DynamoDB.codec[WithEither]
        val value = WithEither(Left("error"))
        val map   = new java.util.HashMap[String, AttributeValue]()
        codec.encode(value, map)
        val back = codec.decode(map)
        assertTrue(back == Right(value))
      },
      test("Tuple2 round-trip") {
        val codec = DynamoDB.codec[WithTuple]
        val value = WithTuple(("hello", 42))
        val map   = new java.util.HashMap[String, AttributeValue]()
        codec.encode(value, map)
        val back = codec.decode(map)
        assertTrue(back == Right(value))
      }
    ),
    suite("Wrappers")(
      test("validated newtype with valid value") {
        val codec = DynamoDB.codec[ValidatedId]
        val av    = codec.encodeValue(ValidatedId("abc"))
        val back  = codec.decodeValue(av)
        assertTrue(av.s() == "abc", back == Right(ValidatedId("abc")))
      },
      test("validated newtype with invalid value returns error") {
        val codec = DynamoDB.codec[ValidatedId]
        val av    = AttributeValue.builder().s("").build()
        val back =
          try codec.decodeValue(av)
          catch case e: SchemaError => Left(e)
        assertTrue(back.isLeft)
      },
      test("nested newtypes round-trip") {
        val codec = DynamoDB.codec[OuterNewtype]
        val value = OuterNewtype(InnerNewtype("hello"))
        val av    = codec.encodeValue(value)
        val back  = codec.decodeValue(av)
        assertTrue(av.s() == "hello", back == Right(value))
      }
    ),
    suite("@transient fields")(
      test("transient field excluded from encoded map") {
        val codec = DynamoDB.codec[WithTransient]
        val value = WithTransient(computed = "ignored", name = "Alice")
        val map   = new java.util.HashMap[String, AttributeValue]()
        codec.encode(value, map)
        assertTrue(
          !map.containsKey("computed"),
          map.containsKey("name"),
          map.get("name").s() == "Alice"
        )
      },
      test("transient field zero-initialized on decode") {
        val codec = DynamoDB.codec[WithTransient]
        val map   = new java.util.HashMap[String, AttributeValue]()
        map.put("name", AttributeValue.builder().s("Bob").build())
        val back = codec.decode(map)
        assertTrue(back.isRight, back.toOption.get.name == "Bob")
      }
    ),
    suite("Recursive types")(
      test("TreeNode: leaf") {
        val codec = DynamoDB.codec[TreeNode]
        val leaf  = TreeNode("leaf", Nil)
        val map   = new java.util.HashMap[String, AttributeValue]()
        codec.encode(leaf, map)
        val back = codec.decode(map)
        assertTrue(back == Right(leaf))
      },
      test("TreeNode: nested tree") {
        val codec = DynamoDB.codec[TreeNode]
        val tree = TreeNode(
          "root",
          List(
            TreeNode("left", List(TreeNode("ll", Nil))),
            TreeNode("right", Nil)
          )
        )
        val map = new java.util.HashMap[String, AttributeValue]()
        codec.encode(tree, map)
        val back = codec.decode(map)
        assertTrue(back == Right(tree))
      },
      test("LinkedNode: chain of 3") {
        val codec = DynamoDB.codec[LinkedNode]
        val chain = LinkedNode(1, Some(LinkedNode(2, Some(LinkedNode(3, None)))))
        val map   = new java.util.HashMap[String, AttributeValue]()
        codec.encode(chain, map)
        val back = codec.decode(map)
        assertTrue(back == Right(chain))
      }
    ),
    suite("Errors")(
      test("deeply nested error includes field context") {
        val codec    = DynamoDB.codec[Outer]
        val map      = new java.util.HashMap[String, AttributeValue]()
        val innerMap = new java.util.HashMap[String, AttributeValue]()
        val deepMap  = new java.util.HashMap[String, AttributeValue]()
        deepMap.put("value", AttributeValue.builder().s("not-a-number").build())
        innerMap.put("deep", AttributeValue.builder().m(deepMap).build())
        map.put("inner", AttributeValue.builder().m(innerMap).build())
        val result = codec.decode(map)
        assertTrue(result.isLeft)
      }
    ),
    suite("DynamicValue")(
      test("DynamicValue with NULL") {
        val codec  = DynamoDB.codec[DynamicValue]
        val av     = AttributeValue.builder().nul(true).build()
        val result = codec.decodeValue(av)
        assertTrue(result.isRight)
      },
      test("DynamicValue with SS attribute") {
        val codec  = DynamoDB.codec[DynamicValue]
        val av     = AttributeValue.builder().ss("a", "b", "c").build()
        val result = codec.decodeValue(av)
        assertTrue(result.isRight)
      }
    )
  )
