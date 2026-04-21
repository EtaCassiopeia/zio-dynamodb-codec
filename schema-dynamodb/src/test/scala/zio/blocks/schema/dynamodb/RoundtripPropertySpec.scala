package zio.blocks.schema.dynamodb

import zio.blocks.schema.*
import zio.test.*

import java.time.*
import java.util.UUID

object RoundtripPropertySpec extends ZIOSpecDefault:

  case class SimpleRec(x: String, y: Int) derives Schema
  case class NestedRec(label: String, inner: SimpleRec) derives Schema
  case class AllOptional(a: Option[String], b: Option[Int], c: Option[Boolean]) derives Schema
  case class WithList(items: List[Int]) derives Schema
  case class WithVector(items: Vector[String]) derives Schema
  case class WithStringMap(data: Map[String, Int]) derives Schema
  case class WithNonStringMap(data: Map[Int, String]) derives Schema
  case class WithSet(tags: Set[String]) derives Schema
  case class WithNumericSet(ids: Set[Int]) derives Schema
  case class WithEither(result: Either[String, Int]) derives Schema

  enum Color derives Schema:
    case Red, Green, Blue

  private val codec = DynamoDB.codec[SimpleRec]

  private def roundtripValue[A](codec: DynamoDBCodec[A], value: A): Boolean =
    codec.decodeValue(codec.encodeValue(value)) == Right(value)

  private def roundtripRecord[A](codec: DynamoDBCodec[A], value: A): Boolean =
    val map = new java.util.HashMap[String, software.amazon.awssdk.services.dynamodb.model.AttributeValue]()
    codec.encode(value, map)
    codec.decode(map) == Right(value)

  private val genSimpleRec = for
    x <- Gen.string
    y <- Gen.int
  yield SimpleRec(x, y)

  private val genNestedRec = for
    label <- Gen.string
    inner <- genSimpleRec
  yield NestedRec(label, inner)

  private val genAllOptional = for
    a <- Gen.option(Gen.string)
    b <- Gen.option(Gen.int)
    c <- Gen.option(Gen.boolean)
  yield AllOptional(a, b, c)

  def spec = suite("RoundtripPropertySpec")(
    suite("Primitives")(
      test("String roundtrip") {
        check(Gen.string) { s =>
          val c = DynamoDB.codec[String]
          assertTrue(roundtripValue(c, s))
        }
      },
      test("Int roundtrip") {
        check(Gen.int) { n =>
          val c = DynamoDB.codec[Int]
          assertTrue(roundtripValue(c, n))
        }
      },
      test("Long roundtrip") {
        check(Gen.long) { n =>
          val c = DynamoDB.codec[Long]
          assertTrue(roundtripValue(c, n))
        }
      },
      test("Double roundtrip") {
        check(Gen.double.filter(d => !d.isNaN && !d.isInfinite)) { n =>
          val c = DynamoDB.codec[Double]
          assertTrue(roundtripValue(c, n))
        }
      },
      test("Float roundtrip") {
        check(Gen.float.filter(f => !f.isNaN && !f.isInfinite)) { n =>
          val c = DynamoDB.codec[Float]
          assertTrue(roundtripValue(c, n))
        }
      },
      test("Boolean roundtrip") {
        check(Gen.boolean) { b =>
          val c = DynamoDB.codec[Boolean]
          assertTrue(roundtripValue(c, b))
        }
      },
      test("BigDecimal roundtrip") {
        check(Gen.bigDecimal(BigDecimal(-1e15), BigDecimal(1e15))) { n =>
          val c = DynamoDB.codec[BigDecimal]
          assertTrue(roundtripValue(c, n))
        }
      },
      test("BigInt roundtrip") {
        check(Gen.bigInt(BigInt(-1000000000L), BigInt(1000000000L))) { n =>
          val c = DynamoDB.codec[BigInt]
          assertTrue(roundtripValue(c, n))
        }
      },
      test("UUID roundtrip") {
        check(Gen.uuid) { u =>
          val c = DynamoDB.codec[UUID]
          assertTrue(roundtripValue(c, u))
        }
      },
      test("Char roundtrip") {
        check(Gen.char) { ch =>
          val c = DynamoDB.codec[Char]
          assertTrue(roundtripValue(c, ch))
        }
      },
      test("LocalDate roundtrip") {
        check(Gen.localDate) { d =>
          val c = DynamoDB.codec[LocalDate]
          assertTrue(roundtripValue(c, d))
        }
      },
      test("LocalDateTime roundtrip") {
        check(Gen.localDateTime) { dt =>
          val c = DynamoDB.codec[LocalDateTime]
          assertTrue(roundtripValue(c, dt))
        }
      },
      test("Instant roundtrip") {
        check(Gen.instant) { i =>
          val c = DynamoDB.codec[Instant]
          assertTrue(roundtripValue(c, i))
        }
      }
    ),
    suite("Records")(
      test("SimpleRec roundtrip via encodeValue/decodeValue") {
        check(genSimpleRec) { rec =>
          assertTrue(roundtripValue(codec, rec))
        }
      },
      test("SimpleRec roundtrip via encode/decode") {
        check(genSimpleRec) { rec =>
          assertTrue(roundtripRecord(codec, rec))
        }
      },
      test("NestedRec roundtrip") {
        check(genNestedRec) { rec =>
          val c = DynamoDB.codec[NestedRec]
          assertTrue(roundtripValue(c, rec) && roundtripRecord(c, rec))
        }
      },
      test("AllOptional roundtrip") {
        check(genAllOptional) { rec =>
          val c = DynamoDB.codec[AllOptional]
          assertTrue(roundtripValue(c, rec) && roundtripRecord(c, rec))
        }
      }
    ),
    suite("Collections")(
      test("List[Int] roundtrip") {
        check(Gen.listOf(Gen.int)) { items =>
          val c = DynamoDB.codec[WithList]
          val v = WithList(items)
          assertTrue(roundtripValue(c, v))
        }
      },
      test("Vector[String] roundtrip") {
        check(Gen.vectorOf(Gen.string)) { items =>
          val c = DynamoDB.codec[WithVector]
          val v = WithVector(items)
          assertTrue(roundtripValue(c, v))
        }
      },
      test("Map[String, Int] roundtrip") {
        check(Gen.mapOf(Gen.string, Gen.int)) { data =>
          val c = DynamoDB.codec[WithStringMap]
          val v = WithStringMap(data)
          assertTrue(roundtripValue(c, v))
        }
      },
      test("Map[Int, String] non-string keys roundtrip") {
        check(Gen.mapOf(Gen.int, Gen.string)) { data =>
          val c = DynamoDB.codec[WithNonStringMap]
          val v = WithNonStringMap(data)
          assertTrue(roundtripValue(c, v))
        }
      },
      test("Set[String] roundtrip (non-empty)") {
        check(Gen.setOf1(Gen.string)) { items =>
          val c = DynamoDB.codec[WithSet]
          val v = WithSet(items)
          assertTrue(roundtripValue(c, v))
        }
      },
      test("Set[Int] roundtrip (non-empty)") {
        check(Gen.setOf1(Gen.int)) { items =>
          val c = DynamoDB.codec[WithNumericSet]
          val v = WithNumericSet(items)
          assertTrue(roundtripValue(c, v))
        }
      }
    ),
    suite("Variants")(
      test("Enum roundtrip") {
        check(Gen.elements(Color.Red, Color.Green, Color.Blue)) { color =>
          val c = DynamoDB.codec[Color]
          assertTrue(roundtripValue(c, color))
        }
      },
      test("Either roundtrip") {
        check(Gen.either(Gen.string, Gen.int)) { e =>
          val c = DynamoDB.codec[WithEither]
          val v = WithEither(e)
          assertTrue(roundtripValue(c, v))
        }
      }
    ),
    suite("Error accumulation")(
      test("multiple missing fields reported") {
        val c      = DynamoDB.codec[SimpleRec]
        val empty  = new java.util.HashMap[String, software.amazon.awssdk.services.dynamodb.model.AttributeValue]()
        val result = c.decode(empty)
        assertTrue(
          result.isLeft,
          result.left.toOption.exists(_.errors.size >= 2)
        )
      }
    ),
    suite("Codec combinators")(
      test("transform roundtrip") {
        val stringCodec = DynamoDB.codec[String]
        case class Name(value: String)
        val nameCodec = stringCodec.transform[Name](Name(_), _.value)
        check(Gen.string) { s =>
          val name = Name(s)
          assertTrue(nameCodec.decodeValue(nameCodec.encodeValue(name)) == Right(name))
        }
      },
      test("transformOrFail with validation") {
        val intCodec = DynamoDB.codec[Int]
        case class PosInt(value: Int)
        val posCodec = intCodec.transformOrFail[PosInt](
          n =>
            if n > 0 then Right(PosInt(n))
            else Left(SchemaError.validationFailed("Must be positive")),
          _.value
        )
        check(Gen.int(1, Int.MaxValue)) { n =>
          val v = PosInt(n)
          assertTrue(posCodec.decodeValue(posCodec.encodeValue(v)) == Right(v))
        }
      },
      test("transformOrFail rejects invalid") {
        val intCodec = DynamoDB.codec[Int]
        case class PosInt(value: Int)
        val posCodec = intCodec.transformOrFail[PosInt](
          n =>
            if n > 0 then Right(PosInt(n))
            else Left(SchemaError.validationFailed("Must be positive")),
          _.value
        )
        val encoded = intCodec.encodeValue(-1)
        assertTrue(posCodec.decodeValue(encoded).isLeft)
      }
    ),
    suite("Safe encode")(
      test("encodeSafe returns Right for valid data") {
        check(genSimpleRec) { rec =>
          assertTrue(codec.encodeSafe(rec).isRight)
        }
      },
      test("encodeValueSafe returns Left for empty set") {
        val c = DynamoDB.codec[WithSet]
        val v = WithSet(Set.empty)
        assertTrue(c.encodeValueSafe(v).isLeft)
      }
    )
  )
