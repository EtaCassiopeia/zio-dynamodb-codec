package zio.blocks.schema.dynamodb

import cats.syntax.all.*
import dynosaur.Schema as DSchema
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import zio.blocks.schema.*
import zio.test.*

import scala.jdk.CollectionConverters.*

/** Validates wire format compatibility between zio-blocks-schema-dynamodb and dynosaur.
  *
  * These tests document the exact wire format produced by our codec for every type category, and verify that dynosaur
  * produces identical output. Any difference found here would indicate a migration risk.
  */
object WireFormatComparisonSpec extends ZIOSpecDefault:

  // --- Test models ---

  case class SimpleRecord(name: String, age: Int, active: Boolean) derives Schema
  val dSimpleRecord: DSchema[SimpleRecord] = DSchema.record[SimpleRecord] { field =>
    (field("name", _.name), field("age", _.age), field("active", _.active)).mapN(SimpleRecord.apply)
  }

  case class WithOption(required: String, optional: Option[Int]) derives Schema
  val dWithOption: DSchema[WithOption] = DSchema.record[WithOption] { field =>
    (field("required", _.required), field.opt("optional", _.optional)).mapN(WithOption.apply)
  }

  case class WithList(items: List[Int]) derives Schema
  val dWithList: DSchema[WithList] = DSchema.record[WithList] { field =>
    field("items", _.items).map(WithList.apply)
  }

  case class WithStringMap(data: Map[String, Int]) derives Schema
  val dWithStringMap: DSchema[WithStringMap] = DSchema.record[WithStringMap] { field =>
    field("data", _.data).map(WithStringMap.apply)
  }

  case class WithSet(tags: Set[String]) derives Schema
  val dWithSet: DSchema[WithSet] = DSchema.record[WithSet] { field =>
    field("tags", _.tags)(DSchema[String].asList.imap(_.toSet)(_.toList)).map(WithSet.apply)
  }

  case class WithNumericSet(ids: Set[Int]) derives Schema
  val dWithNumericSet: DSchema[WithNumericSet] = DSchema.record[WithNumericSet] { field =>
    field("ids", _.ids)(DSchema[Int].asList.imap(_.toSet)(_.toList)).map(WithNumericSet.apply)
  }

  case class Nested(label: String, inner: SimpleRecord) derives Schema
  val dNested: DSchema[Nested] = DSchema.record[Nested] { field =>
    (field("label", _.label), field("inner", _.inner)(dSimpleRecord)).mapN(Nested.apply)
  }

  case class WithOptionalNested(data: Option[SimpleRecord]) derives Schema
  val dWithOptionalNested: DSchema[WithOptionalNested] = DSchema.record[WithOptionalNested] { field =>
    field.opt("data", _.data)(dSimpleRecord).map(WithOptionalNested.apply)
  }

  // --- Helpers ---

  private def ourEncode[A](value: A)(using schema: Schema[A]): java.util.Map[String, AttributeValue] =
    val codec = DynamoDB.codec[A]
    val map   = new java.util.HashMap[String, AttributeValue]()
    codec.encode(value, map)
    map

  private def ourDecode[A](map: java.util.Map[String, AttributeValue])(using schema: Schema[A]): Either[Any, A] =
    DynamoDB.codec[A].decode(map)

  private def dynosaurEncode[A](value: A, schema: DSchema[A]): java.util.Map[String, AttributeValue] =
    val dv = schema.write(value).toOption.get
    dv.value.m()

  private def dynosaurDecode[A](map: java.util.Map[String, AttributeValue], schema: DSchema[A]): Either[Any, A] =
    val av = AttributeValue.builder().m(map).build()
    schema.read(dynosaur.DynamoValue(av))

  private def assertMapsEqual(
    label: String,
    ours: java.util.Map[String, AttributeValue],
    dynosaurs: java.util.Map[String, AttributeValue]
  ): TestResult =
    val ourKeys = ours.keySet().asScala
    val dynKeys = dynosaurs.keySet().asScala
    assertTrue(ourKeys == dynKeys) &&
    ourKeys.foldLeft(assertTrue(true)) { (acc, key) =>
      acc && assertTrue(ours.get(key) == dynosaurs.get(key))
    }

  private def crossReadTest[A](
    value: A,
    dSchema: DSchema[A]
  )(using schema: Schema[A]): TestResult =
    val ourMap            = ourEncode(value)
    val dynosaurMap       = dynosaurEncode(value, dSchema)
    val ourReadsDynosaur  = ourDecode[A](dynosaurMap)
    val dynosaurReadsOurs = dynosaurDecode(ourMap, dSchema)
    assertTrue(
      ourReadsDynosaur == Right(value),
      dynosaurReadsOurs == Right(value)
    )

  def spec = suite("WireFormatComparisonSpec")(
    suite("Primitives - identical wire format")(
      test("String") {
        val ours     = DynamoDB.codec[String].encodeValue("hello")
        val dynosaur = DSchema[String].write("hello").toOption.get.value
        assertTrue(ours == dynosaur)
      },
      test("Int") {
        val ours     = DynamoDB.codec[Int].encodeValue(42)
        val dynosaur = DSchema[Int].write(42).toOption.get.value
        assertTrue(ours == dynosaur)
      },
      test("Boolean") {
        val ours     = DynamoDB.codec[Boolean].encodeValue(true)
        val dynosaur = DSchema[Boolean].write(true).toOption.get.value
        assertTrue(ours == dynosaur)
      },
      test("Long") {
        val ours     = DynamoDB.codec[Long].encodeValue(123456789L)
        val dynosaur = DSchema[Long].write(123456789L).toOption.get.value
        assertTrue(ours == dynosaur)
      }
    ),
    suite("Records - identical wire format")(
      test("simple record: same fields and values") {
        val value = SimpleRecord("Alice", 30, true)
        val ours  = ourEncode(value)
        val dyn   = dynosaurEncode(value, dSimpleRecord)
        assertMapsEqual("SimpleRecord", ours, dyn)
      },
      test("nested record") {
        val value = Nested("outer", SimpleRecord("inner", 1, false))
        val ours  = ourEncode(value)
        val dyn   = dynosaurEncode(value, dNested)
        assertMapsEqual("Nested", ours, dyn)
      },
      test("cross-read: simple record") {
        crossReadTest(SimpleRecord("Bob", 25, false), dSimpleRecord)
      },
      test("cross-read: nested record") {
        crossReadTest(Nested("x", SimpleRecord("y", 2, true)), dNested)
      }
    ),
    suite("Options - identical wire format")(
      test("Some value: field present") {
        val value = WithOption("req", Some(42))
        val ours  = ourEncode(value)
        val dyn   = dynosaurEncode(value, dWithOption)
        assertMapsEqual("WithOption(Some)", ours, dyn)
      },
      test("None: field absent") {
        val value = WithOption("req", None)
        val ours  = ourEncode(value)
        val dyn   = dynosaurEncode(value, dWithOption)
        assertMapsEqual("WithOption(None)", ours, dyn)
      },
      test("cross-read: Some") {
        crossReadTest(WithOption("req", Some(42)), dWithOption)
      },
      test("cross-read: None") {
        crossReadTest(WithOption("req", None), dWithOption)
      },
      test("cross-read: optional nested Some") {
        crossReadTest(WithOptionalNested(Some(SimpleRecord("x", 1, true))), dWithOptionalNested)
      },
      test("cross-read: optional nested None") {
        crossReadTest(WithOptionalNested(None), dWithOptionalNested)
      }
    ),
    suite("Lists - identical wire format")(
      test("List[Int]") {
        val value = WithList(List(1, 2, 3))
        val ours  = ourEncode(value)
        val dyn   = dynosaurEncode(value, dWithList)
        assertMapsEqual("WithList", ours, dyn)
      },
      test("empty list") {
        val value = WithList(List.empty)
        val ours  = ourEncode(value)
        val dyn   = dynosaurEncode(value, dWithList)
        assertMapsEqual("WithList(empty)", ours, dyn)
      },
      test("cross-read: list") {
        crossReadTest(WithList(List(10, 20, 30)), dWithList)
      }
    ),
    suite("String-key Maps - identical wire format")(
      test("Map[String, Int]") {
        val value = WithStringMap(Map("a" -> 1, "b" -> 2))
        val ours  = ourEncode(value)
        val dyn   = dynosaurEncode(value, dWithStringMap)
        assertMapsEqual("WithStringMap", ours, dyn)
      },
      test("cross-read: string map") {
        crossReadTest(WithStringMap(Map("x" -> 99)), dWithStringMap)
      }
    ),
    suite("Newtypes - identical wire format")(
      test("newtype roundtrip with custom encoding") {
        case class UserId(value: String)
        given Schema[UserId]         = NewtypeSchema[UserId, String](UserId(_), _.value)
        val dUserId: DSchema[UserId] = DSchema[String].imap(UserId(_))(_.value)
        val value                    = UserId("user-123")
        val ours                     = DynamoDB.codec[UserId].encodeValue(value)
        val dyn                      = dUserId.write(value).toOption.get.value
        assertTrue(ours == dyn)
      },
      test("zero-padded number newtype") {
        case class SeqNum(value: Int)
        given Schema[SeqNum] = NewtypeSchema[SeqNum, String](
          s => SeqNum(Integer.parseInt(s)),
          n => f"${n.value}%010d"
        )
        val dSeqNum: DSchema[SeqNum] =
          DSchema[String].imap(s => SeqNum(Integer.parseInt(s)))(n => f"${n.value}%010d")
        val value = SeqNum(42)
        val ours  = DynamoDB.codec[SeqNum].encodeValue(value)
        val dyn   = dSeqNum.write(value).toOption.get.value
        assertTrue(ours == dyn, ours.s() == "0000000042")
      }
    )
  )
