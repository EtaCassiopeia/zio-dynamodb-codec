package zio.blocks.schema.dynamodb

import cats.syntax.all.*
import dynosaur.Schema as DSchema
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import zio.blocks.schema.*
import zio.test.*

import scala.jdk.CollectionConverters.*

object WireCompatMirrorSpec extends ZIOSpecDefault:

  private def assertAVEqual(dynosaur: AttributeValue, ours: AttributeValue): TestResult =
    assertTrue(dynosaur == ours)

  private def assertMapsEqual(
    dMap: java.util.Map[String, AttributeValue],
    oMap: java.util.Map[String, AttributeValue]
  ): TestResult =
    assertTrue(dMap.keySet() == oMap.keySet()) &&
      dMap.keySet().asScala.foldLeft(assertTrue(true)) { (acc, key) =>
        acc && assertTrue(dMap.get(key) == oMap.get(key))
      }

  def mirrorPrimitive[A](dSchema: DSchema[A], zSchema: Schema[A], value: A): TestResult =
    val dav = dSchema.write(value).toOption.get.value
    val oav = DynamoDB.codec[A](using zSchema).encodeValue(value)
    assertAVEqual(dav, oav)

  def mirrorRoundTrip[A](dSchema: DSchema[A], zSchema: Schema[A], value: A): TestResult =
    val dav   = dSchema.write(value).toOption.get.value
    val oav   = DynamoDB.codec[A](using zSchema).encodeValue(value)
    val dBack = dSchema.read(dynosaur.DynamoValue(oav))
    val oBack = DynamoDB.codec[A](using zSchema).decodeValue(dav)
    assertAVEqual(dav, oav) && assertTrue(dBack == Right(value), oBack == Right(value))

  // Domain types
  case class ContractId(value: String):
    def show: String = value
  val dContractId: DSchema[ContractId] = DSchema[String].imap(ContractId(_))(_.show)
  given Schema[ContractId]             = NewtypeSchema[ContractId, String](ContractId(_), _.show)

  case class LedgerSeqNum(value: Int)
  val dSeqNum: DSchema[LedgerSeqNum] =
    DSchema[String].imap(s => LedgerSeqNum(Integer.parseInt(s)))(n => f"${n.value}%010d")
  given Schema[LedgerSeqNum] = NewtypeSchema[LedgerSeqNum, String](
    s => LedgerSeqNum(Integer.parseInt(s)),
    n => f"${n.value}%010d"
  )

  case class SeqNumInt(value: Int)
  val dSeqNumInt: DSchema[SeqNumInt] = DSchema[Int].imap(SeqNumInt(_))(_.value)
  given Schema[SeqNumInt]            = NewtypeSchema[SeqNumInt, Int](SeqNumInt(_), _.value)

  case class CompositeKey(seqNum: LedgerSeqNum, uniqueId: Option[String])
  val dCompositeKey: DSchema[CompositeKey] = DSchema[String].imap { s =>
    val p = s.split("#", 2); CompositeKey(LedgerSeqNum(Integer.parseInt(p(0))), p.lift(1))
  }(ck => ck.uniqueId.fold(f"${ck.seqNum.value}%010d")(id => f"${ck.seqNum.value}%010d#$id"))
  given Schema[CompositeKey] = NewtypeSchema[CompositeKey, String](
    s =>
      val p = s.split("#", 2); CompositeKey(LedgerSeqNum(Integer.parseInt(p(0))), p.lift(1))
    ,
    ck => ck.uniqueId.fold(f"${ck.seqNum.value}%010d")(id => f"${ck.seqNum.value}%010d#$id")
  )

  case class EvtOrdering(start: Int, end: Int, tr: Int, uid: String)
  val dOrdering: DSchema[EvtOrdering] = DSchema[String].imap { s =>
    val p = s.stripPrefix("(").stripSuffix(")").split(",", 4); EvtOrdering(p(0).toInt, p(1).toInt, p(2).toInt, p(3))
  }(eo => s"(${eo.start},${eo.end},${eo.tr},${eo.uid})")
  given Schema[EvtOrdering] = NewtypeSchema[EvtOrdering, String](
    s =>
      val p = s.stripPrefix("(").stripSuffix(")").split(",", 4); EvtOrdering(p(0).toInt, p(1).toInt, p(2).toInt, p(3))
    ,
    eo => s"(${eo.start},${eo.end},${eo.tr},${eo.uid})"
  )

  // Simple record
  case class SimpleRec(name: String, age: Int, active: Boolean)
  val dSimple: DSchema[SimpleRec] = DSchema.record[SimpleRec] { field =>
    (field("name", _.name), field("age", _.age), field("active", _.active)).mapN(SimpleRec.apply)
  }
  case class ZSimple(name: String, age: Int, active: Boolean) derives Schema

  // Record with optional
  case class WithOpt(required: String, optional: Option[Int])
  val dWithOpt: DSchema[WithOpt] = DSchema.record[WithOpt] { field =>
    (field("required", _.required), field.opt("optional", _.optional)).mapN(WithOpt.apply)
  }
  case class ZWithOpt(required: String, optional: Option[Int]) derives Schema

  def spec = suite("WireCompatMirrorSpec")(
    suite("Primitives")(
      test("String")(mirrorPrimitive(DSchema[String], Schema[String], "hello")),
      test("Int")(mirrorPrimitive(DSchema[Int], Schema[Int], 42)),
      test("Long")(mirrorPrimitive(DSchema[Long], Schema[Long], 123456789L)),
      test("Double")(mirrorPrimitive(DSchema[Double], Schema[Double], 3.14)),
      test("Boolean")(mirrorPrimitive(DSchema[Boolean], Schema[Boolean], true)),
      test("String round-trip")(mirrorRoundTrip(DSchema[String], Schema[String], "hello")),
      test("Int round-trip")(mirrorRoundTrip(DSchema[Int], Schema[Int], 42))
    ),
    suite("Newtypes")(
      test("ContractId as plain S") {
        mirrorPrimitive(dContractId, summon[Schema[ContractId]], ContractId("my-id"))
      },
      test("LedgerSeqNum zero-padded") {
        mirrorPrimitive(dSeqNum, summon[Schema[LedgerSeqNum]], LedgerSeqNum(42)) &&
        assertTrue(dSeqNum.write(LedgerSeqNum(42)).toOption.get.value.s() == "0000000042")
      },
      test("CompositeKey with uniqueId") {
        val ck = CompositeKey(LedgerSeqNum(5), Some("evt-abc"))
        mirrorPrimitive(dCompositeKey, summon[Schema[CompositeKey]], ck) &&
        assertTrue(dCompositeKey.write(ck).toOption.get.value.s() == "0000000005#evt-abc")
      },
      test("EventOrdering tuple-format") {
        val eo = EvtOrdering(0, 5, 0, "evt-123")
        mirrorPrimitive(dOrdering, summon[Schema[EvtOrdering]], eo) &&
        assertTrue(dOrdering.write(eo).toOption.get.value.s() == "(0,5,0,evt-123)")
      }
    ),
    suite("Records")(
      test("simple record: identical field names and values") {
        val d = dSimple.write(SimpleRec("Alice", 30, true)).toOption.get.value.m()
        val o =
          val m = new java.util.HashMap[String, AttributeValue]();
          DynamoDB.codec[ZSimple].encode(ZSimple("Alice", 30, true), m); m
        assertMapsEqual(d, o)
      }
    ),
    suite("Optional fields")(
      test("Some value: field present in both") {
        val d = dWithOpt.write(WithOpt("hello", Some(42))).toOption.get.value.m()
        val o =
          val m = new java.util.HashMap[String, AttributeValue]();
          DynamoDB.codec[ZWithOpt].encode(ZWithOpt("hello", Some(42)), m); m
        assertMapsEqual(d, o)
      },
      test("None: field absent in both") {
        val d = dWithOpt.write(WithOpt("hello", None)).toOption.get.value.m()
        val o =
          val m = new java.util.HashMap[String, AttributeValue]();
          DynamoDB.codec[ZWithOpt].encode(ZWithOpt("hello", None), m); m
        assertMapsEqual(d, o) && assertTrue(!d.containsKey("optional"), !o.containsKey("optional"))
      },
      test("missing field decoded as None by both") {
        val map = new java.util.HashMap[String, AttributeValue]()
        map.put("required", AttributeValue.builder().s("hello").build())
        val dResult = dWithOpt.read(dynosaur.DynamoValue.attributeMap(map))
        val oResult = DynamoDB.codec[ZWithOpt].decode(map)
        assertTrue(dResult.isRight, oResult.isRight, oResult.toOption.get.optional.isEmpty)
      }
    ),
    suite("Cross-reading")(
      test("simple record: dynosaur reads our output") {
        val o =
          val m = new java.util.HashMap[String, AttributeValue]();
          DynamoDB.codec[ZSimple].encode(ZSimple("Alice", 30, true), m); m
        val result = dSimple.read(dynosaur.DynamoValue.attributeMap(o))
        assertTrue(result == Right(SimpleRec("Alice", 30, true)))
      },
      test("simple record: our codec reads dynosaur output") {
        val d      = dSimple.write(SimpleRec("Alice", 30, true)).toOption.get.value.m()
        val result = DynamoDB.codec[ZSimple].decode(d)
        assertTrue(result == Right(ZSimple("Alice", 30, true)))
      }
    )
  )
