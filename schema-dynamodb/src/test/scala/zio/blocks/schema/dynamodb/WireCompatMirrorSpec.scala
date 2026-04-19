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

  // Full record
  val dEventContractId: DSchema[ContractId] =
    DSchema[String].imap(s => ContractId(s.stripPrefix("event-")))("event-" + _.show)

  case class LedgerItem(
    contractId: ContractId,
    sortKey: CompositeKey,
    uniqueId: String,
    status: String,
    orderings: List[EvtOrdering],
    pivotSeqNum: Option[SeqNumInt],
    date: String,
    version: Option[Int],
    tamperKey: Option[String]
  )

  val dLedgerItem: DSchema[LedgerItem] = DSchema.record[LedgerItem] { field =>
    (
      field("contract_id", _.contractId)(dEventContractId),
      field("SK", _.sortKey)(dCompositeKey),
      field("unique_id", _.uniqueId),
      field("status", _.status),
      field("orderings", _.orderings)(dOrdering.asList),
      field.opt("pivot_seq_num", _.pivotSeqNum)(dSeqNumInt),
      field("date", _.date),
      field.opt("version", _.version),
      field.opt("tamper_key", _.tamperKey)
    ).mapN(LedgerItem.apply)
  }

  case class ZLedgerItem(
    @Modifier.rename("contract_id") @Modifier.config("dynamodb.key-prefix", "event-") contractId: ContractId,
    @Modifier.rename("SK") sortKey: CompositeKey,
    @Modifier.rename("unique_id") uniqueId: String,
    status: String,
    orderings: List[EvtOrdering],
    @Modifier.rename("pivot_seq_num") pivotSeqNum: Option[SeqNumInt],
    date: String,
    version: Option[Int],
    @Modifier.rename("tamper_key") tamperKey: Option[String]
  ) derives Schema

  val orderings = List(EvtOrdering(0, 5, 0, "evt-1"), EvtOrdering(6, 10, 1, "evt-2"))
  val fullItem = LedgerItem(
    ContractId("c-001"),
    CompositeKey(LedgerSeqNum(5), Some("evt-abc")),
    "evt-abc",
    "ACTIVE",
    orderings,
    Some(SeqNumInt(10)),
    "2025-04-17",
    Some(1),
    Some("key-123")
  )
  val nonesItem = fullItem.copy(pivotSeqNum = None, version = None, tamperKey = None)

  def toZ(item: LedgerItem): ZLedgerItem = ZLedgerItem(
    item.contractId, item.sortKey, item.uniqueId, item.status,
    item.orderings, item.pivotSeqNum, item.date, item.version, item.tamperKey
  )

  def dEncode(item: LedgerItem): java.util.Map[String, AttributeValue] =
    dLedgerItem.write(item).toOption.get.value.m()
  def oEncode(item: ZLedgerItem): java.util.Map[String, AttributeValue] =
    val m = new java.util.HashMap[String, AttributeValue](); DynamoDB.codec[ZLedgerItem].encode(item, m); m

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
    suite("Full ledger item")(
      test("all populated: identical maps")(assertMapsEqual(dEncode(fullItem), oEncode(toZ(fullItem)))),
      test("all Nones: identical maps")(assertMapsEqual(dEncode(nonesItem), oEncode(toZ(nonesItem)))),
      test("field names identical") {
        assertTrue(dEncode(fullItem).keySet() == oEncode(toZ(fullItem)).keySet())
      },
      test("contract_id with prefix") {
        assertTrue(dEncode(fullItem).get("contract_id") == oEncode(toZ(fullItem)).get("contract_id"))
      },
      test("orderings list")(assertTrue(dEncode(fullItem).get("orderings") == oEncode(toZ(fullItem)).get("orderings")))
    ),
    suite("Cross-reading")(
      test("our codec reads dynosaur data (all populated)") {
        val result = DynamoDB.codec[ZLedgerItem].decode(dEncode(fullItem))
        assertTrue(
          result.isRight,
          result.toOption.get.contractId == fullItem.contractId,
          result.toOption.get.orderings == fullItem.orderings
        )
      },
      test("dynosaur reads our data (all populated)") {
        val result = dLedgerItem.read(dynosaur.DynamoValue.attributeMap(oEncode(toZ(fullItem))))
        assertTrue(
          result.isRight,
          result.toOption.get.contractId == fullItem.contractId,
          result.toOption.get.orderings == fullItem.orderings
        )
      },
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
