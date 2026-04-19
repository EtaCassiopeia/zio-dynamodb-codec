package zio.blocks.schema.dynamodb.bench

import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole
import software.amazon.awssdk.services.dynamodb.model.AttributeValue as AwsAV

import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters.*
import scala.util.Random

case class BenchEventOrdering(start: Int, end: Int, timesReplayed: Int, uniqueId: String)

object DynosaurSetup:
  import dynosaur.Schema as DSchema
  import cats.syntax.all.*

  case class Item(
    contractId: String,
    sortKey: String,
    uniqueId: String,
    dslVersion: String,
    transactionId: String,
    lamportTimestamp: Long,
    instrumentClassId: String,
    transactabilityStatus: String,
    maintenanceLifecycleAccountState: String,
    cryptoDigest: Array[Byte],
    processedPrimaryEvent: Array[Byte],
    interpretedPrimaryEvent: Option[Array[Byte]],
    primaryEventHash: Array[Byte],
    correctedOrdering: List[BenchEventOrdering],
    historicEvidenceOrdering: List[BenchEventOrdering],
    pivotSeqNum: Option[Int],
    instrumentId: String,
    state: Array[Byte],
    stateCurrentDate: String,
    stateLastEndOfDayDate: String,
    cryptoDigestVersion: Option[Int],
    tamperKeyVersion: Option[String],
    tags: Set[String]
  )

  private case class KG(a: String, b: String)
  private case class EG(a: Array[Byte], b: Option[Array[Byte]], c: Array[Byte])

  val eoSchema: DSchema[BenchEventOrdering] =
    DSchema[String].imap { s =>
      val p = s.stripPrefix("(").stripSuffix(")").split(",", 4)
      BenchEventOrdering(p(0).toInt, p(1).toInt, p(2).toInt, p(3))
    }(e => s"(${e.start},${e.end},${e.timesReplayed},${e.uniqueId})")

  val schema: DSchema[Item] = DSchema.record[Item] { field =>
    (
      (
        field("contract_id", _.contractId)(DSchema[String].imap(_.stripPrefix("event-"))("event-" + _)),
        field("SK", _.sortKey)
      ).mapN(KG.apply),
      field("unique_id", _.uniqueId),
      field("dsl_version", _.dslVersion),
      field("transaction_id", _.transactionId),
      field("lamport_timestamp", _.lamportTimestamp),
      field("instrument_class_id", _.instrumentClassId),
      field("transactability_status", _.transactabilityStatus),
      field("maintenance_lifecycle_account_state", _.maintenanceLifecycleAccountState),
      field("crypto_digest", _.cryptoDigest),
      (
        field("processed_primary_event", _.processedPrimaryEvent),
        field.opt("interpreted_primary_event", _.interpretedPrimaryEvent),
        field("primary_event_hash", _.primaryEventHash)
      ).mapN(EG.apply),
      field("corrected_ordering", _.correctedOrdering)(eoSchema.asList),
      field("historic_evidence_ordering", _.historicEvidenceOrdering)(eoSchema.asList),
      field.opt("pivot_seq_num", _.pivotSeqNum),
      field("instrument_id", _.instrumentId),
      field("state", _.state),
      field("state_current_date", _.stateCurrentDate),
      field("state_last_end_of_day_date", _.stateLastEndOfDayDate),
      field.opt("crypto_digest_version", _.cryptoDigestVersion),
      field.opt("tamper_key_version", _.tamperKeyVersion),
      field("tags", _.tags)(DSchema[String].asList.imap(_.toSet)(_.toList))
    ).mapN { (k, uid, dsl, txn, lts, icid, ts, mla, cd, eg, co, heo, ps, iid, st, scd, sld, cdv, tkv, tags) =>
      Item(k.a, k.b, uid, dsl, txn, lts, icid, ts, mla, cd, eg.a, eg.b, eg.c, co, heo, ps, iid, st, scd, sld, cdv, tkv, tags)
    }
  }

object ZioBlocksSetup:
  import zio.blocks.schema.*
  import zio.blocks.schema.dynamodb.*

  given Schema[BenchEventOrdering] = NewtypeSchema[BenchEventOrdering, String](
    s =>
      val p = s.stripPrefix("(").stripSuffix(")").split(",", 4);
      BenchEventOrdering(p(0).toInt, p(1).toInt, p(2).toInt, p(3))
    ,
    e => s"(${e.start},${e.end},${e.timesReplayed},${e.uniqueId})"
  )

  case class Item(
    @Modifier.rename("contract_id") contractId: String,
    @Modifier.rename("SK") sortKey: String,
    @Modifier.rename("unique_id") uniqueId: String,
    @Modifier.rename("dsl_version") dslVersion: String,
    @Modifier.rename("transaction_id") transactionId: String,
    @Modifier.rename("lamport_timestamp") lamportTimestamp: Long,
    @Modifier.rename("instrument_class_id") instrumentClassId: String,
    @Modifier.rename("transactability_status") transactabilityStatus: String,
    @Modifier.rename("maintenance_lifecycle_account_state") maintenanceLifecycleAccountState: String,
    @Modifier.rename("crypto_digest") cryptoDigest: Array[Byte],
    @Modifier.rename("processed_primary_event") processedPrimaryEvent: Array[Byte],
    @Modifier.rename("interpreted_primary_event") interpretedPrimaryEvent: Option[Array[Byte]],
    @Modifier.rename("primary_event_hash") primaryEventHash: Array[Byte],
    @Modifier.rename("corrected_ordering") correctedOrdering: List[BenchEventOrdering],
    @Modifier.rename("historic_evidence_ordering") historicEvidenceOrdering: List[BenchEventOrdering],
    @Modifier.rename("pivot_seq_num") pivotSeqNum: Option[Int],
    @Modifier.rename("instrument_id") instrumentId: String,
    state: Array[Byte],
    @Modifier.rename("state_current_date") stateCurrentDate: String,
    @Modifier.rename("state_last_end_of_day_date") stateLastEndOfDayDate: String,
    @Modifier.rename("crypto_digest_version") cryptoDigestVersion: Option[Int],
    @Modifier.rename("tamper_key_version") tamperKeyVersion: Option[String],
    tags: Set[String]
  ) derives Schema

object ScanamoSetup:
  import org.scanamo.*
  import org.scanamo.generic.semiauto.*

  given DynamoFormat[BenchEventOrdering] =
    DynamoFormat.coercedXmap[BenchEventOrdering, String, Exception](
      s =>
        val p = s.stripPrefix("(").stripSuffix(")").split(",", 4);
        BenchEventOrdering(p(0).toInt, p(1).toInt, p(2).toInt, p(3))
      ,
      e => s"(${e.start},${e.end},${e.timesReplayed},${e.uniqueId})"
    )

  case class Item(
    contract_id: String, SK: String, unique_id: String, dsl_version: String,
    transaction_id: String, lamport_timestamp: Long, instrument_class_id: String,
    transactability_status: String, maintenance_lifecycle_account_state: String,
    crypto_digest: Array[Byte], processed_primary_event: Array[Byte],
    interpreted_primary_event: Option[Array[Byte]], primary_event_hash: Array[Byte],
    corrected_ordering: List[BenchEventOrdering], historic_evidence_ordering: List[BenchEventOrdering],
    pivot_seq_num: Option[Int], instrument_id: String, state: Array[Byte],
    state_current_date: String, state_last_end_of_day_date: String,
    crypto_digest_version: Option[Int], tamper_key_version: Option[String],
    tags: Set[String]
  )

  given DynamoFormat[Item] = deriveDynamoFormat[Item]

@State(Scope.Thread)
class BenchState:
  private val rng        = new Random(42)
  private def rs(n: Int) = rng.alphanumeric.take(n).mkString
  private def rb(n: Int) = val b = new Array[Byte](n); rng.nextBytes(b); b
  private def mkOrdering(n: Int) =
    (0 until n).map(i => BenchEventOrdering(i * 5, (i + 1) * 5, i % 3, s"evt-${rs(8)}")).toList

  val orderings = mkOrdering(5)
  val tagSet    = Set("tag1", "tag2", "tag3", "vip", "active")

  val dynosaurItem = DynosaurSetup.Item(
    "contract-" + rs(12), String.format("%010d", 42: Integer) + "#" + rs(12),
    rs(12), "3.0", rs(16), System.currentTimeMillis(), rs(10), "ACTIVE", "OPEN",
    rb(64), rb(256), Some(rb(128)), rb(32), orderings, orderings,
    Some(10), rs(12), rb(512), "2025-04-17", "2025-04-16", Some(1),
    Some(java.util.UUID.randomUUID().toString), tagSet
  )
  val dynosaurDV = DynosaurSetup.schema.write(dynosaurItem).toOption.get

  val zioBlocksItem = ZioBlocksSetup.Item(
    dynosaurItem.contractId, dynosaurItem.sortKey, dynosaurItem.uniqueId, dynosaurItem.dslVersion,
    dynosaurItem.transactionId, dynosaurItem.lamportTimestamp, dynosaurItem.instrumentClassId,
    dynosaurItem.transactabilityStatus, dynosaurItem.maintenanceLifecycleAccountState,
    dynosaurItem.cryptoDigest, dynosaurItem.processedPrimaryEvent, dynosaurItem.interpretedPrimaryEvent,
    dynosaurItem.primaryEventHash, orderings, orderings, dynosaurItem.pivotSeqNum,
    dynosaurItem.instrumentId, dynosaurItem.state, dynosaurItem.stateCurrentDate,
    dynosaurItem.stateLastEndOfDayDate, dynosaurItem.cryptoDigestVersion, dynosaurItem.tamperKeyVersion, tagSet
  )
  val zioBlocksCodec = zio.blocks.schema.dynamodb.DynamoDB.codec[ZioBlocksSetup.Item]
  val zioBlocksMap: java.util.Map[String, AwsAV] =
    val m = new java.util.HashMap[String, AwsAV](); zioBlocksCodec.encode(zioBlocksItem, m); m

  val scanamoItem = ScanamoSetup.Item(
    dynosaurItem.contractId, dynosaurItem.sortKey, dynosaurItem.uniqueId, dynosaurItem.dslVersion,
    dynosaurItem.transactionId, dynosaurItem.lamportTimestamp, dynosaurItem.instrumentClassId,
    dynosaurItem.transactabilityStatus, dynosaurItem.maintenanceLifecycleAccountState,
    dynosaurItem.cryptoDigest, dynosaurItem.processedPrimaryEvent, dynosaurItem.interpretedPrimaryEvent,
    dynosaurItem.primaryEventHash, orderings, orderings, dynosaurItem.pivotSeqNum,
    dynosaurItem.instrumentId, dynosaurItem.state, dynosaurItem.stateCurrentDate,
    dynosaurItem.stateLastEndOfDayDate, dynosaurItem.cryptoDigestVersion, dynosaurItem.tamperKeyVersion, tagSet
  )
  val scanamoFmt = summon[org.scanamo.DynamoFormat[ScanamoSetup.Item]]
  val scanamoDV  = scanamoFmt.write(scanamoItem)

@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
@State(Scope.Benchmark)
class EncodeBench:
  val s = new BenchState()

  @Benchmark def dynosaur(bh: Blackhole): Unit =
    bh.consume(DynosaurSetup.schema.write(s.dynosaurItem))

  @Benchmark def zioBlocks(bh: Blackhole): Unit =
    val m = new java.util.HashMap[String, AwsAV]()
    s.zioBlocksCodec.encode(s.zioBlocksItem, m)
    bh.consume(m)

  @Benchmark def scanamo(bh: Blackhole): Unit =
    bh.consume(s.scanamoFmt.write(s.scanamoItem))

@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
@State(Scope.Benchmark)
class DecodeBench:
  val s = new BenchState()

  @Benchmark def dynosaur(bh: Blackhole): Unit =
    bh.consume(DynosaurSetup.schema.read(s.dynosaurDV))

  @Benchmark def zioBlocks(bh: Blackhole): Unit =
    bh.consume(s.zioBlocksCodec.decode(s.zioBlocksMap))

  @Benchmark def scanamo(bh: Blackhole): Unit =
    bh.consume(s.scanamoFmt.read(s.scanamoDV))
