package zio.blocks.schema.dynamodb

import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import zio.blocks.schema.*
import zio.test.*

object TableCodecSpec extends ZIOSpecDefault:

  // --- Test models ---

  case class SimpleItem(
    @Modifier.config("dynamodb.key", "partition") id: String,
    @Modifier.config("dynamodb.key", "sort") @Modifier.rename("SK") sortKey: String,
    data: String,
    count: Int
  ) derives Schema

  case class PkOnlyItem(
    @Modifier.config("dynamodb.key", "partition") @Modifier.rename("PK") id: String,
    value: String
  ) derives Schema

  case class PrefixedItem(
    @Modifier.config("dynamodb.key", "partition") @Modifier.config("dynamodb.key-prefix", "event-") contractId: String,
    @Modifier.config("dynamodb.key", "sort") @Modifier.rename("SK") sortKey: String,
    payload: String
  ) derives Schema

  case class SnakeCaseItem(
    @Modifier.config("dynamodb.key", "partition") @Modifier.config("dynamodb.key-prefix", "state-") contractId: String,
    @Modifier.config("dynamodb.key", "sort") @Modifier.rename("SK") sortKey: String,
    uniqueId: String,
    instrumentId: String,
    cryptoDigest: Option[String]
  ) derives Schema

  case class NoKeyItem(name: String, age: Int) derives Schema

  // --- Specs ---

  def spec = suite("TableCodec")(
    tableCodecSuite,
    keyCodecSuite,
    queryValuesSuite,
    projectionSuite,
    snakeCaseSuite,
    errorSuite
  )

  val tableCodecSuite = suite("TableCodec creation")(
    test("creates TableCodec from annotated model") {
      val table = DynamoDB.table[SimpleItem]
      assertTrue(
        table.key.partitionKeyName == "id",
        table.key.sortKeyName == Some("SK")
      )
    },
    test("creates TableCodec with partition key only") {
      val table = DynamoDB.table[PkOnlyItem]
      assertTrue(
        table.key.partitionKeyName == "PK",
        table.key.sortKeyName == None
      )
    },
    test("full codec still works identically") {
      val table = DynamoDB.table[SimpleItem]
      val item  = SimpleItem("pk1", "sk1", "hello", 42)
      val map   = new java.util.HashMap[String, AttributeValue]()
      table.codec.encode(item, map)
      assertTrue(
        map.get("id").s() == "pk1",
        map.get("SK").s() == "sk1",
        map.get("data").s() == "hello",
        map.get("count").n() == "42"
      )
    }
  )

  val keyCodecSuite = suite("KeyCodec")(
    test("encodes only key fields (PK + SK)") {
      val table  = DynamoDB.table[SimpleItem]
      val item   = SimpleItem("pk1", "sk1", "hello", 42)
      val keyMap = table.key.encode(item)
      assertTrue(
        keyMap.size() == 2,
        keyMap.get("id").s() == "pk1",
        keyMap.get("SK").s() == "sk1",
        keyMap.containsKey("data") == false,
        keyMap.containsKey("count") == false
      )
    },
    test("encodes only partition key when no sort key") {
      val table  = DynamoDB.table[PkOnlyItem]
      val item   = PkOnlyItem("myId", "someValue")
      val keyMap = table.key.encode(item)
      assertTrue(
        keyMap.size() == 1,
        keyMap.get("PK").s() == "myId",
        keyMap.containsKey("value") == false
      )
    },
    test("applies key-prefix to key encoding") {
      val table  = DynamoDB.table[PrefixedItem]
      val item   = PrefixedItem("c-001", "sk-val", "payload")
      val keyMap = table.key.encode(item)
      assertTrue(
        keyMap.size() == 2,
        keyMap.get("contractId").s() == "event-c-001",
        keyMap.get("SK").s() == "sk-val"
      )
    }
  )

  val queryValuesSuite = suite("QueryValues")(
    test("forPartitionKey produces colon-prefixed map") {
      val table = DynamoDB.table[SimpleItem]
      val item  = SimpleItem("pk1", "sk1", "data", 0)
      val qMap  = table.query.forPartitionKey(item)
      assertTrue(
        qMap.size() == 1,
        qMap.get(":id").s() == "pk1",
        qMap.containsKey("id") == false,
        qMap.containsKey(":SK") == false
      )
    },
    test("forKey produces colon-prefixed map with both keys") {
      val table = DynamoDB.table[SimpleItem]
      val item  = SimpleItem("pk1", "sk1", "data", 0)
      val qMap  = table.query.forKey(item)
      assertTrue(
        qMap.size() == 2,
        qMap.get(":id").s() == "pk1",
        qMap.get(":SK").s() == "sk1"
      )
    },
    test("forPartitionKey with prefix") {
      val table = DynamoDB.table[PrefixedItem]
      val item  = PrefixedItem("c-001", "sk-val", "payload")
      val qMap  = table.query.forPartitionKey(item)
      assertTrue(
        qMap.size() == 1,
        qMap.get(":contractId").s() == "event-c-001"
      )
    },
    test("forKey with PK-only item") {
      val table = DynamoDB.table[PkOnlyItem]
      val item  = PkOnlyItem("myId", "val")
      val qMap  = table.query.forKey(item)
      assertTrue(
        qMap.size() == 1,
        qMap.get(":PK").s() == "myId"
      )
    },
    test("partitionKeyCondition string") {
      val table = DynamoDB.table[SimpleItem]
      assertTrue(table.query.partitionKeyCondition == "id = :id")
    },
    test("fullKeyCondition string with PK + SK") {
      val table = DynamoDB.table[SimpleItem]
      assertTrue(table.query.fullKeyCondition == "id = :id AND SK = :SK")
    },
    test("fullKeyCondition string with PK only") {
      val table = DynamoDB.table[PkOnlyItem]
      assertTrue(table.query.fullKeyCondition == "PK = :PK")
    },
    test("beginsWithCondition string") {
      val table = DynamoDB.table[SimpleItem]
      assertTrue(table.query.beginsWithCondition == "id = :id AND begins_with(SK, :SK)")
    }
  )

  val projectionSuite = suite("Projection auto-derived")(
    test("partitionKeyCondition from schema") {
      assertTrue(Projection.partitionKeyCondition[SimpleItem] == "id = :id")
    },
    test("fullKeyCondition from schema") {
      assertTrue(Projection.fullKeyCondition[SimpleItem] == "id = :id AND SK = :SK")
    },
    test("derivedBeginsWithCondition from schema") {
      assertTrue(Projection.derivedBeginsWithCondition[SimpleItem] == "id = :id AND begins_with(SK, :SK)")
    },
    test("fullKeyCondition with PK only") {
      assertTrue(Projection.fullKeyCondition[PkOnlyItem] == "PK = :PK")
    }
  )

  val snakeCaseSuite = suite("snakeCase integration")(
    test("table with snakeCase mapper") {
      val table = DynamoDB.snakeCase.table[SnakeCaseItem]
      val item  = SnakeCaseItem("c-002", "0#0#uid", "uid-123", "inst-1", Some("digest"))
      assertTrue(
        table.key.partitionKeyName == "contract_id",
        table.key.sortKeyName == Some("SK")
      )
      && {
        val keyMap = table.key.encode(item)
        assertTrue(
          keyMap.get("contract_id").s() == "state-c-002",
          keyMap.get("SK").s() == "0#0#uid"
        )
      }
      && {
        val qMap = table.query.forPartitionKey(item)
        assertTrue(qMap.get(":contract_id").s() == "state-c-002")
      }
      && {
        assertTrue(
          table.query.partitionKeyCondition == "contract_id = :contract_id",
          table.query.fullKeyCondition == "contract_id = :contract_id AND SK = :SK"
        )
      }
      && {
        val fullMap = new java.util.HashMap[String, AttributeValue]()
        table.codec.encode(item, fullMap)
        assertTrue(
          fullMap.containsKey("unique_id"),
          fullMap.containsKey("instrument_id"),
          fullMap.containsKey("crypto_digest"),
          fullMap.size() == 5
        )
      }
    }
  )

  val errorSuite = suite("error handling")(
    test("throws on model with no key annotations") {
      assertTrue(
        try
          DynamoDB.table[NoKeyItem]
          false
        catch case e: IllegalArgumentException => e.getMessage.contains("no partition key found")
      )
    }
  )
