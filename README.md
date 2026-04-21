# zio-dynamodb-codec

[![CI](https://github.com/EtaCassiopeia/zio-dynamodb-codec/actions/workflows/ci.yml/badge.svg)](https://github.com/EtaCassiopeia/zio-dynamodb-codec/actions/workflows/ci.yml)
[![Release](https://github.com/EtaCassiopeia/zio-dynamodb-codec/actions/workflows/release.yml/badge.svg)](https://github.com/EtaCassiopeia/zio-dynamodb-codec/actions/workflows/release.yml)
[![Maven Central](https://img.shields.io/maven-central/v/io.github.etacassiopeia/zio-blocks-schema-dynamodb_3)](https://central.sonatype.com/artifact/io.github.etacassiopeia/zio-blocks-schema-dynamodb_3)
[![License](https://img.shields.io/github/license/EtaCassiopeia/zio-dynamodb-codec)](https://www.apache.org/licenses/LICENSE-2.0)

High-performance DynamoDB codec for Scala 3 using [zio-blocks-schema](https://github.com/zio/zio-blocks).

Derives bidirectional codecs from schema definitions — encodes and decodes directly to/from AWS SDK `AttributeValue` with no intermediate representation.

## Installation

```scala
// Core codec (no ZIO dependency)
libraryDependencies += "io.github.etacassiopeia" %% "zio-blocks-schema-dynamodb" % "0.0.1"

// ZIO integration (typed errors, IO operations)
libraryDependencies += "io.github.etacassiopeia" %% "zio-dynamodb-codec" % "0.0.1"
```

Check the [Maven Central badge](#) above for the latest version.

## Usage

```scala
import zio.blocks.schema.*
import zio.blocks.schema.dynamodb.*

case class User(name: String, age: Int, active: Boolean) derives Schema

// Default field names
val codec = DynamoDB.codec[User]

// Snake-case field names (contractId → contract_id)
val codec = DynamoDB.snakeCase.codec[User]

// Encode
val map = new java.util.HashMap[String, AttributeValue]()
codec.encode(User("Alice", 30, true), map)

// Decode
val user: Either[SchemaError, User] = codec.decode(map)
```

## Newtypes

```scala
case class ContractId(value: String)
object ContractId:
  given Schema[ContractId] = NewtypeSchema[ContractId, String](ContractId(_), _.value)
```

## Schema Evolution

```scala
given DynamoDBCodec[EventItemV3] = DynamoDB.codec[EventItemV3]

val codec = VersionedCodec[EventItemV3]
  .withFallback(Schema[EventItemV2])(_.toV3)
  .withFallback(Schema[EventItemV1])(_.toV3)
```

## Modules

- `schema-dynamodb` — Core codec, no ZIO dependency
- `dynamodb-codec-zio` — ZIO integration (typed errors, `IO` operations)
- `benchmarks` — JMH benchmarks comparing with dynosaur and scanamo
