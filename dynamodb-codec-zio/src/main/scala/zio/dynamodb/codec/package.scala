package zio.dynamodb.codec

import zio.blocks.schema.Schema
import zio.blocks.schema.dynamodb.{DynamoDB, DynamoDBCodec}

extension [A](schema: Schema[A]) def dynamoDBCodec: DynamoDBCodec[A] = DynamoDB.codec[A](using schema)
