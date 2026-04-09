package zio.blocks.schema.dynamodb

import zio.blocks.schema.*
import zio.blocks.schema.derive.Deriver

object DynamoDB:

  def codec[A](using schema: Schema[A]): DynamoDBCodec[A] =
    schema.deriving[DynamoDBCodec](DynamoDBCodecDeriver).derive.asInstanceOf[DynamoDBCodec[A]]
