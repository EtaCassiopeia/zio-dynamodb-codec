package zio.blocks.schema.dynamodb

import zio.blocks.schema.*
import zio.blocks.schema.derive.Deriver

object DynamoDB:

  def codec[A](using schema: Schema[A]): DynamoDBCodec[A] =
    schema.deriving[DynamoDBCodec](DynamoDBCodecDeriver).derive.asInstanceOf[DynamoDBCodec[A]]

  def table[A](using schema: Schema[A]): TableCodec[A] =
    TableCodec.fromCodec(codec[A])

  val snakeCase: DynamoDBConfig = DynamoDBConfig(NameMapper.snakeCase)

  def withNameMapper(mapper: NameMapper): DynamoDBConfig = DynamoDBConfig(mapper)

  extension [A](schema: Schema[A])
    def dynamoDBCodec: DynamoDBCodec[A] =
      schema.deriving[DynamoDBCodec](DynamoDBCodecDeriver).derive.asInstanceOf[DynamoDBCodec[A]]

final class DynamoDBConfig(mapper: NameMapper):
  private val deriver = new DynamoDBCodecDeriver(mapper)

  def codec[A](using schema: Schema[A]): DynamoDBCodec[A] =
    schema.deriving[DynamoDBCodec](deriver).derive.asInstanceOf[DynamoDBCodec[A]]

  def table[A](using schema: Schema[A]): TableCodec[A] =
    TableCodec.fromCodec(codec[A])
