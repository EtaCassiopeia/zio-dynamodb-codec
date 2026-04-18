package zio.dynamodb.codec

import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import zio.*
import zio.blocks.schema.Schema
import zio.blocks.schema.dynamodb.{DynamoDB, DynamoDBCodec}

object DynamoDBOps:

  def decode[A](item: java.util.Map[String, AttributeValue])(using codec: DynamoDBCodec[A]): IO[DynamoCodecError, A] =
    ZIO.fromEither(codec.decode(item)).mapError(e => DynamoCodecError.DecodeError(e.getMessage, Some(e)))

  def encode[A](value: A)(using codec: DynamoDBCodec[A]): IO[DynamoCodecError, java.util.Map[String, AttributeValue]] =
    ZIO
      .attempt:
        val map = new java.util.HashMap[String, AttributeValue]()
        codec.encode(value, map)
        map
      .mapError(e => DynamoCodecError.EncodeError(e.getMessage, Some(e)))

  def encodeValue[A](value: A)(using codec: DynamoDBCodec[A]): IO[DynamoCodecError, AttributeValue] =
    ZIO
      .attempt(codec.encodeValue(value))
      .mapError(e => DynamoCodecError.EncodeError(e.getMessage, Some(e)))

  def decodeValue[A](av: AttributeValue)(using codec: DynamoDBCodec[A]): IO[DynamoCodecError, A] =
    ZIO.fromEither(codec.decodeValue(av)).mapError(e => DynamoCodecError.DecodeError(e.getMessage, Some(e)))

  def decodeAll[A](items: List[java.util.Map[String, AttributeValue]])(using
    codec: DynamoDBCodec[A]
  ): IO[DynamoCodecError, List[A]] =
    ZIO.foreach(items)(item =>
      ZIO.fromEither(codec.decode(item)).mapError(e => DynamoCodecError.DecodeError(e.getMessage, Some(e)))
    )
