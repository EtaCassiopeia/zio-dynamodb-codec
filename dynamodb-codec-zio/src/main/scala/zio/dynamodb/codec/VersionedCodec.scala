package zio.dynamodb.codec

import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import zio.*
import zio.blocks.schema.{Schema, SchemaError}
import zio.blocks.schema.dynamodb.{DynamoDB, DynamoDBCodec}

final class VersionedCodec[A] private (
  currentCodec: DynamoDBCodec[A],
  fallbacks: List[VersionedCodec.Fallback[?, A]]
):

  def decode(input: java.util.Map[String, AttributeValue]): Either[SchemaError, A] =
    currentCodec.decode(input) match
      case right @ Right(_) => right
      case Left(primaryError) =>
        fallbacks.iterator
          .map(_.tryDecode(input))
          .collectFirst { case right @ Right(_) => right }
          .getOrElse(Left(primaryError))

  def encode(value: A, output: java.util.Map[String, AttributeValue]): Unit =
    currentCodec.encode(value, output)

  def encodeValue(value: A): AttributeValue =
    currentCodec.encodeValue(value)

  def withFallback[Old](oldSchema: Schema[Old])(migrate: Old => A): VersionedCodec[A] =
    val oldCodec = DynamoDB.codec[Old](using oldSchema)
    val fb       = new VersionedCodec.Fallback[Old, A](oldCodec, migrate)
    new VersionedCodec[A](currentCodec, fallbacks :+ fb)

  def decodeZIO(input: java.util.Map[String, AttributeValue]): IO[DynamoCodecError, A] =
    ZIO.fromEither(decode(input)).mapError(e => DynamoCodecError.DecodeError(e.getMessage, Some(e)))

  def encodeZIO(value: A): IO[DynamoCodecError, java.util.Map[String, AttributeValue]] =
    ZIO
      .attempt:
        val map = new java.util.HashMap[String, AttributeValue]()
        encode(value, map)
        map
      .mapError(e => DynamoCodecError.EncodeError(e.getMessage, Some(e)))

object VersionedCodec:

  def apply[A](using codec: DynamoDBCodec[A]): VersionedCodec[A] =
    new VersionedCodec[A](codec, List.empty)

  def fromCodec[A](codec: DynamoDBCodec[A]): VersionedCodec[A] =
    new VersionedCodec[A](codec, List.empty)

  final private[codec] class Fallback[Old, A](codec: DynamoDBCodec[Old], migrate: Old => A):
    def tryDecode(input: java.util.Map[String, AttributeValue]): Either[SchemaError, A] =
      codec.decode(input).map(migrate)
