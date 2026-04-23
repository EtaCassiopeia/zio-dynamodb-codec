package zio.blocks.schema.dynamodb

import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import zio.blocks.schema.SchemaError

enum KeyRole:
  case Partition, Sort, None

final private[dynamodb] class FieldMeta(
  val name: String,
  val keyRole: KeyRole,
  val prefix: Option[String],
  val idx: Int
)

abstract class DynamoDBCodec[A]:

  def encode(value: A, output: java.util.Map[String, AttributeValue]): Unit

  def decode(input: java.util.Map[String, AttributeValue]): Either[SchemaError, A]

  def encodeValue(value: A): AttributeValue

  def decodeValue(av: AttributeValue): Either[SchemaError, A]

  final def transform[B](from: A => B, to: B => A): DynamoDBCodec[B] =
    val self = this
    new DynamoDBCodec[B]:
      def encode(value: B, output: java.util.Map[String, AttributeValue]): Unit =
        self.encode(to(value), output)

      def decode(input: java.util.Map[String, AttributeValue]): Either[SchemaError, B] =
        self.decode(input).map(from)

      def encodeValue(value: B): AttributeValue =
        self.encodeValue(to(value))

      def decodeValue(av: AttributeValue): Either[SchemaError, B] =
        self.decodeValue(av).map(from)

  final def transformOrFail[B](from: A => Either[SchemaError, B], to: B => A): DynamoDBCodec[B] =
    val self = this
    new DynamoDBCodec[B]:
      def encode(value: B, output: java.util.Map[String, AttributeValue]): Unit =
        self.encode(to(value), output)

      def decode(input: java.util.Map[String, AttributeValue]): Either[SchemaError, B] =
        self.decode(input).flatMap(from)

      def encodeValue(value: B): AttributeValue =
        self.encodeValue(to(value))

      def decodeValue(av: AttributeValue): Either[SchemaError, B] =
        self.decodeValue(av).flatMap(from)

  /** Field metadata for record codecs. Returns empty array for non-record codecs. */
  private[dynamodb] def fieldMetadata: Array[FieldMeta] = Array.empty

  final def encodeSafe(value: A): Either[SchemaError, java.util.Map[String, AttributeValue]] =
    try
      val map = new java.util.HashMap[String, AttributeValue]()
      encode(value, map)
      Right(map)
    catch case e: SchemaError => Left(e)

  final def encodeValueSafe(value: A): Either[SchemaError, AttributeValue] =
    try Right(encodeValue(value))
    catch case e: SchemaError => Left(e)

object DynamoDBCodec:

  def primitive[A](
    enc: A => AttributeValue,
    dec: AttributeValue => Either[SchemaError, A]
  ): DynamoDBCodec[A] =
    new DynamoDBCodec[A]:
      def encode(value: A, output: java.util.Map[String, AttributeValue]): Unit =
        throw new UnsupportedOperationException("Primitive codec cannot encode to a top-level attribute map")

      def decode(input: java.util.Map[String, AttributeValue]): Either[SchemaError, A] =
        Left(SchemaError.expectationMismatch(Nil, "Primitive codec cannot decode from a top-level attribute map"))

      def encodeValue(value: A): AttributeValue = enc(value)

      def decodeValue(av: AttributeValue): Either[SchemaError, A] = dec(av)

  def record[A](
    enc: (A, java.util.Map[String, AttributeValue]) => Unit,
    dec: java.util.Map[String, AttributeValue] => Either[SchemaError, A],
    fieldMeta: Array[FieldMeta] = Array.empty
  ): DynamoDBCodec[A] =
    val meta = fieldMeta
    new DynamoDBCodec[A]:
      def encode(value: A, output: java.util.Map[String, AttributeValue]): Unit =
        enc(value, output)

      def decode(input: java.util.Map[String, AttributeValue]): Either[SchemaError, A] =
        dec(input)

      def encodeValue(value: A): AttributeValue =
        val map = new java.util.HashMap[String, AttributeValue]()
        enc(value, map)
        AttributeValue.builder().m(map).build()

      def decodeValue(av: AttributeValue): Either[SchemaError, A] =
        if av.hasM then dec(av.m())
        else Left(SchemaError.expectationMismatch(Nil, "Expected M (map) attribute"))

      override private[dynamodb] def fieldMetadata: Array[FieldMeta] = meta
