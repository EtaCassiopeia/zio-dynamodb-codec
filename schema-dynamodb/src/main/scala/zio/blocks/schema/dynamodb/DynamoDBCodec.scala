package zio.blocks.schema.dynamodb

import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import zio.blocks.schema.SchemaError

abstract class DynamoDBCodec[A]:

  def encode(value: A, output: java.util.Map[String, AttributeValue]): Unit

  def decode(input: java.util.Map[String, AttributeValue]): Either[SchemaError, A]

  def encodeValue(value: A): AttributeValue

  def decodeValue(av: AttributeValue): Either[SchemaError, A]

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
    dec: java.util.Map[String, AttributeValue] => Either[SchemaError, A]
  ): DynamoDBCodec[A] =
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
