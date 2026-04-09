package zio.blocks.schema.dynamodb

import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import zio.blocks.docs.Doc
import zio.blocks.schema.*
import zio.blocks.schema.binding.*
import zio.blocks.schema.derive.*
import zio.blocks.typeid.TypeId

class DynamoDBCodecDeriver extends Deriver[DynamoDBCodec]:

  override def derivePrimitive[A](
    primitiveType: PrimitiveType[A],
    typeId: TypeId[A],
    binding: Binding.Primitive[A],
    doc: Doc,
    modifiers: Seq[Modifier.Reflect],
    defaultValue: Option[A],
    examples: Seq[A]
  ): Lazy[DynamoDBCodec[A]] =
    Lazy(primitiveCodec(primitiveType))

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def primitiveCodec[A](pt: PrimitiveType[A]): DynamoDBCodec[A] =
    (pt match
      case _: PrimitiveType.String  => stringCodec
      case _: PrimitiveType.Boolean => booleanCodec
      case _: PrimitiveType.Int     => numCodec[Int](_.toString, _.toInt)
      case _: PrimitiveType.Long    => numCodec[Long](_.toString, _.toLong)
      case _: PrimitiveType.Double  => numCodec[Double](_.toString, _.toDouble)
      case _ => throw new UnsupportedOperationException(s"Unsupported primitive type: $pt")
    ).asInstanceOf[DynamoDBCodec[A]]

  private val stringCodec: DynamoDBCodec[String] = DynamoDBCodec.primitive[String](
    a => AttributeValue.builder().s(a).build(),
    av => expectS(av)
  )

  private val booleanCodec: DynamoDBCodec[Boolean] = DynamoDBCodec.primitive[Boolean](
    a => AttributeValue.builder().bool(a).build(),
    av =>
      if av.bool() != null then Right(av.bool().booleanValue())
      else Left(SchemaError.expectationMismatch(Nil, "Expected BOOL attribute"))
  )

  private def numCodec[A](enc: A => String, dec: String => A): DynamoDBCodec[A] =
    DynamoDBCodec.primitive[A](
      a => AttributeValue.builder().n(enc(a)).build(),
      av =>
        if av.n() != null then
          try Right(dec(av.n()))
          catch
            case e: NumberFormatException =>
              Left(SchemaError.expectationMismatch(Nil, s"Invalid number: ${av.n()}"))
        else Left(SchemaError.expectationMismatch(Nil, "Expected N (number) attribute"))
    )

  private def expectS(av: AttributeValue): Either[SchemaError, String] =
    if av.s() != null then Right(av.s())
    else Left(SchemaError.expectationMismatch(Nil, "Expected S (string) attribute"))

  private def tryParse[A](s: String, f: String => A): Either[SchemaError, A] =
    try Right(f(s))
    catch
      case e: Exception =>
        Left(SchemaError.expectationMismatch(Nil, s"Failed to parse '$s': ${e.getMessage}"))

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def resolveCodec[F[_, _]](meta: Any)(using hi: HasInstance[F]): DynamoDBCodec[Any] =
    instance(meta.asInstanceOf[F[Any, Any]])(using hi).force.asInstanceOf[DynamoDBCodec[Any]]

  override def deriveRecord[F[_, _], A](
    fields: IndexedSeq[Term[F, A, ?]],
    typeId: TypeId[A],
    binding: Binding.Record[A],
    doc: Doc,
    modifiers: Seq[Modifier.Reflect],
    defaultValue: Option[A],
    examples: Seq[A]
  )(implicit hasBinding: HasBinding[F], hasInstance: HasInstance[F]): Lazy[DynamoDBCodec[A]] =
    val fieldCount     = fields.size
    val constructor    = binding.constructor
    val deconstructor  = binding.deconstructor
    val totalRegisters = constructor.usedRegisters

    val bindingFields = fields.map { term =>
      Term[Binding, A, Any](term.name, term.value.asInstanceOf[Reflect[Binding, Any]], term.doc, term.modifiers)
    }
    val bindingRecord = Reflect.Record[Binding, A](
      bindingFields,
      typeId,
      binding,
      doc,
      modifiers,
      None,
      Seq.empty
    )
    val fieldRegisters = bindingRecord.registers

    Lazy {
      val fieldNames  = new Array[String](fieldCount)
      val fieldCodecs = new Array[DynamoDBCodec[Any]](fieldCount)

      var i = 0
      while i < fieldCount do
        fieldNames(i) = fields(i).name
        fieldCodecs(i) = resolveCodec[F](fields(i).value.metadata)
        i += 1

      DynamoDBCodec.record[A](
        enc = (value, output) =>
          val regs = Registers(totalRegisters)
          deconstructor.deconstruct(regs, 0, value)

          var idx = 0
          while idx < fieldCount do
            val fieldVal = fieldRegisters(idx).asInstanceOf[Register[Any]].get(regs, 0)
            val av       = fieldCodecs(idx).encodeValue(fieldVal)
            output.put(fieldNames(idx), av)
            idx += 1
        ,
        dec = input =>
          val regs               = Registers(totalRegisters)
          var idx                = 0
          var error: SchemaError = null

          while idx < fieldCount && error == null do
            val raw = input.get(fieldNames(idx))
            if raw == null then error = SchemaError.missingField(Nil, fieldNames(idx))
            else
              fieldCodecs(idx).decodeValue(raw) match
                case Right(v) =>
                  fieldRegisters(idx).asInstanceOf[Register[Any]].set(regs, 0, v)
                case Left(e) =>
                  error = e
            idx += 1

          if error != null then Left(error)
          else Right(constructor.construct(regs, 0))
      )
    }

  override def deriveVariant[F[_, _], A](
    cases: IndexedSeq[Term[F, A, ?]],
    typeId: TypeId[A],
    binding: Binding.Variant[A],
    doc: Doc,
    modifiers: Seq[Modifier.Reflect],
    defaultValue: Option[A],
    examples: Seq[A]
  )(implicit hasBinding: HasBinding[F], hasInstance: HasInstance[F]): Lazy[DynamoDBCodec[A]] =
    throw new UnsupportedOperationException("Variant derivation not yet implemented")

  override def deriveSequence[F[_, _], C[_], A](
    element: Reflect[F, A],
    typeId: TypeId[C[A]],
    binding: Binding.Seq[C, A],
    doc: Doc,
    modifiers: Seq[Modifier.Reflect],
    defaultValue: Option[C[A]],
    examples: Seq[C[A]]
  )(implicit hasBinding: HasBinding[F], hasInstance: HasInstance[F]): Lazy[DynamoDBCodec[C[A]]] =
    throw new UnsupportedOperationException("Sequence derivation not yet implemented")

  override def deriveMap[F[_, _], M[_, _], K, V](
    key: Reflect[F, K],
    value: Reflect[F, V],
    typeId: TypeId[M[K, V]],
    binding: Binding.Map[M, K, V],
    doc: Doc,
    modifiers: Seq[Modifier.Reflect],
    defaultValue: Option[M[K, V]],
    examples: Seq[M[K, V]]
  )(implicit hasBinding: HasBinding[F], hasInstance: HasInstance[F]): Lazy[DynamoDBCodec[M[K, V]]] =
    throw new UnsupportedOperationException("Map derivation not yet implemented")

  override def deriveWrapper[F[_, _], A, B](
    wrapped: Reflect[F, B],
    typeId: TypeId[A],
    binding: Binding.Wrapper[A, B],
    doc: Doc,
    modifiers: Seq[Modifier.Reflect],
    defaultValue: Option[A],
    examples: Seq[A]
  )(implicit hasBinding: HasBinding[F], hasInstance: HasInstance[F]): Lazy[DynamoDBCodec[A]] =
    throw new UnsupportedOperationException("Wrapper derivation not yet implemented")

  override def deriveDynamic[F[_, _]](
    binding: Binding.Dynamic,
    doc: Doc,
    modifiers: Seq[Modifier.Reflect],
    defaultValue: Option[DynamicValue],
    examples: Seq[DynamicValue]
  )(implicit hasBinding: HasBinding[F], hasInstance: HasInstance[F]): Lazy[DynamoDBCodec[DynamicValue]] =
    throw new UnsupportedOperationException("Dynamic derivation not yet implemented")

object DynamoDBCodecDeriver extends DynamoDBCodecDeriver
