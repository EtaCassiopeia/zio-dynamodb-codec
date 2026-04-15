package zio.blocks.schema.dynamodb

import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import zio.blocks.docs.Doc
import zio.blocks.schema.*
import zio.blocks.schema.binding.*
import zio.blocks.schema.derive.*
import zio.blocks.typeid.TypeId

import java.time.*
import java.util.UUID

class DynamoDBCodecDeriver(val fieldNameMapper: NameMapper = NameMapper.identity) extends Deriver[DynamoDBCodec]:

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
      case _: PrimitiveType.String     => stringCodec
      case _: PrimitiveType.Boolean    => booleanCodec
      case _: PrimitiveType.Byte       => numCodec[Byte](_.toString, _.toByte)
      case _: PrimitiveType.Short      => numCodec[Short](_.toString, _.toShort)
      case _: PrimitiveType.Int        => numCodec[Int](_.toString, _.toInt)
      case _: PrimitiveType.Long       => numCodec[Long](_.toString, _.toLong)
      case _: PrimitiveType.Float      => numCodec[Float](_.toString, _.toFloat)
      case _: PrimitiveType.Double     => numCodec[Double](_.toString, _.toDouble)
      case _: PrimitiveType.BigInt     => numCodec[scala.math.BigInt](_.toString, s => scala.math.BigInt(s))
      case _: PrimitiveType.BigDecimal => numCodec[scala.math.BigDecimal](_.toString, s => scala.math.BigDecimal(s))
      case _: PrimitiveType.Char       => charCodec
      case PrimitiveType.Unit          => unitCodec
      case _: PrimitiveType.UUID           => uuidCodec
      case _: PrimitiveType.Instant        => temporalCodec[Instant](_.toString, Instant.parse)
      case _: PrimitiveType.LocalDate      => temporalCodec[LocalDate](_.toString, LocalDate.parse)
      case _: PrimitiveType.LocalDateTime  => temporalCodec[LocalDateTime](_.toString, LocalDateTime.parse)
      case _: PrimitiveType.LocalTime      => temporalCodec[LocalTime](_.toString, LocalTime.parse)
      case _: PrimitiveType.OffsetDateTime => temporalCodec[OffsetDateTime](_.toString, OffsetDateTime.parse)
      case _: PrimitiveType.OffsetTime     => temporalCodec[OffsetTime](_.toString, OffsetTime.parse)
      case _: PrimitiveType.ZonedDateTime  => temporalCodec[ZonedDateTime](_.toString, ZonedDateTime.parse)
      case _: PrimitiveType.Duration       => temporalCodec[Duration](_.toString, Duration.parse)
      case _: PrimitiveType.Period         => temporalCodec[Period](_.toString, Period.parse)
      case _: PrimitiveType.Year           => temporalCodec[Year](_.toString, Year.parse)
      case _: PrimitiveType.YearMonth      => temporalCodec[YearMonth](_.toString, YearMonth.parse)
      case _: PrimitiveType.MonthDay       => temporalCodec[MonthDay](_.toString, MonthDay.parse)
      case _: PrimitiveType.Month =>
        DynamoDBCodec.primitive[Month](
          a => AttributeValue.builder().s(a.name()).build(),
          av => expectS(av).flatMap(s => tryParse(s, Month.valueOf))
        )
      case _: PrimitiveType.DayOfWeek =>
        DynamoDBCodec.primitive[DayOfWeek](
          a => AttributeValue.builder().s(a.name()).build(),
          av => expectS(av).flatMap(s => tryParse(s, DayOfWeek.valueOf))
        )
      case _: PrimitiveType.ZoneId =>
        DynamoDBCodec.primitive[ZoneId](
          a => AttributeValue.builder().s(a.getId).build(),
          av => expectS(av).flatMap(s => tryParse(s, ZoneId.of))
        )
      case _: PrimitiveType.ZoneOffset =>
        DynamoDBCodec.primitive[ZoneOffset](
          a => AttributeValue.builder().s(a.getId).build(),
          av => expectS(av).flatMap(s => tryParse(s, ZoneOffset.of))
        )
      case _: PrimitiveType.Currency =>
        DynamoDBCodec.primitive[java.util.Currency](
          a => AttributeValue.builder().s(a.getCurrencyCode).build(),
          av => expectS(av).flatMap(s => tryParse(s, java.util.Currency.getInstance))
        )
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

  private val charCodec: DynamoDBCodec[Char] = DynamoDBCodec.primitive[Char](
    a => AttributeValue.builder().s(a.toString).build(),
    av =>
      expectS(av).flatMap(s =>
        if s.length == 1 then Right(s.charAt(0))
        else Left(SchemaError.expectationMismatch(Nil, "Expected single-char S attribute"))
      )
  )

  private val unitCodec: DynamoDBCodec[Unit] = DynamoDBCodec.primitive[Unit](
    _ => AttributeValue.builder().nul(true).build(),
    _ => Right(())
  )

  private val uuidCodec: DynamoDBCodec[UUID] = DynamoDBCodec.primitive[UUID](
    a => AttributeValue.builder().s(a.toString).build(),
    av => expectS(av).flatMap(s => tryParse(s, UUID.fromString))
  )

  private def temporalCodec[A](enc: A => String, dec: String => A): DynamoDBCodec[A] =
    DynamoDBCodec.primitive[A](
      a => AttributeValue.builder().s(enc(a)).build(),
      av => expectS(av).flatMap(s => tryParse(s, dec))
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

  private class FieldInfo(
    val name: String,
    val register: Register[?],
    val idx: Int,
    val isOptional: Boolean,
    val isTransient: Boolean
  ):
    var codec: DynamoDBCodec[Any]      = null
    var innerCodec: DynamoDBCodec[Any] = null

    def setCodec(c: DynamoDBCodec[?]): Unit =
      codec = c.asInstanceOf[DynamoDBCodec[Any]]

    def setInnerCodec(c: DynamoDBCodec[?]): Unit =
      innerCodec = c.asInstanceOf[DynamoDBCodec[Any]]

    def getFieldValue(regs: Registers): Any =
      register.asInstanceOf[Register[Any]].get(regs, 0)

    def setFieldValue(regs: Registers, value: Any): Unit =
      register.asInstanceOf[Register[Any]].set(regs, 0, value)

    def effectiveCodec: DynamoDBCodec[Any] =
      if innerCodec != null then innerCodec else codec

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
      val infos = new Array[FieldInfo](fieldCount)

      var i = 0
      while i < fieldCount do
        val field = fields(i)
        val name = field.modifiers
          .collectFirst { case m: Modifier.rename => m.name }
          .getOrElse(fieldNameMapper(field.name))
        val isOpt       = isOptionReflect(field.value)
        val isTransient = field.modifiers.exists(_.isInstanceOf[Modifier.transient])

        infos(i) = new FieldInfo(
          name = name,
          register = fieldRegisters(i),
          idx = i,
          isOptional = isOpt,
          isTransient = isTransient
        )
        i += 1

      // Resolve codecs
      var j = 0
      while j < fieldCount do
        infos(j).setCodec(resolveCodec[F](fields(j).value.metadata))
        if infos(j).isOptional then resolveOptionInnerCodec[F](fields(j), infos(j))
        j += 1

      DynamoDBCodec.record[A](
        enc = (value, output) =>
          val regs = Registers(totalRegisters)
          deconstructor.deconstruct(regs, 0, value)

          var idx = 0
          while idx < fieldCount do
            val fi = infos(idx)
            if !fi.isTransient then
              val fieldVal = fi.getFieldValue(regs)

              if fi.isOptional then
                fieldVal match
                  case None    => ()
                  case Some(v) => output.put(fi.name, fi.effectiveCodec.encodeValue(v))
                  case _       => output.put(fi.name, fi.codec.encodeValue(fieldVal))
              else output.put(fi.name, fi.codec.encodeValue(fieldVal))
            idx += 1
        ,
        dec = input =>
          val regs               = Registers(totalRegisters)
          var idx                = 0
          var error: SchemaError = null

          while idx < fieldCount && error == null do
            val fi = infos(idx)
            if fi.isTransient then () // skip
            else
              val raw = input.get(fi.name)

              if raw == null || (raw.nul() != null && raw.nul().booleanValue()) then
                if fi.isOptional then fi.setFieldValue(regs, None)
                else error = SchemaError.missingField(Nil, fi.name)
              else if fi.isOptional then
                fi.effectiveCodec.decodeValue(raw) match
                  case Right(v) => fi.setFieldValue(regs, Some(v))
                  case Left(e)  => error = e
              else
                fi.codec.decodeValue(raw) match
                  case Right(v) => fi.setFieldValue(regs, v)
                  case Left(e)  => error = e
            idx += 1

          if error != null then Left(error)
          else Right(constructor.construct(regs, 0))
      )
    }

  private def isOptionReflect[F[_, _]](reflect: Reflect[F, ?]): Boolean =
    reflect.asVariant.exists(_.typeId.name == "Option")

  private def resolveOptionInnerCodec[F[_, _]](
    field: Term[F, ?, ?],
    fi: FieldInfo
  )(using hasInstance: HasInstance[F]): Unit =
    val variant = field.value.asVariant.filter(v => isOptionReflect(field.value))
    variant.foreach { v =>
      v.cases.find(_.name == "Some").foreach { someTerm =>
        someTerm.value.asRecord.foreach { someRecord =>
          if someRecord.fields.nonEmpty then
            fi.setInnerCodec(resolveCodec[F](someRecord.fields(0).value.metadata))
        }
      }
    }

  private val DiscriminatorField = "_type"

  override def deriveVariant[F[_, _], A](
    cases: IndexedSeq[Term[F, A, ?]],
    typeId: TypeId[A],
    binding: Binding.Variant[A],
    doc: Doc,
    modifiers: Seq[Modifier.Reflect],
    defaultValue: Option[A],
    examples: Seq[A]
  )(implicit hasBinding: HasBinding[F], hasInstance: HasInstance[F]): Lazy[DynamoDBCodec[A]] =
    val variantBinding = binding
    val caseCount      = cases.size
    val caseNames      = new Array[String](caseCount)
    val lazyCaseMetas  = new Array[Any](caseCount)
    val caseNameMap    = new java.util.HashMap[String, Integer](caseCount)

    var i = 0
    while i < caseCount do
      caseNames(i) = cases(i).modifiers
        .collectFirst { case m: Modifier.rename => m.name }
        .getOrElse(cases(i).name)
      lazyCaseMetas(i) = cases(i).value.metadata
      caseNameMap.put(caseNames(i), Integer.valueOf(i))
      i += 1

    val isEnum = cases.forall(_.value.asRecord.exists(_.fields.isEmpty))

    Lazy:
      val caseCodecs = new Array[DynamoDBCodec[Any]](caseCount)
      var ci         = 0
      while ci < caseCount do
        caseCodecs(ci) = resolveCodec[F](lazyCaseMetas(ci))
        ci += 1

      if isEnum then
        DynamoDBCodec.primitive[A](
          value =>
            val idx = variantBinding.discriminator.discriminate(value)
            AttributeValue.builder().s(caseNames(idx)).build()
          ,
          av =>
            expectS(av).flatMap { s =>
              val idx = caseNameMap.get(s)
              if idx != null then
                caseCodecs(idx.intValue())
                  .decodeValue(AttributeValue.builder().m(java.util.Collections.emptyMap()).build())
                  .map(_.asInstanceOf[A])
              else Left(SchemaError.unknownCase(Nil, s))
            }
        )
      else
        DynamoDBCodec.record[A](
          enc = (value, output) =>
            val idx = variantBinding.discriminator.discriminate(value)
            output.put(DiscriminatorField, AttributeValue.builder().s(caseNames(idx)).build())
            caseCodecs(idx).encode(value, output)
          ,
          dec = input =>
            val discAv = input.get(DiscriminatorField)
            if discAv == null || discAv.s() == null then Left(SchemaError.missingField(Nil, DiscriminatorField))
            else
              val caseName = discAv.s()
              val idx      = caseNameMap.get(caseName)
              if idx != null then caseCodecs(idx.intValue()).decode(input).map(_.asInstanceOf[A])
              else Left(SchemaError.unknownCase(Nil, caseName))
        )

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def newSeqBuilder[C[_], A](constructor: zio.blocks.schema.binding.SeqConstructor[C], size: Int): Any =
    given scala.reflect.ClassTag[A] = scala.reflect.classTag[AnyRef].asInstanceOf[scala.reflect.ClassTag[A]]
    constructor.newBuilder[A](size)

  override def deriveSequence[F[_, _], C[_], A](
    element: Reflect[F, A],
    typeId: TypeId[C[A]],
    binding: Binding.Seq[C, A],
    doc: Doc,
    modifiers: Seq[Modifier.Reflect],
    defaultValue: Option[C[A]],
    examples: Seq[C[A]]
  )(implicit hasBinding: HasBinding[F], hasInstance: HasInstance[F]): Lazy[DynamoDBCodec[C[A]]] =
    // Special case: Array[Byte] → DynamoDB B (binary) attribute
    val isByteArray = element.asPrimitive.exists(_.primitiveType.isInstanceOf[PrimitiveType.Byte]) &&
      typeId.name == "Array"

    if isByteArray then
      return Lazy:
        DynamoDBCodec.primitive[C[A]](
          value =>
            val bytes = value.asInstanceOf[Array[Byte]]
            AttributeValue.builder().b(SdkBytes.fromByteArray(bytes)).build()
          ,
          av =>
            if av.b() != null then Right(av.b().asByteArray().asInstanceOf[C[A]])
            else Left(SchemaError.expectationMismatch(Nil, "Expected B (binary) attribute"))
        )

    val seqBinding       = binding
    val isSet            = typeId.name == "Set"
    val isStringElement  = element.asPrimitive.exists(_.primitiveType.isInstanceOf[PrimitiveType.String])
    val isNumericElement = element.asPrimitive.exists(p => isNumericPrimitive(p.primitiveType))

    instance(element.metadata.asInstanceOf[F[Any, Any]])(using hasInstance).map { elemCodecRaw =>
      val elemCodec = elemCodecRaw.asInstanceOf[DynamoDBCodec[Any]]

      if isSet && isStringElement then
        DynamoDBCodec.primitive[C[A]](
          value =>
            val items = new java.util.ArrayList[String]()
            val iter  = seqBinding.deconstructor.deconstruct[A](value)
            while iter.hasNext do items.add(iter.next().toString)
            if items.isEmpty then
              throw SchemaError.expectationMismatch(Nil, "DynamoDB does not support empty sets (SS)")
            AttributeValue.builder().ss(items).build()
          ,
          av =>
            if av.hasSs then
              val seqBuilder = newSeqBuilder[C, A](seqBinding.constructor, av.ss().size())
                .asInstanceOf[seqBinding.constructor.Builder[A]]
              av.ss().forEach(s => seqBinding.constructor.add(seqBuilder, s.asInstanceOf[A]))
              Right(seqBinding.constructor.result(seqBuilder))
            else if av.hasL then decodeAsList(av, elemCodec, seqBinding)
            else Left(SchemaError.expectationMismatch(Nil, "Expected SS (string set) or L attribute"))
        )
      else if isSet && isNumericElement then
        DynamoDBCodec.primitive[C[A]](
          value =>
            val items = new java.util.ArrayList[String]()
            val iter  = seqBinding.deconstructor.deconstruct[A](value)
            while iter.hasNext do items.add(iter.next().toString)
            if items.isEmpty then
              throw SchemaError.expectationMismatch(Nil, "DynamoDB does not support empty sets (NS)")
            AttributeValue.builder().ns(items).build()
          ,
          av =>
            if av.hasNs then
              val seqBuilder = newSeqBuilder[C, A](seqBinding.constructor, av.ns().size())
                .asInstanceOf[seqBinding.constructor.Builder[A]]
              av.ns().forEach { nStr =>
                val decoded = elemCodec.decodeValue(AttributeValue.builder().n(nStr).build())
                decoded.foreach(v => seqBinding.constructor.add(seqBuilder, v.asInstanceOf[A]))
              }
              Right(seqBinding.constructor.result[A](seqBuilder))
            else if av.hasL then decodeAsList(av, elemCodec, seqBinding)
            else Left(SchemaError.expectationMismatch(Nil, "Expected NS (number set) or L attribute"))
        )
      else
        DynamoDBCodec.primitive[C[A]](
          value =>
            val builder = new java.util.ArrayList[AttributeValue]()
            val iter    = seqBinding.deconstructor.deconstruct[A](value)
            while iter.hasNext do builder.add(elemCodec.encodeValue(iter.next()))
            AttributeValue.builder().l(builder).build()
          ,
          av => decodeAsList(av, elemCodec, seqBinding)
        )
    }

  private def decodeAsList[C[_], A](
    av: AttributeValue,
    elemCodec: DynamoDBCodec[Any],
    seqBinding: Binding.Seq[C, A]
  ): Either[SchemaError, C[A]] =
    if av.hasL then
      val items = av.l()
      val seqBuilder =
        newSeqBuilder[C, A](seqBinding.constructor, items.size()).asInstanceOf[seqBinding.constructor.Builder[A]]
      var i                  = 0
      var error: SchemaError = null
      while i < items.size() && error == null do
        elemCodec.decodeValue(items.get(i)) match
          case Right(v) =>
            seqBinding.constructor.add(seqBuilder, v.asInstanceOf[A])
          case Left(e) =>
            error = e
        i += 1
      if error != null then Left(error)
      else Right(seqBinding.constructor.result[A](seqBuilder))
    else Left(SchemaError.expectationMismatch(Nil, "Expected L (list) attribute"))

  private def isNumericPrimitive(pt: PrimitiveType[?]): Boolean =
    pt match
      case _: PrimitiveType.Byte | _: PrimitiveType.Short | _: PrimitiveType.Int | _: PrimitiveType.Long |
          _: PrimitiveType.Float | _: PrimitiveType.Double | _: PrimitiveType.BigInt | _: PrimitiveType.BigDecimal =>
        true
      case _ => false

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
    val mapBinding  = binding
    val isStringKey = key.asPrimitive.exists(_.primitiveType.isInstanceOf[PrimitiveType.String])

    val keyLazy   = instance(key.metadata.asInstanceOf[F[Any, Any]])(using hasInstance)
    val valueLazy = instance(value.metadata.asInstanceOf[F[Any, Any]])(using hasInstance)

    keyLazy.flatMap { keyCodecRaw =>
      valueLazy.map { valueCodecRaw =>
        val keyCodec   = keyCodecRaw.asInstanceOf[DynamoDBCodec[Any]]
        val valueCodec = valueCodecRaw.asInstanceOf[DynamoDBCodec[Any]]

        if isStringKey then
          DynamoDBCodec.primitive[M[K, V]](
            m =>
              val builder = new java.util.HashMap[String, AttributeValue]()
              val iter    = mapBinding.deconstructor.deconstruct[K, V](m)
              while iter.hasNext do
                val entry = iter.next()
                val k     = mapBinding.deconstructor.getKey[K, V](entry)
                val v     = mapBinding.deconstructor.getValue[K, V](entry)
                builder.put(k.asInstanceOf[String], valueCodec.encodeValue(v))
              AttributeValue.builder().m(builder).build()
            ,
            av =>
              if av.hasM then
                val entries            = av.m()
                val mapBuilder         = mapBinding.constructor.newObjectBuilder[K, V](entries.size())
                var error: SchemaError = null
                entries.forEach { (k, v) =>
                  if error == null then
                    valueCodec.decodeValue(v) match
                      case Right(decoded) =>
                        mapBinding.constructor.addObject(mapBuilder, k.asInstanceOf[K], decoded.asInstanceOf[V])
                      case Left(e) =>
                        error = e
                }
                if error != null then Left(error)
                else Right(mapBinding.constructor.resultObject[K, V](mapBuilder))
              else Left(SchemaError.expectationMismatch(Nil, "Expected M (map) attribute"))
          )
        else
          DynamoDBCodec.primitive[M[K, V]](
            m =>
              val builder = new java.util.ArrayList[AttributeValue]()
              val iter    = mapBinding.deconstructor.deconstruct[K, V](m)
              while iter.hasNext do
                val entry = iter.next()
                val k     = mapBinding.deconstructor.getKey[K, V](entry)
                val v     = mapBinding.deconstructor.getValue[K, V](entry)
                val pair  = new java.util.HashMap[String, AttributeValue](2)
                pair.put("k", keyCodec.encodeValue(k))
                pair.put("v", valueCodec.encodeValue(v))
                builder.add(AttributeValue.builder().m(pair).build())
              AttributeValue.builder().l(builder).build()
            ,
            av =>
              if av.hasL then
                val items              = av.l()
                val mapBuilder         = mapBinding.constructor.newObjectBuilder[K, V](items.size())
                var i                  = 0
                var error: SchemaError = null
                while i < items.size() && error == null do
                  val entry = items.get(i)
                  if entry.hasM then
                    val entryMap = entry.m()
                    val kd       = keyCodec.decodeValue(entryMap.get("k"))
                    val vd       = valueCodec.decodeValue(entryMap.get("v"))
                    (kd, vd) match
                      case (Right(k), Right(v)) =>
                        mapBinding.constructor.addObject(mapBuilder, k.asInstanceOf[K], v.asInstanceOf[V])
                      case (Left(e), _) => error = e
                      case (_, Left(e)) => error = e
                  else error = SchemaError.expectationMismatch(Nil, s"Expected M attribute at index $i")
                  i += 1
                if error != null then Left(error)
                else Right(mapBinding.constructor.resultObject[K, V](mapBuilder))
              else Left(SchemaError.expectationMismatch(Nil, "Expected L (list) attribute for non-string-keyed map"))
          )
      }
    }

  override def deriveWrapper[F[_, _], A, B](
    wrapped: Reflect[F, B],
    typeId: TypeId[A],
    binding: Binding.Wrapper[A, B],
    doc: Doc,
    modifiers: Seq[Modifier.Reflect],
    defaultValue: Option[A],
    examples: Seq[A]
  )(implicit hasBinding: HasBinding[F], hasInstance: HasInstance[F]): Lazy[DynamoDBCodec[A]] =
    val wrapperBinding = binding

    instance(wrapped.metadata.asInstanceOf[F[Any, Any]])(using hasInstance).map { innerCodecRaw =>
      val innerCodec = innerCodecRaw.asInstanceOf[DynamoDBCodec[Any]]
      new DynamoDBCodec[A]:
        def encode(value: A, output: java.util.Map[String, AttributeValue]): Unit =
          innerCodec.encode(wrapperBinding.unwrap(value), output)

        def decode(input: java.util.Map[String, AttributeValue]): Either[SchemaError, A] =
          innerCodec.decode(input).map(b => wrapperBinding.wrap(b.asInstanceOf[B]))

        def encodeValue(value: A): AttributeValue =
          innerCodec.encodeValue(wrapperBinding.unwrap(value))

        def decodeValue(av: AttributeValue): Either[SchemaError, A] =
          innerCodec.decodeValue(av).map(b => wrapperBinding.wrap(b.asInstanceOf[B]))
    }

  override def deriveDynamic[F[_, _]](
    binding: Binding.Dynamic,
    doc: Doc,
    modifiers: Seq[Modifier.Reflect],
    defaultValue: Option[DynamicValue],
    examples: Seq[DynamicValue]
  )(implicit hasBinding: HasBinding[F], hasInstance: HasInstance[F]): Lazy[DynamoDBCodec[DynamicValue]] =
    throw new UnsupportedOperationException("Dynamic derivation not yet implemented")

object DynamoDBCodecDeriver extends DynamoDBCodecDeriver(NameMapper.identity)
