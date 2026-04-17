package zio.blocks.schema.dynamodb

import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import zio.blocks.schema.*

import scala.jdk.CollectionConverters.*

private[dynamodb] object DynamicValueCodec:

  private val DiscriminatorField = "_type"

  def encode(dv: DynamicValue): AttributeValue =
    dv match
      case DynamicValue.Primitive(pv) =>
        pv match
          case PrimitiveValue.String(s)     => AttributeValue.builder().s(s).build()
          case PrimitiveValue.Boolean(b)    => AttributeValue.builder().bool(b).build()
          case PrimitiveValue.Byte(n)       => AttributeValue.builder().n(n.toString).build()
          case PrimitiveValue.Short(n)      => AttributeValue.builder().n(n.toString).build()
          case PrimitiveValue.Int(n)        => AttributeValue.builder().n(n.toString).build()
          case PrimitiveValue.Long(n)       => AttributeValue.builder().n(n.toString).build()
          case PrimitiveValue.Float(n)      => AttributeValue.builder().n(n.toString).build()
          case PrimitiveValue.Double(n)     => AttributeValue.builder().n(n.toString).build()
          case PrimitiveValue.BigDecimal(n) => AttributeValue.builder().n(n.toString).build()
          case PrimitiveValue.BigInt(n)     => AttributeValue.builder().n(n.toString).build()
          case PrimitiveValue.Unit          => AttributeValue.builder().nul(true).build()
          case PrimitiveValue.Char(c)       => AttributeValue.builder().s(c.toString).build()
          case PrimitiveValue.UUID(u)       => AttributeValue.builder().s(u.toString).build()
          case _                            => AttributeValue.builder().s(pv.toString).build()
      case DynamicValue.Record(fields) =>
        val map = new java.util.HashMap[String, AttributeValue]()
        fields.foreach { case (name, value) => map.put(name, encode(value)) }
        AttributeValue.builder().m(map).build()
      case DynamicValue.Variant(caseName, value) =>
        val map = new java.util.HashMap[String, AttributeValue]()
        map.put(DiscriminatorField, AttributeValue.builder().s(caseName).build())
        value match
          case DynamicValue.Record(fields) =>
            fields.foreach { case (name, v) => map.put(name, encode(v)) }
          case other =>
            map.put("value", encode(other))
        AttributeValue.builder().m(map).build()
      case DynamicValue.Sequence(values) =>
        val list = new java.util.ArrayList[AttributeValue]()
        values.foreach(v => list.add(encode(v)))
        AttributeValue.builder().l(list).build()
      case DynamicValue.Map(entries) =>
        val map = new java.util.HashMap[String, AttributeValue]()
        entries.foreach { case (k, v) =>
          val keyStr = k match
            case DynamicValue.Primitive(pv) => pv.toString
            case other                      => other.toString
          map.put(keyStr, encode(v))
        }
        AttributeValue.builder().m(map).build()
      case _ =>
        AttributeValue.builder().nul(true).build()

  def decode(av: AttributeValue): Either[SchemaError, DynamicValue] =
    if av.s() != null then Right(DynamicValue.Primitive(PrimitiveValue.String(av.s())))
    else if av.n() != null then
      if av.n().contains(".") then Right(DynamicValue.Primitive(PrimitiveValue.Double(av.n().toDouble)))
      else Right(DynamicValue.Primitive(PrimitiveValue.Long(av.n().toLong)))
    else if av.bool() != null then Right(DynamicValue.Primitive(PrimitiveValue.Boolean(av.bool())))
    else if av.nul() != null && av.nul() then Right(DynamicValue.Primitive(PrimitiveValue.Unit))
    else if av.hasSs then
      Right(DynamicValue.Sequence(av.ss().asScala.map(s => DynamicValue.Primitive(PrimitiveValue.String(s))).toSeq*))
    else if av.hasNs then
      Right(
        DynamicValue.Sequence(
          av.ns()
            .asScala
            .map { n =>
              if n.contains(".") then DynamicValue.Primitive(PrimitiveValue.Double(n.toDouble))
              else DynamicValue.Primitive(PrimitiveValue.Long(n.toLong))
            }
            .toSeq*
        )
      )
    else if av.hasL then
      val items                               = av.l()
      val values                              = scala.collection.immutable.Vector.newBuilder[DynamicValue]
      var i                                   = 0
      var error: Either[SchemaError, Nothing] = null
      while i < items.size() && error == null do
        decode(items.get(i)) match
          case Right(v) => values += v
          case Left(e)  => error = Left(e)
        i += 1
      if error != null then error
      else Right(DynamicValue.Sequence(values.result()*))
    else if av.hasM then
      val entries                                = av.m()
      val fields                                 = scala.collection.immutable.Vector.newBuilder[(String, DynamicValue)]
      var mapError: Either[SchemaError, Nothing] = null
      entries.forEach { (k, v) =>
        if mapError == null then
          decode(v) match
            case Right(dv) => fields += ((k, dv))
            case Left(e)   => mapError = Left(e)
      }
      if mapError != null then mapError
      else Right(DynamicValue.Record(fields.result()*))
    else Left(SchemaError.expectationMismatch(Nil, "Unrecognized AttributeValue type"))
