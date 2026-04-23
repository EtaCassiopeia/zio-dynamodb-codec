package zio.blocks.schema.dynamodb

import software.amazon.awssdk.services.dynamodb.model.AttributeValue

/** Encodes only the primary key fields (partition key + optional sort key) from a full item.
  *
  * Useful for DynamoDB GetItem and DeleteItem operations which require just the key attributes.
  */
final class KeyCodec[A] private[dynamodb] (
  private val fullCodec: DynamoDBCodec[A],
  val partitionKeyName: String,
  val sortKeyName: Option[String],
  private val keyFieldNames: Set[String]
):

  /** Encode only key fields from a full item value. */
  def encode(value: A): java.util.Map[String, AttributeValue] =
    val full = new java.util.HashMap[String, AttributeValue]()
    fullCodec.encode(value, full)
    val result = new java.util.HashMap[String, AttributeValue](keyFieldNames.size)
    keyFieldNames.foreach { name =>
      val av = full.get(name)
      if av != null then result.put(name, av)
    }
    result

object KeyCodec:

  private[dynamodb] def fromFieldMeta[A](codec: DynamoDBCodec[A], meta: Array[FieldMeta]): Option[KeyCodec[A]] =
    var pkName: String = null
    var skName: String = null
    val keyNames       = new java.util.HashSet[String]()
    var i              = 0
    while i < meta.length do
      val fm = meta(i)
      fm.keyRole match
        case KeyRole.Partition =>
          pkName = fm.name
          keyNames.add(fm.name)
        case KeyRole.Sort =>
          skName = fm.name
          keyNames.add(fm.name)
        case KeyRole.None => ()
      i += 1

    if pkName == null then scala.None
    else
      val buf = scala.collection.mutable.ArrayBuffer[String]()
      keyNames.forEach(n => buf += n)
      val keySet = buf.toSet
      Some(new KeyCodec[A](codec, pkName, Option(skName), keySet))
