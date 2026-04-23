package zio.blocks.schema.dynamodb

import software.amazon.awssdk.services.dynamodb.model.AttributeValue

/** Produces DynamoDB expression attribute value maps for Query operations.
  *
  * Encodes key field values with the `:` prefix that DynamoDB's expression syntax requires.
  */
final class QueryValues[A] private[dynamodb] (private val keyCodec: KeyCodec[A]):

  private val prefix = ":"

  /** Encode only the partition key as an expression attribute value map. */
  def forPartitionKey(value: A): java.util.Map[String, AttributeValue] =
    val keyMap = keyCodec.encode(value)
    val result = new java.util.HashMap[String, AttributeValue](1)
    result.put(prefix + keyCodec.partitionKeyName, keyMap.get(keyCodec.partitionKeyName))
    result

  /** Encode partition key + sort key as expression attribute value maps. */
  def forKey(value: A): java.util.Map[String, AttributeValue] =
    val keyMap = keyCodec.encode(value)
    val result = new java.util.HashMap[String, AttributeValue](2)
    result.put(prefix + keyCodec.partitionKeyName, keyMap.get(keyCodec.partitionKeyName))
    keyCodec.sortKeyName.foreach { sk =>
      val skVal = keyMap.get(sk)
      if skVal != null then result.put(prefix + sk, skVal)
    }
    result

  /** Key condition expression for partition key equality. E.g. "contract_id = :contract_id" */
  def partitionKeyCondition: String =
    s"${keyCodec.partitionKeyName} = $prefix${keyCodec.partitionKeyName}"

  /** Key condition expression for partition + sort key equality. */
  def fullKeyCondition: String =
    keyCodec.sortKeyName match
      case Some(sk)   => s"$partitionKeyCondition AND $sk = $prefix$sk"
      case scala.None => partitionKeyCondition

  /** Key condition expression with begins_with on the sort key. */
  def beginsWithCondition: String =
    keyCodec.sortKeyName match
      case Some(sk)   => s"$partitionKeyCondition AND begins_with($sk, $prefix$sk)"
      case scala.None => partitionKeyCondition
