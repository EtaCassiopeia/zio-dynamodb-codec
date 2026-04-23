package zio.blocks.schema.dynamodb

/** Unified codec for a DynamoDB table item, combining full-item, key-only, and query codecs.
  *
  * Derived from a single annotated case class — eliminates the need for separate key types and schemas.
  *
  * @tparam A
  *   the item type (must have key fields annotated with `@Modifier.config("dynamodb.key", "partition"|"sort")`)
  */
final class TableCodec[A] private[dynamodb] (
  val codec: DynamoDBCodec[A],
  val key: KeyCodec[A],
  val query: QueryValues[A]
)

object TableCodec:

  private[dynamodb] def fromCodec[A](codec: DynamoDBCodec[A]): TableCodec[A] =
    val meta = codec.fieldMetadata
    KeyCodec.fromFieldMeta(codec, meta) match
      case Some(keyCodec) =>
        new TableCodec[A](codec, keyCodec, new QueryValues[A](keyCodec))
      case None =>
        throw new IllegalArgumentException(
          "Cannot create TableCodec: no partition key found. " +
            "Annotate your partition key field with @Modifier.config(\"dynamodb.key\", \"partition\")"
        )
