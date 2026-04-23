package zio.blocks.schema.dynamodb

import zio.blocks.schema.*

object Projection:

  def fieldNames[A](using schema: Schema[A], mapper: NameMapper = NameMapper.identity): List[String] =
    schema.reflect.asRecord match
      case Some(record) =>
        record.fields.map { field =>
          field.modifiers
            .collectFirst { case m: Modifier.rename => m.name }
            .getOrElse(mapper(field.name))
        }.toList
      case None => Nil

  def expression[A](using schema: Schema[A], mapper: NameMapper = NameMapper.identity): String =
    fieldNames[A].mkString(", ")

  def keyCondition(fieldName: String, prefix: String = ":"): String =
    s"$fieldName = $prefix$fieldName"

  def keyConditions(fieldNames: String*)(prefix: String = ":"): String =
    fieldNames.map(f => keyCondition(f, prefix)).mkString(" AND ")

  def beginsWithCondition(partitionKey: String, sortKey: String, prefix: String = ":"): String =
    s"${keyCondition(partitionKey, prefix)} AND begins_with($sortKey, $prefix$sortKey)"

  def nonKeyFields[A](keys: Set[String])(using
    schema: Schema[A],
    mapper: NameMapper = NameMapper.identity
  ): List[String] =
    fieldNames[A].filterNot(keys.contains)

  /** Extract key field metadata from a schema's @config("dynamodb.key", ...) annotations. */
  private def keyFields[A](using
    schema: Schema[A],
    mapper: NameMapper = NameMapper.identity
  ): (Option[String], Option[String]) =
    schema.reflect.asRecord match
      case Some(record) =>
        var pk: Option[String] = None
        var sk: Option[String] = None
        record.fields.foreach { field =>
          val name = field.modifiers
            .collectFirst { case m: Modifier.rename => m.name }
            .getOrElse(mapper(field.name))
          field.modifiers.foreach {
            case m: Modifier.config if m.key == "dynamodb.key" =>
              m.value match
                case "partition" => pk = Some(name)
                case "sort"      => sk = Some(name)
                case _           => ()
            case _ => ()
          }
        }
        (pk, sk)
      case None => (None, None)

  /** Auto-derived partition key condition from schema annotations. E.g. "contract_id = :contract_id" */
  def partitionKeyCondition[A](using schema: Schema[A], mapper: NameMapper = NameMapper.identity): String =
    keyFields[A] match
      case (Some(pk), _) => keyCondition(pk)
      case _ =>
        throw new IllegalArgumentException("No partition key annotated with @config(\"dynamodb.key\", \"partition\")")

  /** Auto-derived full key condition (PK AND SK) from schema annotations. */
  def fullKeyCondition[A](using schema: Schema[A], mapper: NameMapper = NameMapper.identity): String =
    keyFields[A] match
      case (Some(pk), Some(sk)) => keyConditions(pk, sk)()
      case (Some(pk), None)     => keyCondition(pk)
      case _ =>
        throw new IllegalArgumentException("No partition key annotated with @config(\"dynamodb.key\", \"partition\")")

  /** Auto-derived begins_with condition from schema annotations. */
  def derivedBeginsWithCondition[A](using schema: Schema[A], mapper: NameMapper = NameMapper.identity): String =
    keyFields[A] match
      case (Some(pk), Some(sk)) => beginsWithCondition(pk, sk)
      case _ => throw new IllegalArgumentException("Requires both partition and sort key annotations")
