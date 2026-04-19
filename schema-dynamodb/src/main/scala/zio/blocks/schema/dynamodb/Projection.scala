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
