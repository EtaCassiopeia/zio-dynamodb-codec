package zio.blocks.schema.dynamodb

trait NameMapper:
  def apply(scalaFieldName: String): String

object NameMapper:

  val identity: NameMapper = (name: String) => name

  val snakeCase: NameMapper = (name: String) =>
    val len = name.length
    val sb  = new StringBuilder(len + 4)
    var i   = 0
    while i < len do
      val c = name.charAt(i)
      if c.isUpper then
        if i > 0 then
          val prevLower = name.charAt(i - 1).isLower
          val nextLower = i + 1 < len && name.charAt(i + 1).isLower
          if prevLower || nextLower then sb.append('_')
        sb.append(c.toLower)
      else sb.append(c)
      i += 1
    sb.toString

  def mapped(mappings: (String, String)*)(fallback: NameMapper = identity): NameMapper =
    val table = mappings.toMap
    (name: String) => table.getOrElse(name, fallback(name))
