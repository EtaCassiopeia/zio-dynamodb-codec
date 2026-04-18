package zio.dynamodb.codec

sealed trait DynamoCodecError extends Throwable:
  def message: String
  override def getMessage: String = message

object DynamoCodecError:

  final case class DecodeError(message: String, cause: Option[Throwable] = None) extends DynamoCodecError:
    override def getCause: Throwable = cause.orNull

  final case class EncodeError(message: String, cause: Option[Throwable] = None) extends DynamoCodecError:
    override def getCause: Throwable = cause.orNull
