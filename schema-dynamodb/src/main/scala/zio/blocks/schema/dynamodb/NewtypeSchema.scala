package zio.blocks.schema.dynamodb

import zio.blocks.schema.*
import zio.blocks.typeid.{Owner, TypeId}

import scala.quoted.*

object NewtypeSchema:

  inline def apply[A, B](wrap: B => A, unwrap: A => B)(using base: Schema[B]): Schema[A] =
    given TypeId[A] = TypeId.nominal[A](typeName[A], Owner.Root)
    base.transform[A](wrap, unwrap)

  inline def validated[A, B](wrapFn: B => Either[String, A], unwrap: A => B)(using base: Schema[B]): Schema[A] =
    val totalWrap: B => A = b =>
      wrapFn(b) match
        case Right(a)  => a
        case Left(msg) => throw SchemaError.expectationMismatch(Nil, msg)
    given TypeId[A] = TypeId.nominal[A](typeName[A], Owner.Root)
    base.transform[A](totalWrap, unwrap)

  private inline def typeName[A]: String = ${ typeNameImpl[A] }

  private def typeNameImpl[A: Type](using Quotes): Expr[String] =
    import quotes.reflect.*
    Expr(TypeRepr.of[A].typeSymbol.name)
