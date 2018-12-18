package com.neighborhood.aka.laplace.gaebolg.flatten

import com.neighborhood.aka.laplace.gaebolg.schema.{Helpers, JsonSchema}
import com.neighborhood.aka.laplace.gaebolg.schema.types._

/**
  * Created by john_liu on 2018/11/1.
  */
sealed trait FlattenJsonSchemaType

object FlattenJsonSchemaType {

  case object FlattenStringSchema extends FlattenJsonSchemaType

  case object FlattenNumberSchema extends FlattenJsonSchemaType

  case object FlattenNullSchema extends FlattenJsonSchemaType

  case object FlattenIntegerSchema extends FlattenJsonSchemaType

  case object FlattenBooleanSchema extends FlattenJsonSchemaType

  case object FlattenZeroSchema extends FlattenJsonSchemaType

  case object FlattenMongoDateSchema extends FlattenJsonSchemaType

  case class FlattenArraySchema(content: FlattenJsonSchemaType) extends FlattenJsonSchemaType

  lazy val MongoDateSchemaStub = StringSchema(pattern = Option("mongoDate"))(Helpers.SchemaContext(0))

  def getFlattenJsonSchemaType(js: JsonSchema): FlattenJsonSchemaType = js match {
    case `MongoDateSchemaStub` => FlattenMongoDateSchema
    case _: IntegerSchema => FlattenIntegerSchema
    case _: NullSchema => FlattenNullSchema
    case _: StringSchema => FlattenStringSchema
    case _: NumberSchema => FlattenNumberSchema
    case _: BooleanSchema => FlattenBooleanSchema
    case ArraySchema(content) => FlattenArraySchema(getFlattenJsonSchemaType(content))
    case _: ZeroSchema => FlattenZeroSchema
    case e => throw new Exception(s"cannot be this type:${e.getType}")
  }

}
