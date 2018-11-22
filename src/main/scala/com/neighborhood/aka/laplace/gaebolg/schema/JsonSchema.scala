/*
 * Copyright (c) 2015 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.neighborhood.aka.laplace.gaebolg.schema

// Scalaz
import com.neighborhood.aka.laplace.gaebolg.generators.SchemaGenerator
import com.neighborhood.aka.laplace.gaebolg.schema.Helpers.SchemaContext
import com.neighborhood.aka.laplace.gaebolg.schema.types._
import org.json4s

import scala.util.Try
import scalaz.Scalaz._
import scalaz._

// json4s
import org.json4s._

// This library
import com.neighborhood.aka.laplace.gaebolg.schema.types.ZeroSchema


/**
  * Base trait for all JSON Schema types
  * Primary methods which every subtype need to implement are:
  * `toJson` represent schema as JSON value
  * `mergeWithSameType` is fine-grained merge with two schemas of the same type
  */
abstract trait JsonSchema {

  /**
    * All Schemas need to have and implicitly pass SchemaContext to it's children
    */
  implicit val schemaContext: SchemaContext

  /**
    * Convert Schema into JSON
    *
    * @return JSON Object with Schema
    */
  def toJson: JObject

  /**
    * Get types of current Schema chunk
    * Types always can be product or absent at all, that's why it's
    * Set[String] and not a String
    *
    * @return set of types for Schema's `type` property
    */
  def getType: Set[String]

  /**
    * Return enum as JValue
    * Enum can be absent, can be sequence or consist of different types in product types
    * To contain enum Schema type need to use ``SchemaWithEnum`` trait
    *
    * @return seq of all possible values for this schema
    */
  def getJEnum: JValue = JNothing

  /**
    * Get partial function merging two same-type schemas with specified enum
    * cardinality
    * e.g. string and string or array and array
    *
    * @return always same type schema
    */
  def mergeSameType(implicit schemaContext: SchemaContext): PartialFunction[JsonSchema, JsonSchema]

  /**
    * Get partial function applying schema with different schema type,
    * creating product type for these two schemas
    * e.g. [string, integer], [object, array], [number, null]
    *
    * @return partial function will return product schema if given argument
    *         has different type
    */
  def createProduct: PartialFunction[JsonSchema, ProductSchema] = {
    case other => ProductSchema()(schemaContext).merge(this).merge(other)
  }

  /**
    * Get partial function merging this schema into product schema
    * e.g. string and [string, number]
    *
    * @return partial function will return product schema if given argument is
    *         product schema too
    */
  def mergeToProduct: PartialFunction[JsonSchema, ProductSchema] = {
    case prod: ProductSchema => prod.merge(this)
  }

  /**
    * Partial function merging this object with zero
    * Kind of identity function
    *
    * @return partial function will always return this schema
    */
  def mergeWithZero: PartialFunction[JsonSchema, JsonSchema] = {
    case ZeroSchema(_) => this
  }

  /**
    * Primary merge function consequentially applying all auxiliary partial functions:
    * `mergeSameType`, `mergeWithZero`, `mergeToProduct`, `createProduct`,
    * trying to guess how to merge these two schemas
    *
    * @param other schema to merge
    * @return merged schema
    */
  def merge(other: JsonSchema)(implicit schemaContext: SchemaContext): JsonSchema = {
    mergeSameType.orElse(mergeWithZero).orElse(mergeToProduct).orElse(createProduct).apply(other)
  }

  /**
    * Overloaded `merge` working with option
    * Will return current schema if other schema is None
    *
    * @param other optional Schema
    * @return same schema if `other` is None, merged schema otherwise
    */
  def merge(other: Option[JsonSchema])(implicit schemaContext: SchemaContext): JsonSchema = other match {
    case Some(o) => o.merge(this)
    case None => this
  }

  // Auxiliary functions

  /**
    * Get maximum value of two options. Or None if one of values is None
    *
    * @param first  any Option with ordering type
    * @param second any Option with ordering type
    * @return maximum value wrapped in Option
    */
  def maxOrNone[A: Order](first: Option[A], second: Option[A]): Option[A] =
    for {a <- first; b <- second} yield a max b

  /**
    * Get minimum value of two options. Or None if one of values is None
    *
    * @param first  any Option with ordering type
    * @param second any Option with ordering type
    * @return minimum value wrapped in Option
    */
  def minOrNone[A: Order](first: Option[A], second: Option[A]): Option[A] =
    for {a <- first; b <- second} yield a min b

  /**
    * Get value wrapped in option if two values are equal, None otherwise
    *
    * @param first  any Option with ordering type
    * @param second any Option with ordering type
    * @return minimum value wrapped in Option
    */
  def eqOrNone[A](first: Option[A], second: Option[A]): Option[A] =
    if (first == second) first
    else None
}


object JsonSchema {

  import org.json4s.jackson.JsonMethods._


  private val JSON_SCHEMA_TYPE = "type"
  implicit private val context = Helpers.SchemaContext(0)
  private val generator = SchemaGenerator(context)
  implicit private val format = DefaultFormats

  /**
    * 判断两个JsonSchema是否相同
    *
    * @param jsonSchema1 待判断的JsonSchema1
    * @param jsonSchema2 待判断的JsonSchema2
    * @return
    */
  private[jsonschema] def isSameJsonSchema(jsonSchema1: JsonSchema, jsonSchema2: JsonSchema): Boolean = {
    @inline
    def judgeByType(schemaType: String): Boolean = schemaType match {
      case "string" => jsonSchema1.toJson.toString == jsonSchema2.toJson.toString
      case "integer" => jsonSchema1.toJson.toString == jsonSchema2.toJson.toString
      case "null" => true
      case "boolean" => jsonSchema1.toJson.toString == jsonSchema2.toJson.toString
      case "zero" => true
      case "object" => jsonSchema1.toJson.toString == jsonSchema2.toJson.toString
      case "array" => jsonSchema1.toJson.toString == jsonSchema2.toJson.toString
      case "number" => jsonSchema1.toJson.toString == jsonSchema2.toJson.toString
    }

    if (jsonSchema1 == null && jsonSchema2 != null) return false
    if (jsonSchema1 == null && jsonSchema2 == null) return true
    if (jsonSchema1 != null && jsonSchema2 == null) return false
    if (jsonSchema1.getType != jsonSchema2.getType) return false
    jsonSchema1.getType.map(judgeByType(_)).forall(_.equals(true))

  }


  /**
    * 处理SchemaJson 尝试生成JsonSchema
    *
    * @param schemaJson the json string which describes json schema.
    * @return
    */
  private[jsonschema] def fromSchemaJson(schemaJson: String): Try[JsonSchema] = Try {
    val parsedJson = parse(schemaJson)
    recursivelyGenerateJsonSchema(parsedJson)
  }

  /**
    * 递归式的生成JsonSchema
    * 根据Json的AST,进行深度优先搜索
    *
    * @param jsonValue
    * @return
    */
  private def recursivelyGenerateJsonSchema(jsonValue: json4s.JValue): JsonSchema = {

    def handleSingleType(str: JString): JsonSchema = {
      generateJsonSchema(str.values, jsonValue)
    }

    def handleMultipleType(arr: JArray): JsonSchema = {
      if (arr.children.isEmpty) generateJsonSchema("zero", null)
      else arr.children.map(_.asInstanceOf[JString]).map(handleSingleType(_)).foldLeft(handleZeroSchema.asInstanceOf[JsonSchema]) { case (x, y) => x.merge(y) }
    }

    (jsonValue \ JSON_SCHEMA_TYPE) match {
      case arr: JArray => handleMultipleType(arr)
      case str: JString => handleSingleType(str)
      case _ => handleZeroSchema
    }
  }

  /**
    * 根据类型生成JsonSchema
    *
    * @param typeString
    * @param jsonValue
    * @return
    */
  @inline
  private def generateJsonSchema(typeString: String, jsonValue: json4s.JValue): JsonSchema = typeString match {
    case "string" => handleStringSchema(jsonValue)
    case "integer" => handleIntegerSchema(jsonValue)
    case "null" => handleNullSchema
    case "boolean" => handleBooleanSchema
    case "zero" => handleZeroSchema
    case "object" => handleObjectSchema(jsonValue)
    case "array" => handleArraySchema(jsonValue)
    case "number" => handleNumberSchema(jsonValue)

  }

  /**
    * 处理stringSchema类型
    * stringSchema 是`终结类型`
    * 目前尝试获取：
    *  1.minLength
    *  2.maxLength
    *
    * @param jsonValue
    * @return
    */
  @inline
  private def handleStringSchema(jsonValue: json4s.JValue): StringSchema = {
    lazy val minLength = Try((jsonValue \ "minLength")).map(_.extract[Int]).toOption
    lazy val maxLength = Try((jsonValue \ "maxLength")).map(_.extract[Int]).toOption
    StringSchema(minLength = minLength, maxLength = maxLength)
  }

  /**
    * 处理integerSchema类型
    * integerSchema 是`终结类型`
    * 目前尝试获取
    * 1.maximum
    * 2.minimum
    *
    * @param jsonValue
    * @return
    */
  @inline
  private def handleIntegerSchema(jsonValue: json4s.JValue): IntegerSchema = {
    lazy val maximum = Try((jsonValue \ "maximum")).map(_.extract[BigInt]).toOption
    lazy val minimum = Try((jsonValue \ "minimum")).map(_.extract[BigInt]).toOption
    IntegerSchema(maximum = maximum, minimum = minimum)
  }

  /**
    * 处理nullSchema类型
    * nullSchema 是`终结类型`
    *
    * @return
    */
  @inline
  private def handleNullSchema: NullSchema = NullSchema()

  /**
    * 处理booleanSchema
    * booleanSchema 是`终结类型`
    *
    * @return
    */
  @inline
  private def handleBooleanSchema: BooleanSchema = BooleanSchema()

  /**
    * 处理zeroSchema
    * zeroSchema 是`终结类型`
    * zeroSchema 作为"空"存在 幺元
    *
    * @return
    */
  @inline
  private def handleZeroSchema: ZeroSchema = ZeroSchema()

  /**
    * 处理arraySchema
    * arraySchemas 是`非终结类型`
    *
    * @param jsonValue
    * @return
    */
  @inline
  private def handleArraySchema(jsonValue: json4s.JValue): ArraySchema = {
    lazy val itemJsonValue = jsonValue \ "items"
    ArraySchema(recursivelyGenerateJsonSchema(itemJsonValue))
  }

  /**
    * 处理objectSchema
    * objectSchema 是 `非终结类型`
    *
    * @param jsonValue
    * @return
    */
  @inline
  private def handleObjectSchema(jsonValue: json4s.JValue): ObjectSchema = {
    lazy val properties = (jsonValue \ "properties").asInstanceOf[JObject]
    lazy val additionalProperties = (jsonValue \ "additionalProperties").toOption.map(_.extract[Boolean])
    lazy val map = properties.obj.map { case (name, jField) => (name -> recursivelyGenerateJsonSchema(jField)) }.toMap
    ObjectSchema(map)
  }

  /**
    * 处理numberSchema类型
    * numberSchema 是`终结类型`
    * 目前尝试获取
    * 1.maximum
    * 2.minimum
    *
    * @param jsonValue
    * @return
    */
  private def handleNumberSchema(jsonValue: json4s.JValue): NumberSchema = {
    lazy val maximum = Try((jsonValue \ "maximum")).map(_.extract[Double]).toOption
    lazy val minimum = Try(jsonValue \ "minimum").map(_.extract[Double]).toOption
    NumberSchema(maximum = maximum, minimum = minimum)
  }
}
