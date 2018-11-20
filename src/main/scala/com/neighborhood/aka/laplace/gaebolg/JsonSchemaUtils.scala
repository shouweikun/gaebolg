package com.neighborhood.aka.laplace.gaebolg

import com.neighborhood.aka.laplace.gaebolg.schema.types.ZeroSchema
import com.finup.daas.jsonschema.schema.JsonSchema
import com.neighborhood.aka.laplace.gaebolg.exception.InvalidSchemaJsonException
import com.neighborhood.aka.laplace.gaebolg.generators.{SchemaGenerator, SchemaGeneratorNonRec}
import com.neighborhood.aka.laplace.gaebolg.schema.{Helpers, JsonSchema}

import scala.annotation.tailrec
// json4s
import org.json4s._
import org.json4s.jackson.JsonMethods._


object JsonSchemaUtils {
  implicit val context = Helpers.SchemaContext(0)
  lazy private val generator = SchemaGenerator(context)
  lazy private val generatorNonRec = SchemaGeneratorNonRec(context)

  /**
    * 通过json数据获取jsonschema
    *
    * @param jsonStr
    * @return
    */
  def getJsonSchema(jsonStr: String, rec: Boolean = true): Option[JsonSchema] = {
    lazy val json = parse(jsonStr)
    if (rec) generator.jsonToSchema(json).toOption else generatorNonRec.Json2Schema(json)

  }

  /**
    * 合并两个jsonschema
    *
    * @param jsonSchema1
    * @param jsonSchema2
    * @return
    */
  def mergeSchema(jsonSchema1: JsonSchema, jsonSchema2: JsonSchema): JsonSchema = {
    jsonSchema1.merge(jsonSchema2)
  }

  /**
    * 通过jsonschema字符串获取jsonschema对象
    *
    * @param jsonStr
    * @return
    */
  @throws[InvalidSchemaJsonException]
  def str2JsonSchema(jsonStr: String): JsonSchema = {
    JsonSchema.fromSchemaJson(jsonStr).getOrElse(throw new InvalidSchemaJsonException)
  }


  /**
    * 判断jsonschema是否相同
    *
    * @param jsonSchema1
    * @param jsonSchema2
    * @return
    */
  def isSameJsonSchema(jsonSchema1: JsonSchema, jsonSchema2: JsonSchema): Boolean = {
    JsonSchema.isSameJsonSchema(jsonSchema1, jsonSchema2)
  }

  /**
    * jsonSchema对象转字符串
    *
    * @param jsonSchema
    * @return
    */
  def schema2Str(jsonSchema: JsonSchema): String = {
    pretty(jsonSchema.toJson)
  }

  /**
    * 批量合成Schema
    * @todo 二路归并merge
    * @param jsonSchema
    * @param acc
    * @return
    */
  @tailrec
  def batchMergeSchema(jsonSchema: List[JsonSchema], acc: => JsonSchema = ZeroSchema()): JsonSchema = {
    jsonSchema match {
      case hd :: tl => batchMergeSchema(tl, (acc merge hd))
      case Nil => acc
    }
  }

}
