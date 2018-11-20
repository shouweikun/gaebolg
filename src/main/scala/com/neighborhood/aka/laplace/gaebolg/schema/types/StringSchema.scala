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
package com.neighborhood.aka.laplace.gaebolg.schema.types

// Scalaz

import com.neighborhood.aka.laplace.gaebolg.schema.{JsonSchema, SchemaWithEnum}

import scalaz.Scalaz._
import scalaz._

// json4s
import org.json4s.JsonDSL._
import org.json4s._

// This library
import com.neighborhood.aka.laplace.gaebolg.schema.Helpers.SchemaContext

/**
 * Schema for string values
 * http://spacetelescope.github.io/understanding-json-schema/reference/string.html
 *
 * @param format one of possible format values
 * @param pattern regex pattern
 * @param maxLength maximum length of value
 * @param enum set of all acceptable values
 */
final case class StringSchema(
  format: Option[String] = None,
  pattern: Option[String] = None,
  minLength: Option[Int] = None,
  maxLength: Option[Int] = None,
  enum: Option[List[JValue]] = None
)(implicit val schemaContext: SchemaContext) extends JsonSchema with SchemaWithEnum {

  def toJson = ("type" -> "string") ~ ("format" -> format) ~ ("pattern" -> pattern) ~ ("minLength" -> minLength) ~ ("maxLength" -> maxLength) ~ ("enum" -> getJEnum)

  def mergeSameType(implicit schemaContext: SchemaContext) = {
    case StringSchema(otherFormat, otherPattern, otherMinLength, otherMaxLength, otherEnum) => {
      StringSchema(
        eqOrNone(otherFormat, format),
        eqOrNone(otherPattern, pattern),
        minOrNone(otherMinLength, minLength),
        maxOrNone(otherMaxLength, maxLength),
        mergeEnums(otherEnum)
      )
    }
  }

  def getType = Set("string")

  override def getJEnum: JValue = enum
}

