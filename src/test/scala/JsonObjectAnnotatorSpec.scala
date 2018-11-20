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
package com.finup.daas.jsonschema
package schema

import com.neighborhood.aka.laplace.gaebolg.generators.SchemaGenerator
import com.neighborhood.aka.laplace.gaebolg.Helpers.SchemaContext
import com.neighborhood.aka.laplace.gaebolg.schema.Helpers
import com.neighborhood.aka.laplace.gaebolg.schema.types.ProductSchema
import org.json4s.jackson.Serialization._
// json4s
import org.json4s._
import org.json4s.jackson.JsonMethods._

// This library

object JsonObjectAnnotatorSpec  extends  App{
  val context = Helpers.SchemaContext(0)
  val generator = SchemaGenerator(context)

  val json1 = parse("""{"a":1,"b":{"b1":"hhh","b2":2}}""")
  val json1schema = generator.jsonToSchema(json1).toOption.get
  val json2 = parse("""{"a":1,"b":{"b1":"sdfsdfs","b3":2}}""")
  val json2schema = generator.jsonToSchema(json2).toOption.get
  val json3 = parse("""{"a":1,"b":{"b1":[1,2],"b3":2}}""")
  val json3schema = generator.jsonToSchema(json3).toOption.get
  val json4 = parse("""{"a":1,"b":{"b1":[{"bb1":1,"bb2":2},{"bb2":1,"bb3":2}],"b3":2,"b4":{"bb41":1}}}""")

  //    原表      a
  //              1
  //    原表.b    a  b.b3
  //              1  2
  //    原表.b.b1 a  b.b3 b.b1.bb1 b.b1.bb2 b.b1.bb3
  //              1   2     1          2
  //              1   2               1        2
  val json4schema = generator.jsonToSchema(json4).toOption.get


  val json5 = parse("""{"a":1,"b":{"b1":[1,2],"b3":2}}""")
  val json5schema = generator.jsonToSchema(json3).toOption.get

//  println(pretty(json5schema.toJson))


  implicit val ctx = SchemaContext(0)
  println(json1schema.merge(json2schema).merge(json3schema).merge(json4schema).toJson)

}
