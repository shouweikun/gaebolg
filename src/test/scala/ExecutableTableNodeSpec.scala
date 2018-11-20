import com.neighborhood.aka.laplace.gaebolg.flatten.ExecutableTableNodeAnalyzer
import com.neighborhood.aka.laplace.gaebolg.flatten.FlattenJsonSchemaType.FlattenArraySchema
import com.neighborhood.aka.laplace.gaebolg.JsonSchemaUtils
import com.neighborhood.aka.laplace.gaebolg.schema.JsonSchema
import org.json4s._

/**
  * Created by john_liu on 2018/11/2.
  */

class ExecutableTableNodeSpec extends UnitSpec {
  implicit val format = DefaultFormats


  val json1 =
    """
      {
        "a" : 1,
        "b" : "v"
      }

    """.stripMargin

  val json2 =
    """
      {
      "a":[1],
      "b":["v"]
      }
    """.stripMargin

  val json4 =
    """
      {
       "a":[1],
        "b":{
          "b1":1,
          "b2":["1"],
          "b3":[1.234],
          "b4":{
            "b44":1,
            "b45":[[44]],
            "b46":["ssd"]
          }
        }
      }
    """.stripMargin
  val json5_1 =
    """
      {
      "a":[1]
      }
    """.stripMargin

  val json5_2 =
    """
      {
      "a":{
      "a1":["a"]
      }
      }
    """.stripMargin

  val json6 =
    """
      {
      	"a": [{
      		"aa1": [
      			[1.245]
      		],
      		"aa3": [1.3],
      		"aa2": "113"
      	}, {
      		"aa4": ["11"]
      	}]
      }
    """.stripMargin

  val json7 = """{"a":1,"b":{"b1":[{"b11":"1","b12":2},{"b11":"3","b12":"2"}],"b2":[[1],[2]]}}"""
  val schemaJson8 =
    s"""{
       |  "type" : "object",
       |  "properties" : {
       |    "bankinfo" : {
       |      "type" : [ "array", "null" ],
       |      "items" : {
       |        "type" : "object",
       |        "properties" : {
       |          "mapping_id" : {
       |            "type" : "string",
       |            "minLength" : 14,
       |            "maxLength" : 19
       |          },
       |          "provider_userid" : {
       |            "type" : [ "string", "null" ],
       |            "minLength" : 24,
       |            "maxLength" : 64
       |          },
       |          "bank_name" : {
       |            "type" : "string",
       |            "minLength" : 3,
       |            "maxLength" : 16
       |          },
       |          "mobile" : {
       |            "type" : [ "string", "null" ],
       |            "minLength" : 11,
       |            "maxLength" : 13
       |          },
       |          "active_date" : {
       |            "type" : [ "string", "null" ],
       |            "minLength" : 26,
       |            "maxLength" : 26
       |          },
       |          "open_fpcard" : {
       |            "type" : [ "boolean", "null" ]
       |          },
       |          "card_type" : {
       |            "type" : "string",
       |            "minLength" : 3,
       |            "maxLength" : 6
       |          },
       |          "level" : {
       |            "type" : [ "integer", "null" ],
       |            "maximum" : 20,
       |            "minimum" : 10
       |          },
       |          "user_name" : {
       |            "type" : [ "string", "null" ],
       |            "minLength" : 1,
       |            "maxLength" : 22
       |          },
       |          "card_number" : {
       |            "type" : "string",
       |            "minLength" : 2,
       |            "maxLength" : 4
       |          },
       |          "sign_id" : {
       |            "type" : [ "string", "null" ],
       |            "minLength" : 5,
       |            "maxLength" : 16
       |          }
       |        },
       |        "additionalProperties" : false
       |      }
       |    },
       |    "task_id" : {
       |      "type" : "string",
       |      "minLength" : 36,
       |      "maxLength" : 36
       |    },
       |    "recenttraders" : {
       |      "type" : [ "array", "null" ],
       |      "items" : {
       |        "type" : "object",
       |        "properties" : {
       |          "nick_name" : {
       |            "type" : "string",
       |            "minLength" : 2,
       |            "maxLength" : 32
       |          },
       |          "mapping_id" : {
       |            "type" : "string",
       |            "minLength" : 14,
       |            "maxLength" : 19
       |          },
       |          "account" : {
       |            "type" : "string",
       |            "minLength" : 9,
       |            "maxLength" : 32
       |          },
       |          "real_name" : {
       |            "type" : "string",
       |            "minLength" : 2,
       |            "maxLength" : 32
       |          },
       |          "alipay_userid" : {
       |            "type" : "string",
       |            "minLength" : 16,
       |            "maxLength" : 16
       |          }
       |        },
       |        "additionalProperties" : false
       |      }
       |    },
       |    "wealth" : {
       |      "type" : [ "object", "null" ],
       |      "properties" : {
       |        "mapping_id" : {
       |          "type" : [ "string", "null" ],
       |          "minLength" : 14,
       |          "maxLength" : 19
       |        },
       |        "huabai_balance" : {
       |          "type" : "integer",
       |          "maximum" : 4999800,
       |          "minimum" : -1899228
       |        },
       |        "huabei_overdue_amount" : {
       |          "type" : [ "integer", "null" ],
       |          "maximum" : 2021901,
       |          "minimum" : 0
       |        },
       |        "taolicai" : {
       |          "type" : "integer",
       |          "maximum" : 10539,
       |          "minimum" : 0
       |        },
       |        "huabei_overdue_interest" : {
       |          "type" : [ "integer", "null" ],
       |          "maximum" : 238349,
       |          "minimum" : 0
       |        },
       |        "cjb" : {
       |          "type" : "integer",
       |          "maximum" : 786045,
       |          "minimum" : 0
       |        },
       |        "yeb" : {
       |          "type" : "integer",
       |          "maximum" : 22529314,
       |          "minimum" : 0
       |        },
       |        "yue" : {
       |          "type" : "integer",
       |          "maximum" : 16505005,
       |          "minimum" : 0
       |        },
       |        "zcb" : {
       |          "type" : "integer",
       |          "maximum" : 10345775,
       |          "minimum" : 0
       |        },
       |        "fund" : {
       |          "type" : "integer",
       |          "maximum" : 43210318,
       |          "minimum" : 0
       |        },
       |        "huabei_overdue" : {
       |          "type" : [ "boolean", "null" ]
       |        },
       |        "huabai_limit" : {
       |          "type" : "integer",
       |          "maximum" : 6350000,
       |          "minimum" : 0
       |        }
       |      },
       |      "additionalProperties" : false
       |    },
       |    "alipaydeliveraddresses" : {
       |      "type" : "array",
       |      "items" : {
       |        "type" : "object",
       |        "properties" : {
       |          "mapping_id" : {
       |            "type" : "string",
       |            "minLength" : 14,
       |            "maxLength" : 19
       |          },
       |          "address" : {
       |            "type" : "string",
       |            "minLength" : 1,
       |            "maxLength" : 139
       |          },
       |          "province" : {
       |            "type" : "string",
       |            "minLength" : 2,
       |            "maxLength" : 8
       |          },
       |          "phone_number" : {
       |            "type" : "string",
       |            "minLength" : 1,
       |            "maxLength" : 18
       |          },
       |          "area_code" : {
       |            "type" : "string",
       |            "minLength" : 6,
       |            "maxLength" : 6
       |          },
       |          "area" : {
       |            "type" : "string",
       |            "minLength" : 0,
       |            "maxLength" : 15
       |          },
       |          "city" : {
       |            "type" : "string",
       |            "minLength" : 0,
       |            "maxLength" : 11
       |          },
       |          "name" : {
       |            "type" : "string",
       |            "minLength" : 1,
       |            "maxLength" : 60
       |          },
       |          "full_address" : {
       |            "type" : "string",
       |            "minLength" : 4,
       |            "maxLength" : 149
       |          },
       |          "post_code" : {
       |            "type" : "string",
       |            "minLength" : 0,
       |            "maxLength" : 16
       |          }
       |        },
       |        "additionalProperties" : false
       |      }
       |    },
       |    "create_time" : {
       |      "type" : "string",
       |      "minLength" : 19,
       |      "maxLength" : 19
       |    },
       |    "alipayjiebei" : {
       |      "type" : [ "object", "null" ],
       |      "properties" : {
       |        "mapping_id" : {
       |          "type" : "string",
       |          "minLength" : 14,
       |          "maxLength" : 19
       |        },
       |        "credit_amt" : {
       |          "type" : "number",
       |          "maximum" : 8.70714E7,
       |          "minimum" : 0
       |        },
       |        "risk_int_by_thousand" : {
       |          "type" : "number",
       |          "maximum" : 5.5,
       |          "minimum" : 0
       |        },
       |        "ovd_able" : {
       |          "type" : "boolean"
       |        },
       |        "refuse_reason" : {
       |          "type" : [ "string", "null" ],
       |          "minLength" : 0,
       |          "maxLength" : 24
       |        },
       |        "binded_mobile" : {
       |          "type" : [ "string", "null" ],
       |          "minLength" : 0,
       |          "maxLength" : 4
       |        },
       |        "new_able" : {
       |          "type" : "boolean"
       |        },
       |        "loanable_amt" : {
       |          "type" : "number",
       |          "maximum" : 1.234567E8,
       |          "minimum" : 0
       |        }
       |      },
       |      "additionalProperties" : false
       |    },
       |    "userinfo" : {
       |      "type" : [ "object", "null" ],
       |      "properties" : {
       |        "mapping_id" : {
       |          "type" : [ "string", "null" ],
       |          "minLength" : 14,
       |          "maxLength" : 19
       |        },
       |        "login_name" : {
       |          "type" : "null"
       |        },
       |        "certified" : {
       |          "type" : "boolean"
       |        },
       |        "register_time" : {
       |          "type" : [ "string", "null" ],
       |          "minLength" : 26,
       |          "maxLength" : 26
       |        },
       |        "gender" : {
       |          "type" : [ "string", "null" ],
       |          "minLength" : 4,
       |          "maxLength" : 6
       |        },
       |        "phone_number" : {
       |          "type" : [ "string", "null" ],
       |          "minLength" : 3,
       |          "maxLength" : 14
       |        },
       |        "idcard_number" : {
       |          "type" : [ "string", "null" ],
       |          "minLength" : 4,
       |          "maxLength" : 18
       |        },
       |        "email" : {
       |          "type" : [ "string", "null" ],
       |          "minLength" : 0,
       |          "maxLength" : 34
       |        },
       |        "user_name" : {
       |          "type" : [ "string", "null" ],
       |          "minLength" : 2,
       |          "maxLength" : 19
       |        },
       |        "taobao_id" : {
       |          "type" : [ "string", "null" ],
       |          "minLength" : 2,
       |          "maxLength" : 25
       |        },
       |        "alipay_userid" : {
       |          "type" : [ "string", "null" ],
       |          "minLength" : 0,
       |          "maxLength" : 16
       |        }
       |      },
       |      "additionalProperties" : false
       |    },
       |    "user_id" : {
       |      "type" : "string",
       |      "minLength" : 32,
       |      "maxLength" : 32
       |    },
       |    "_id" : {
       |      "type" : "string",
       |      "minLength" : 24,
       |      "maxLength" : 24
       |    },
       |    "alipaycontacts" : {
       |      "type" : [ "array", "null" ],
       |      "items" : {
       |        "type" : "object",
       |        "properties" : {
       |          "mapping_id" : {
       |            "type" : "string",
       |            "minLength" : 14,
       |            "maxLength" : 19
       |          },
       |          "account" : {
       |            "type" : "string",
       |            "minLength" : 9,
       |            "maxLength" : 32
       |          },
       |          "real_name" : {
       |            "type" : "string",
       |            "minLength" : 0,
       |            "maxLength" : 32
       |          },
       |          "alipay_userid" : {
       |            "type" : "string",
       |            "minLength" : 16,
       |            "maxLength" : 16
       |          }
       |        },
       |        "additionalProperties" : false
       |      }
       |    },
       |    "tradeinfo" : {
       |      "type" : [ "array", "null" ],
       |      "items" : {
       |        "type" : "object",
       |        "properties" : {
       |          "mapping_id" : {
       |            "type" : "string",
       |            "minLength" : 14,
       |            "maxLength" : 19
       |          },
       |          "trade_amount" : {
       |            "type" : "number",
       |            "maximum" : 7.999989E10,
       |            "minimum" : 0
       |          },
       |          "counterparty" : {
       |            "type" : [ "string", "null" ],
       |            "minLength" : 0,
       |            "maxLength" : 217
       |          },
       |          "trade_number" : {
       |            "type" : "string",
       |            "minLength" : 1,
       |            "maxLength" : 86
       |          },
       |          "trade_status" : {
       |            "type" : "string",
       |            "minLength" : 0,
       |            "maxLength" : 11
       |          },
       |          "refund" : {
       |            "type" : [ "number", "null" ],
       |            "maximum" : 3160000.0,
       |            "minimum" : 0
       |          },
       |          "comments" : {
       |            "type" : [ "string", "null" ],
       |            "minLength" : 0,
       |            "maxLength" : 83
       |          },
       |          "product_name" : {
       |            "type" : [ "string", "null" ],
       |            "minLength" : 1,
       |            "maxLength" : 594
       |          },
       |          "capital_status" : {
       |            "type" : [ "string", "null" ],
       |            "minLength" : 0,
       |            "maxLength" : 4
       |          },
       |          "trade_time" : {
       |            "type" : "string",
       |            "minLength" : 26,
       |            "maxLength" : 26
       |          },
       |          "trade_type" : {
       |            "type" : [ "string", "null" ],
       |            "minLength" : 4,
       |            "maxLength" : 7
       |          },
       |          "service_charge" : {
       |            "type" : [ "number", "null" ],
       |            "maximum" : 15669.0,
       |            "minimum" : 0
       |          },
       |          "trade_location" : {
       |            "type" : [ "string", "null" ],
       |            "minLength" : 2,
       |            "maxLength" : 15
       |          },
       |          "incomeorexpense" : {
       |            "type" : "string",
       |            "minLength" : 0,
       |            "maxLength" : 2
       |          }
       |        },
       |        "additionalProperties" : false
       |      }
       |    }
       |  },
       |  "additionalProperties" : false
       |}""".stripMargin

  val schemaJson9 =
    """
      {"type":"object",
       "properties":{
       |"address" : {
 |                 "type" : [ "number", "string" ],
 |                "minLength" : 1,
 |                "maxLength" : 139
 |              }
       }
      }
    """.stripMargin

  val schemaJson10 =
    s"""{
       |  "type" : "object",
       |  "properties" : {
       |    "task_id" : {
       |      "type" : "string",
       |      "minLength" : 36,
       |      "maxLength" : 36
       |    },
       |    "mobile" : {
       |      "type" : "string",
       |      "minLength" : 11,
       |      "maxLength" : 11
       |    },
       |    "create_time" : {
       |      "type" : "string",
       |      "minLength" : 19,
       |      "maxLength" : 19
       |    },
       |    "user_id" : {
       |      "type" : "string",
       |      "minLength" : 32,
       |      "maxLength" : 32
       |    },
       |    "_id" : {
       |      "type" : "string",
       |      "minLength" : 24,
       |      "maxLength" : 24
       |    },
       |    "bill" : {
       |      "type" : "array",
       |      "items" : {
       |        "type" : "string",
       |        "minLength" : 7,
       |        "maxLength" : 7
       |      }
       |    },
       |    "nets" : {
       |      "type" : "array",
       |      "items" : {
       |        "type" : "object",
       |        "properties" : {
       |          "total_size" : {
       |            "type" : "integer",
       |            "maximum" : 29456,
       |            "minimum" : 0
       |          },
       |          "items" : {
       |            "type" : "array",
       |            "items" : {
       |              "type" : "object",
       |              "properties" : {
       |                "duration" : {
       |                  "type" : [ "integer", "null" ],
       |                  "maximum" : 2147483647,
       |                  "minimum" : -2147483648
       |                },
       |                "subflow" : {
       |                  "type" : "integer",
       |                  "maximum" : 2097151,
       |                  "minimum" : -2097152
       |                },
       |                "details_id" : {
       |                  "type" : "string",
       |                  "minLength" : 30,
       |                  "maxLength" : 33
       |                },
       |                "fee" : {
       |                  "type" : "integer",
       |                  "maximum" : 25223000,
       |                  "minimum" : -8347
       |                },
       |                "time" : {
       |                  "type" : "string",
       |                  "minLength" : 19,
       |                  "maxLength" : 19
       |                },
       |                "service_name" : {
       |                  "type" : [ "string", "null" ],
       |                  "minLength" : 0,
       |                  "maxLength" : 125
       |                },
       |                "net_type" : {
       |                  "type" : [ "string", "null" ],
       |                  "minLength" : 0,
       |                  "maxLength" : 99
       |                },
       |                "location" : {
       |                  "type" : [ "string", "null" ],
       |                  "minLength" : 0,
       |                  "maxLength" : 34
       |                }
       |              },
       |              "additionalProperties" : false
       |            }
       |          },
       |          "bill_month" : {
       |            "type" : "string",
       |            "minLength" : 7,
       |            "maxLength" : 7
       |          }
       |        },
       |        "additionalProperties" : false
       |      }
       |    }
       |  },
       |  "additionalProperties" : false
       |}""".stripMargin
  lazy val json1Schema = json2JsonSchema(json1)
  lazy val json2Schema = json2JsonSchema(json2)
  lazy val json1merge2Schema = mergeSchema(json1Schema, json2Schema)
  lazy val json4Schema = json2JsonSchema(json4)
  lazy val json5Schema = mergeSchema(json2JsonSchema(json5_1), json2JsonSchema(json5_2))
  lazy val json6Schema = json2JsonSchema(json6)
  lazy val json7Schema = json2JsonSchema(json7)
  lazy val json8Schema = schemaJson2Schema(schemaJson8)
  val json9Schema = schemaJson2Schema(schemaJson9)
  val json10Schema = schemaJson2Schema(schemaJson10)
  "test 10" should "correctly build flattenSchema" in {
    val list = ExecutableTableNodeAnalyzer.recuisivelyAnalyzeJsonSchemaAndGenerateExcutableTableNodes(json10Schema, name = "d.x")
    assert(list.size == 1)
    val flattenList = ExecutableTableNodeAnalyzer.flattenExcutableTableNodeByStack(list)
    assert(flattenList.forall(_.depth <= 3))
    assert(flattenList.size == 3)
    val oringalList = flattenList
    val maxDepth = Option(1)
    val minDepth = Option(1)
    val filterDepthList = oringalList.filter {
      node =>
        val upper = maxDepth.fold(true)(max => node.depth <= max)
        val downer = minDepth.fold(true)(min => node.depth >= min)
        upper && downer
    }
    filterDepthList

    assert(filterDepthList.size == 1)
  }
  "test 9 " should "correctly build flattenSchema where only stringschema exist all other terminal schema Is ignored" in {
    val list = ExecutableTableNodeAnalyzer.recuisivelyAnalyzeJsonSchemaAndGenerateExcutableTableNodes(json9Schema, name = "d.x")
    assert(list.size == 1)
    val flattenList = ExecutableTableNodeAnalyzer.flattenExcutableTableNodeByStack(list)
    assert(flattenList.size == 1)
  }
  "test 8" should "correctly build flattenSchema where ignore nullschema" in {
    val list = ExecutableTableNodeAnalyzer.recuisivelyAnalyzeJsonSchemaAndGenerateExcutableTableNodes(json8Schema)
    assert(list.size == 8)
    val flattenList = ExecutableTableNodeAnalyzer.flattenExcutableTableNodeByStack(list)
    assert(flattenList.size == 9)
  }
  "test 7 " should "correctly build flattenSchema where product split table carrys arrayNest info " in {
    val list = ExecutableTableNodeAnalyzer.recuisivelyAnalyzeJsonSchemaAndGenerateExcutableTableNodes(json7Schema)
    assert(list.size == 2)
  }
  "test 6" should "correctly build flattenSchema where table is ArrayNested" in {
    val list = ExecutableTableNodeAnalyzer.recuisivelyAnalyzeJsonSchemaAndGenerateExcutableTableNodes(json6Schema)
    assert(list.size == 1)
    assert(list(0).isArrayNested)
    assert(list(0).allTableFieldInfo.size == 4)
    val flattenList = ExecutableTableNodeAnalyzer.flattenExcutableTableNodeByStack(list)
    assert(flattenList.size == 1)
  }
  "test 5" should "correctly build flattenSchema where contains type info in tableName on split" in {
    val list = ExecutableTableNodeAnalyzer.recuisivelyAnalyzeJsonSchemaAndGenerateExcutableTableNodes(json5Schema)
    assert(list.size == 2)
    assert(list.forall(x => x.tableName.contains("object") || x.tableName.contains("array")))
    val flattenList = ExecutableTableNodeAnalyzer.flattenExcutableTableNodeByStack(list)
    assert(flattenList.size == 2)
  }
  "test 4" should "correctly build flattenSchema where are object nesting object" in {
    val list = ExecutableTableNodeAnalyzer.recuisivelyAnalyzeJsonSchemaAndGenerateExcutableTableNodes(json4Schema)
    assert(list.size == 1)
    assert(list(0).currentTableFields.size == 3)
    assert(list(0).allTableFieldInfo.size == 7)
    val flattenList = ExecutableTableNodeAnalyzer.flattenExcutableTableNodeByStack(list)
    assert(flattenList.size == 3)

  }

  "test 3" should "correctly build flattenSchema where are product" in {
    val list = ExecutableTableNodeAnalyzer.recuisivelyAnalyzeJsonSchemaAndGenerateExcutableTableNodes(json1merge2Schema)
    assert(list.size == 4)
  }

  "test 2" should "correctly build flattenSchema which are array" in {
    val list = ExecutableTableNodeAnalyzer.recuisivelyAnalyzeJsonSchemaAndGenerateExcutableTableNodes(json2Schema)
    assert(list.size == 1)
    assert(list(0).allTableFieldInfo.size == 2)
    assert(list(0).allTableFieldInfo.forall(x => x.dataType.isInstanceOf[FlattenArraySchema]))
  }

  "test 1" should "build correctly  flattenSchema" in {
    val list = ExecutableTableNodeAnalyzer.recuisivelyAnalyzeJsonSchemaAndGenerateExcutableTableNodes(json1Schema)
    assert(list.size == 1)
    assert(list(0).allTableFieldInfo.size == 2)

  }


  private def json2JsonSchema(jsonString: String): JsonSchema = {
    JsonSchemaUtils.getJsonSchema(jsonString).get
  }

  private def schemaJson2Schema(jsonString: String): JsonSchema = {
    JsonSchemaUtils.str2JsonSchema(jsonString)
  }

  private def mergeSchema(schema1: JsonSchema, schema2: JsonSchema): JsonSchema = {
    JsonSchemaUtils.mergeSchema(schema1, schema2)
  }
}
