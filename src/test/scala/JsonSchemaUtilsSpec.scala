import com.neighborhood.aka.laplace.gaebolg.generators.SchemaGenerator
import com.neighborhood.aka.laplace.gaebolg.JsonSchemaUtils
import com.neighborhood.aka.laplace.gaebolg.schema.Helpers
import org.json4s.jackson.JsonMethods.parse
import org.json4s._
import org.json4s.jackson.JsonMethods._
class JsonSchemaUtilsSpec extends UnitSpec {

  it should "str2jsonschema is true -----1" in{
    val context = Helpers.SchemaContext(0)
    val generator = SchemaGenerator(context)

    val json1 = parse("""{"a":1,"b":{"b1":"hhh","b2":2}}""")
    val json1schema = generator.jsonToSchema(json1).toOption.get

    val json1schemaStr = pretty(json1schema.toJson)
    println(json1schemaStr)

    val json1schema_new = JsonSchemaUtils.str2JsonSchema(json1schemaStr)
    val json1schema_newStr = pretty(json1schema_new.toJson)
    println(json1schema_newStr)


    assert(json1schema_newStr==json1schemaStr)
    assert(JsonSchemaUtils.isSameJsonSchema(json1schema,json1schema_new))
  }


  it should "str2jsonschema is true -----2" in{
    val context = Helpers.SchemaContext(0)
    val generator = SchemaGenerator(context)

    val json1 = parse("""{"a":1,"b":{"b1":[{"bb1":1,"bb2":2},{"bb2":1,"bb3":2}],"b3":2,"b4":{"bb41":1}}}""")
    val json1schema = generator.jsonToSchema(json1).toOption.get

    val json1schemaStr = pretty(json1schema.toJson)
    println(json1schemaStr)

    val json1schema_new = JsonSchemaUtils.str2JsonSchema(json1schemaStr)
    val json1schema_newStr = pretty(json1schema_new.toJson)
    println(json1schema_newStr)


    assert(json1schema_newStr==json1schemaStr)
    assert(JsonSchemaUtils.isSameJsonSchema(json1schema,json1schema_new))
  }


  it should "str2jsonschema is true -----3" in{
    val context = Helpers.SchemaContext(0)
    val generator = SchemaGenerator(context)

    val json1 = parse("""{"a":1,"b":{"b1":[{"bb1":1,"bb2":2},{"bb2":1,"bb3":2}],"b3":2,"b4":{"bb41":1}}}""")
    val json1schema = generator.jsonToSchema(json1).toOption.get
    val json1schemaStr = pretty(json1schema.toJson)

    val json2 = parse("""{"a":1,"b":{"b3":2,"b1":[]}}""")
    val json2schema = generator.jsonToSchema(json2).toOption.get

    val jsonMerge = JsonSchemaUtils.mergeSchema(json1schema,json2schema)
    val jsonMergeStr = pretty(jsonMerge.toJson)



    println(json1schemaStr)
    println(jsonMergeStr)
    assert(json1schemaStr==jsonMergeStr)
    assert(JsonSchemaUtils.isSameJsonSchema(json1schema,jsonMerge))
  }

  it should "desc" in {
    val json1 = """{"a":1,"b":{"b1":[{"bb1":1,"bb2":2},{"bb2":1,"bb3":2}],"b3":2,"b4":{"bb41":1}}}"""
    val json1schema = JsonSchemaUtils.getJsonSchema(json1,false).get

    val json2 = """{"a":1,"b":{"b3":2,"b1":[]}}"""
    val json2schema = JsonSchemaUtils.getJsonSchema(json2).get


    val mergeschema = JsonSchemaUtils.mergeSchema(json1schema,json2schema)

    val schemaStr = JsonSchemaUtils.schema2Str(mergeschema)

    print(schemaStr)
  }

  it should "dd" in {
    val json = """"""
  }
}
