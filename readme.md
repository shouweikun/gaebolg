# gaebolg

yet a simple [JSON Schema](http://json-schema.org/) extracting and flatting tool implemented by Scala 

gaebolg means "spear of mortal pain/death", was the name of the spear of Cúchulainn in the Ulster Cycle of Irish mythology.

一个简单的基于Scala实现的JSON Schema的提取和展开的工具

gaebolg的意思是致命之枪,取自爱尔兰神话 Ulster Cycle

 Current features include:
 
 - generate JSON schema from a set of JSON instances
 - from JSON schema instance to `String` and vice versa
 - flat a JSON schema and transform it into a set of `ExecutableTableNode`

目前的功能:

- 从一组JSON实例中生成一个JSON schema
- 字符串与JSON schema的互转
- 将一个JSON schema 转成一组`ExecutableTableNode`

### Dependency
currently depends on following libs:
- gaebolg uses [json4s](https://github.com/json4s/json4s) to do JSON related operation
- gaebolg uses [scalaz](https://github.com/scalaz/scalaz) for functional operation support

目前依赖以下库:

- gaebolg使用[json4s](https://github.com/json4s/json4s)处理json操作
- gaebolg使用[scalaz](https://github.com/scalaz/scalaz)提供FP特性


### Why I made this?
   Currently, in my company, there are lots of data stored as JSON, including but not limited to MongoDB,App logs. JSON now is popular with the whole world and more and more people become familiar with it, but there comes a so wired requirement that we have to transform json and flat it into several tables based on relational database model... Large quantities of table and lack of time, we need to find some ways to make this process more automatic instead of manually making the JSON schema, flatting the JSON data according to the schema. What's worse, for some reason, no one knows the precise JSON schema(seems terribly annoying,huh). So I made this tool to help me easily generate and flat json schema and hope to make json data flatting automatically.

   
### Guide & Explanation
  First,let me clarify some concepts
  - **JSON AST**
   
 >  the following code is the AST which models the structure of a JSON document as a syntax tree
  ```scala
sealed abstract class JValue
case object JNothing extends JValue // 'zero' for JValue
case object JNull extends JValue
case class JString(s: String) extends JValue
case class JDouble(num: Double) extends JValue
case class JDecimal(num: BigDecimal) extends JValue
case class JInt(num: BigInt) extends JValue
case class JLong(num: Long) extends JValue
case class JBool(value: Boolean) extends JValue
case class JObject(obj: List[JField]) extends JValue
case class JArray(arr: List[JValue]) extends JValue

type JField = (String, JValue)
```
json4s is strictly implemented the JSON AST which can be used easily, based on this JSON AST, gaebolg designs and implements its own `JSON Schema`

- **JSON Schema**

  this is core concepts in gaebolg.
  A set of JSON can be described as a complicated JSON Schema which contain a lot of JSON schema content
  
  we got 9 kind of [JSON Schema](https://github.com/shouweikun/gaebolg/blob/master/src/main/scala/com/neighborhood/aka/laplace/gaebolg/schema/JsonSchema.scala):
  
  1. ArraySchema   ->  JArray
  2. ObjectSchema  ->  JObject
  3. StringSchema  ->  JString
  4. NumberSchema  ->  JDouble/JDecimal
  5. IntegerSchema ->  JInt/JLong
  6. BooleanSchema ->  JBool
  7. ZeroSchema    ->  JNothing
  8. NullSchema    ->  JNull
  9. ProductSchema ->  [*]
  
  > productSchema is a special case, it is for multiple schemas in the same position(always happen in merge schema)
  
### QuickStart
//todo 

### Use Case
//todo




### Acknowledgments

  - First of all, really appreciate [schema-guru](https://github.com/snowplow/schema-guru), which inspiring gaebolg a lot
  
  - thanks 4 wenjin's advice on writing format
 
 - 首先，非常感谢[schema-guru](https://github.com/snowplow/schema-guru)，受了很多启发
 - 感谢文瑾的英文书写建议