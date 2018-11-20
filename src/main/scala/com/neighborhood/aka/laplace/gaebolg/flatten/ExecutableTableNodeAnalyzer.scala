package com.neighborhood.aka.laplace.gaebolg.flatten

import com.neighborhood.aka.laplace.gaebolg.flatten.ExecutableTableNode.TableFieldInfo
import com.neighborhood.aka.laplace.gaebolg.flatten.FlattenJsonSchemaType.FlattenArraySchema
import com.neighborhood.aka.laplace.gaebolg.schema.types._
import com.finup.daas.jsonschema.schema.JsonSchema
import com.neighborhood.aka.laplace.gaebolg.JsonSchemaUtils
import com.neighborhood.aka.laplace.gaebolg.exception.InvalidJsonSchemaTypeException
import com.neighborhood.aka.laplace.gaebolg.schema.{Helpers, JsonSchema}

import scala.annotation.tailrec
import scala.collection.mutable

/**
  * Created by john_liu on 2018/10/30.
  *
  * 处理JsonSchema生成List[ExcutableTableNode]
  * 试图去解决复杂JsonSchema的flatten问题
  * 将复杂JsonSchema的蕴含的信息提取和封装
  *
  * 在阅读这段代码之前，让我们来达成几个共识
  * 1. JsonSchema的定义和分类 详情见com.finup.daas.jsonschema.schema.JsonSchema
  * 2. JsonSchema中分为两种,终结类型和非终结类型，详细定义参考方法this.isNotTerminal
  * 3. JsonSchema是非终结的,需要递归处理，直至非终结
  * 4. JsonSchema经过处理后，最终产物的List[ExcutableTableNode]
  * 5. ExcutableTableNode之间的关系:child ExcutableTableNode 会持有 parent ExcutableTableNode
  * 6. ExcutableTableNode与实际需要flatten的表是1:1的关系
  * 7. ExcutableTableNode存储了需要flatten所需的信息,详情见com.finup.daas.jsonschema.flatten.ExcutableTableNode
  *
  * @author nbhd.aka.laplace
  * @version 0.2.1
  * @since 2018-11-14
  * @note
  */
object ExecutableTableNodeAnalyzer {
  private lazy val stubStringSchema = StringSchema()(Helpers.SchemaContext(0))

  /**
    * 将一系列ExcutableTableNode flatten化
    *
    * @param tableNode 待处理的ExcutableTableNode
    * @param acc       中间结果累积
    * @return
    */
  def flattenExcutableTableNode(tableNode: ExecutableTableNode, acc: => List[ExecutableTableNode]): List[ExecutableTableNode] = {
    lazy val nextAcc = tableNode :: acc
    if (tableNode.deps.isEmpty) nextAcc.distinct
    else tableNode.deps.flatMap(node => flattenExcutableTableNode(node, nextAcc))
  }

  /**
    * 使用stack进行table展开
    * 自顶向下顺序
    *
    * @param leafTableNodes 叶子节点
    * @return 自顶向下顺序的TableNode
    */
  def flattenExcutableTableNodeByStack(leafTableNodes: List[ExecutableTableNode]): List[ExecutableTableNode] = {
    if (leafTableNodes.isEmpty) return List.empty;
    val stack = new mutable.Stack[ExecutableTableNode]

    @tailrec
    @inline
    def loopPushStack(tableNodes: => List[ExecutableTableNode]): Unit = {
      if (tableNodes.isEmpty) return;
      lazy val nextAcc = tableNodes.flatMap(_.deps)
      tableNodes.foreach(elem => if (!stack.contains(elem)) stack.push(elem))
      loopPushStack(nextAcc)
    }

    loopPushStack(leafTableNodes)
    stack.toList.distinct
  }

  /**
    * 自顶向下执行处理TableNode
    *
    * @param leafTableNodes
    * @param f 执行方法
    * @tparam R 结果类型
    * @return
    */
  def ExecuteTopDown[R](leafTableNodes: List[ExecutableTableNode])(f: ExecutableTableNode => R): List[R] = {
    flattenExcutableTableNodeByStack(leafTableNodes).map(x => f(x))
  }

  /**
    * 生成ExcutableTableNode的入口方法
    *
    * @param jsonSchema                   待处理的jsonSchema
    * @param dep                          依赖的ExcutableNodes
    * @param name                         名称
    * @param path                         路径
    * @param tableNameIfTerminalTypePutIn 如果传入的是Terminal类型，需要提供这样的表名，否则抛异常
    * @param isArrayNested                这个表是否经过了ArrayNested
    * @param splitMark                    这次任务是否是split发起的，表名需要附加类型信息
    * @throws InvalidJsonSchemaTypeException
    * @return
    */
  @throws[InvalidJsonSchemaTypeException]
  def recuisivelyAnalyzeJsonSchemaAndGenerateExcutableTableNodes(
                                                                  jsonSchema: JsonSchema,
                                                                  dep: List[ExecutableTableNode] = List.empty,
                                                                  name: => String = "",
                                                                  path: => List[String] = List.empty,
                                                                  tableNameIfTerminalTypePutIn: => Option[String] = None,
                                                                  isArrayNested: Boolean = false,
                                                                  splitMark: Boolean = false
                                                                ): List[ExecutableTableNode] = {
    jsonSchema match {
      case objectSchema: ObjectSchema => handleObjectSchema(objectSchema, dep, name, path, isArrayNested, splitMark)
      case productSchema: ProductSchema => handleProductSchema(productSchema, dep, name, path, isArrayNested)
      case arraySchema: ArraySchema if (isNotTerminal(arraySchema)) => handleArrayNestNotTerminalSchema(arraySchema, dep, name, path)
      case _ => tableNameIfTerminalTypePutIn.fold(throw new InvalidJsonSchemaTypeException(s"cannot handle terminal json type:${jsonSchema.getType},flattenType:${FlattenJsonSchemaType.getFlattenJsonSchemaType(jsonSchema)}"))(tbName => handleSingleTerminalSchema(jsonSchema, name, path, dep, tbName))
    }
  }

  /**
    * 处理objectSchema类型
    *
    * @param objectSchema 待处理的objectSchema
    * @param dep          依赖的TableNode
    * @param name         当前表名
    * @param path         json路径
    * @param splitMark    split标识，是否需要在表上携带type
    * @return
    */
  @inline
  private def handleObjectSchema(objectSchema: ObjectSchema, dep: List[ExecutableTableNode] = List.empty, name: String, path: List[String] = List.empty, isArrayNested: Boolean = false, splitMark: Boolean = false): List[ExecutableTableNode] = {
    val properties = objectSchema.properties
    lazy val terminals = properties.filter { case (_, js) => !isNotTerminal(js) }
    lazy val nonTerminal = properties.withFilter { case (_, js) => isNotTerminal(js) }
    lazy val thePath = path ::: List(name, SettingConstant.OBJECT_SCHEMA_PROPERTY_NAME)
    lazy val typeInfo: Option[String] = getTypeInfo(objectSchema, splitMark)
    val tableName = generateTableName(name, thePath, typeInfo) //注意是传的thePath,rather than path
    lazy val originalFieldNames: List[String] = properties.map(_._1).toList
    lazy val currentTableNode = handleTerminalSchemas(terminals, dep, tableName, thePath, isArrayNested).map(_.copy(originalFieldNamesWhenPutIn = originalFieldNames)) //2018-11-13
    if (terminals.size == properties.size) currentTableNode //如果全是终结型,直接返回currentTableNode
    else nonTerminal.flatMap {
      case (key, js) =>
        val thisJsonSchema = js
        val thisDep = currentTableNode
        val thisName = key
        val thisPath = thePath
        recuisivelyAnalyzeJsonSchemaAndGenerateExcutableTableNodes(thisJsonSchema, thisDep, thisName, thisPath)
    }.toList
  }

  /**
    * 处理终结类型的JsonSchema
    *
    * @param properties (fieldName ->待处理的JsonSchema)
    * @param dep        依赖的TableNode
    * @param tableName  表名称
    * @param path       json路径
    * @return
    */
  @inline
  private def handleTerminalSchemas(properties: Map[String, JsonSchema], dep: List[ExecutableTableNode] = List.empty, tableName: String, path: List[String] = List.empty, isArrayNested: Boolean = false): List[ExecutableTableNode] = {
    if (properties.isEmpty) return List.empty
    val fields = properties.map { case (key, js) => generateTableFieldInfoBySingleTerminalSchema(js, key, path) }.toList
    List(ExecutableTableNode(tableName, fields, dep, isArrayNested, path))
  }

  /**
    * 处理单个终结类型成为表
    * (当然表只有一个字段)
    *
    * @param jsonSchema 待处理的JsonSchema
    * @param name       字段名称
    * @param path       路径
    * @param tableName  表名称 ps:这项一定是外界提供的
    * @return
    */
  @inline
  private def handleSingleTerminalSchema(jsonSchema: JsonSchema, name: String, path: List[String], dep: List[ExecutableTableNode], tableName: String): List[ExecutableTableNode] = {
    val tableFieldInfo = List(generateTableFieldInfoBySingleTerminalSchema(jsonSchema, name, path))
    List(ExecutableTableNode(tableName, tableFieldInfo, dep, path = path))
  }

  /**
    * 处理单个终结JsonSchema,转换成tableFieldInfo
    *
    * 对于ProductSchema(terminal1,terminal2...terminalX)的情况，强行StringSchema  2018-11-14
    *
    * @param jsonSchema 待处理的jsonSchema
    * @param name       名称
    * @param path       路径
    * @return
    */
  @inline
  private def generateTableFieldInfoBySingleTerminalSchema(jsonSchema: JsonSchema, name: String, path: List[String]): TableFieldInfo = {
    val js = if (jsonSchema.isInstanceOf[ProductSchema]) {
      jsonSchema.asInstanceOf[ProductSchema].types.filter(!_.isInstanceOf[NullSchema]) match {
        case hd :: Nil => hd //让ProductSchema(nullSchema,xSchema)的xSchema提取出来
        case Nil => throw new RuntimeException("this cannot be happen!")
        case _ => stubStringSchema //如果是多个非空终结型，强行String
      }
    } else jsonSchema
    val (_, finalKey, finalPath, flattenJsonSchemaTypeOption) = deStructNestFromArraySchema(js, name, path, needGetFlattenArraySchema = true) //对schema进行去ArraySchema
    TableFieldInfo(finalKey, finalPath, flattenJsonSchemaTypeOption.get)
  }

  /**
    * 处理productSchema类型
    *
    * @param productSchema 待处理的productSchema
    * @param dep           依赖的TableNode
    * @param name          当前表名
    * @param path          json路径
    * @return
    */
  @inline
  private def handleProductSchema(productSchema: ProductSchema, dep: List[ExecutableTableNode] = List.empty, name: String, path: List[String] = List.empty, isArrayNested: Boolean = false): List[ExecutableTableNode] = {

    lazy val allMatterSchemas = productSchema.types.filter(js => !js.isInstanceOf[NullSchema]) //不为空
    lazy val terminalSchema = allMatterSchemas.filter(js => !this.isNotTerminal(js)) match {
      case Nil => List.empty
      case hd :: Nil => List(hd)
      case _ => List(stubStringSchema)
    }
    lazy val nonTerminalSchemas = allMatterSchemas.filter(js => this.isNotTerminal(js))
    (nonTerminalSchemas ::: terminalSchema)
      .flatMap {
        js =>
          val typeInfo = getTypeInfo(js, true)
          val tableName = Option(generateTableName(name, path, typeInfo))
          recuisivelyAnalyzeJsonSchemaAndGenerateExcutableTableNodes(js, dep, name, path, tableName, isArrayNested, splitMark = true)
      }
  }

  /**
    * 处理嵌套非终结类型的ArraySchema
    *
    * @param arraySchema 待处理的arraySchema
    * @param dep         依赖的表
    * @param name        名称
    * @param path        路径
    * @param splitMark   split标识，是否需要在表上携带type
    * @return
    */
  @inline
  private def handleArrayNestNotTerminalSchema(arraySchema: ArraySchema, dep: List[ExecutableTableNode], name: String, path: List[String] = List.empty, splitMark: Boolean = false): List[ExecutableTableNode] = {

    val (js, finalName, finalPath, _) = deStructNestFromArraySchema(arraySchema, name, path, needGetFlattenArraySchema = false) //不需要获得flattenSchema
    recuisivelyAnalyzeJsonSchemaAndGenerateExcutableTableNodes(js, dep, finalName, finalPath, isArrayNested = true, splitMark = splitMark)
  }

  /**
    * 解构ArraySchema的嵌套
    * 如果不是ArraySchema,直接返回
    *
    * @todo 对于长嵌套，目前名称肯定是SettingConstant.ARRAY_SCHEMA_ITEMS_NAME，之后改善它
    * @param schema                    待处理解嵌套的Schema
    * @param name                      名称
    * @param path                      路径
    * @param needGetFlattenArraySchema 是否需要获得FlattenArraySchema
    * @return
    */
  @inline
  @tailrec
  private def deStructNestFromArraySchema(schema: JsonSchema, name: String, path: List[String] = List.empty, nestRate: Int = 0, needGetFlattenArraySchema: Boolean = true): (JsonSchema, String, List[String], Option[FlattenJsonSchemaType]) = {

    /**
      * 获取嵌套的FlattenArraySchema
      *
      * @param nestRate
      * @return
      */
    @inline
    def generateFlattenArraySchema(nestRate: Int): FlattenJsonSchemaType = {
      if (nestRate == 0) FlattenJsonSchemaType.getFlattenJsonSchemaType(schema)
      else FlattenArraySchema(generateFlattenArraySchema(nestRate - 1))
    }

    lazy val theFlattenJsonSchemaType = if (needGetFlattenArraySchema) generateFlattenArraySchema(nestRate) else null
    if (!schema.isInstanceOf[ArraySchema]) return (schema, name, path, Option(theFlattenJsonSchemaType))
    //直接返回 递归栈顶
    val arraySchema = schema.asInstanceOf[ArraySchema]
    val theName = SettingConstant.ARRAY_SCHEMA_ITEMS_NAME //固定
    val thePath = path ::: List(name)
    deStructNestFromArraySchema(arraySchema.items, theName, thePath, nestRate + 1, needGetFlattenArraySchema)
  }

  /**
    * 生成tableName
    * 规则:path.name_type
    *
    * @param name    名称
    * @param path    路径
    * @param theType 类型(可选)
    * @return 生成的表名
    */
  @inline
  private def generateTableName(name: String, path: List[String], theType: Option[String] = None): String =
  s"${path.mkString(".")}.$name${theType.map(tpe => s"_$tpe").getOrElse("")}"

  /**
    * 判断是否是终结类型
    * 非终结类型包括
    * 1.objectSchema
    * 2.productSchema 并且对于其内含的schema，存在非终结，即为非终结
    * 3.ArraySchema(ArraySchema(objectSchema || productSchema))
    * |       1 to n        |
    *
    * @param jsonSchema
    * @return
    */
  @inline
  private def isNotTerminal(jsonSchema: JsonSchema): Boolean = jsonSchema match {
    case _: ObjectSchema => true
    case productSchema: ProductSchema if (productSchema.types.exists(isNotTerminal(_))) => true
    case ArraySchema(content) => isNotTerminal(content)
    case _ => false
  }

  /**
    * 是否是需要发起合并的类型
    * 根据11.01号开会讨论结果，暂时不支持merge
    *
    * @param jsonSchema
    * @return
    */
  @inline
  @tailrec
  private def isNeedMerge(jsonSchema: JsonSchema): Boolean = jsonSchema match {
    case ArraySchema(content) => isNeedMerge(content)
    case _ => false
  }

  /**
    * 是否需要发起分隔的类型
    * 根据11.01号开会讨论结果，暂时所有的non-terminal类型都需要split
    *
    * @param jsonSchema
    * @return
    */
  @inline
  @tailrec
  private def isNeedSplit(jsonSchema: JsonSchema): Boolean = jsonSchema match {
    case _: ProductSchema => true
    case objectSchema: ObjectSchema => true
    case ArraySchema(content) => isNeedSplit(content)
    case _ => false
  }

  /**
    * 获取类型名称
    * 主要是配合splitMark使用
    *
    * @param jsonSchema 待处理的JsonSchema
    * @param splitMark  split标识
    * @return
    */
  @inline
  private def getTypeInfo(jsonSchema: JsonSchema, splitMark: Boolean): Option[String] = if (splitMark) Option(jsonSchema.getType.head) else None

  def main(args: Array[String]): Unit = {
    import org.json4s._
    import org.json4s.jackson.JsonMethods._
    import org.json4s.jackson.Serialization
    import org.json4s.jackson.Serialization.{read, write}
    implicit val format = DefaultFormats
    val json1 = ("""{"a":1,"b":{"b1":[[{"bb1":1,"bb2":[[[2]]]},{"bb2":1,"bb3":2}]],"b3":2,"b4":{"bb41":1}},"c":[[[1]]],"d":[[[[[{"d1":"fuck"}]]]]],"e":["1,ssd"]}""")
    val json1schema = JsonSchemaUtils.getJsonSchema(json1).get
    val json2 = ("""{"a":1,"b":{"b1":{"bb2":1,"bb3":2},"b3":2,"b4":{"bb41":1}}}""")
    val json2schema = JsonSchemaUtils.getJsonSchema(json2).get
    val mergeJson = JsonSchemaUtils.mergeSchema(json1schema, json2schema)
    println(pretty(mergeJson.toJson))
    val a = recuisivelyAnalyzeJsonSchemaAndGenerateExcutableTableNodes(json1schema, name = "temp")

    println(pretty(parse(write(a))))
  }
}



