package com.neighborhood.aka.laplace.gaebolg.flatten


import com.neighborhood.aka.laplace.gaebolg.flatten.ExecutableTableNode.TableFieldInfo
import org.json4s

/**
  * Created by john_liu on 2018/10/30.
  *
  * @since 2018-11-01
  */
/**
  * flatten表的一对一抽象
  *
  * @param tableName                   表的名称
  * @param ctf                         当前表的字段
  * @param deps                        表依赖的表
  * @param isArrayNested               是否是Array嵌套型的表
  * @param path                        路径
  * @param originalFieldNamesWhenPutIn 原生当前层字段都有什么，包括终结和非终结的  Specially made for szm
  */
case class ExecutableTableNode(
                                tableName: String,
                                private val ctf: List[TableFieldInfo],
                                deps: List[ExecutableTableNode] = List.empty,
                                var isArrayNested: Boolean = false,
                                private val path: List[String] = List.empty,
                                private val originalFieldNamesWhenPutIn: List[String] = List.empty
                             ) {

  if (deps.nonEmpty && deps.head.isArrayNested) isArrayNested = true //todo 这样做不好
  private lazy val tablePath_ = {
    val ph = if (path.isEmpty) {
      if (currentTableFields.isEmpty) List.empty
      else currentTableFields.head.path
    } else path
    ExecutableTableNode.parseToRealPath(ph)
  }
  private lazy val depth_ : Int = if (deps.isEmpty) 1 else deps.map(_.depth).max + 1
  private lazy val depTableFields_ = deps.flatMap(_.allTableFieldInfo)
  private lazy val allTableFieldInfo_ : List[TableFieldInfo] = {
    currentTableFields_ ::: depTableFields_
  }
  private lazy val currentTableFields_ = ctf.map(_.copy(tableIsArrayNested = isArrayNested))
  private lazy val originalFieldNames_ = if (originalFieldNamesWhenPutIn.nonEmpty) originalFieldNamesWhenPutIn else currentTableFields.map(_.fieldName)

  /**
    * special made 4 szm
    *
    * @return
    */
  def originalFieldNames: List[String] = originalFieldNames_

  /**
    * 表的所有字段
    * currentFields + dep的allTableFieldInfo
    *
    * @return
    */
  def allTableFieldInfo: List[TableFieldInfo] = allTableFieldInfo_

  /**
    * 表的深度(解json的深度)
    *
    * @return
    */
  def depth: Int = depth_

  /**
    * 表路径
    *
    * @return
    */
  def tablePath: List[String] = tablePath_

  /**
    * 依赖表的字段
    *
    * @return
    */
  def depTableFields: List[TableFieldInfo] = depTableFields_

  /**
    * 当前表字段
    *
    * @return
    */
  def currentTableFields: List[TableFieldInfo] = currentTableFields_

}

object ExecutableTableNode {

  /**
    * 字段信息的case class
    *
    * @param fieldName          字段名称
    * @param path               路径
    * @param dataType           字段的类型，详细参考FlattenJsonSchemaType
    * @param tableIsArrayNested 这个字段对应的表是否是arrayNested
    */
  case class TableFieldInfo(fieldName: String, path: List[String], dataType: FlattenJsonSchemaType, tableIsArrayNested: Boolean = false) {

    /**
      * 字段的全名 path + name
      *
      * @return
      */
    def fullName: String = fullName_

    /**
      * 全路径，寻址时使用
      *
      * @return
      */
    def fullPath: List[String] = fullPath_

    private lazy val fullPath_ : List[String] = path ::: List(fieldName)
    private lazy val fullName_ = s"${path.mkString("\\.")}.$fieldName"
  }

  case class TableSingleRow(tableName: String, fields: Map[String, AnyRef])

  def getData(jsonValue: json4s.JValue, tableNode: ExecutableTableNode): TableSingleRow = {
    ???
  }

  def getTableFieldInfo(tableNode: ExecutableTableNode): List[TableFieldInfo] = tableNode.allTableFieldInfo

  @inline
  def parseToRealPath(path: List[String]): List[String] = path.filter(s => s != SettingConstant.OBJECT_SCHEMA_PROPERTY_NAME && s != SettingConstant.ARRAY_SCHEMA_ITEMS_NAME)


  /**
    * 合并两个表
    *
    * @param node1
    * @param node2
    * @param tableName
    * @return
    */
  def mergeTableNodePart(node1: ExecutableTableNode, node2: ExecutableTableNode, tableName: Option[String]): ExecutableTableNode = {
    val fields = (node1.currentTableFields ::: node2.currentTableFields).distinct
    val deps = node1.deps ::: node2.deps
    val theTableName = tableName.getOrElse(s"${node1}_merge_$node2")
    ExecutableTableNode(theTableName, fields, deps)
  }
}