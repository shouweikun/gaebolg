package com.neighborhood.aka.laplace.gaebolg.generators

import java.util.UUID

import com.neighborhood.aka.laplace.gaebolg.generators.SchemaGeneratorNonRec.FieldKey

import scalaz._
import Scalaz._
import com.neighborhood.aka.laplace.gaebolg.schema.Helpers._
import com.neighborhood.aka.laplace.gaebolg.schema.types._
import com.neighborhood.aka.laplace.gaebolg.exception.InvalidJsonException
import com.neighborhood.aka.laplace.gaebolg.schema.JsonSchema
import org.apache.commons.validator.routines.{InetAddressValidator, UrlValidator}
import org.joda.time.DateTime
import org.json4s.JsonAST._
import org.json4s.{JDouble, JInt, JNothing, JNull, JString, JValue}

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by john_liu on 2018/11/8.
  *
  * 由于递归的SchemaGenerator会导致StackOverflow
  * 所以采取非递归的方式进行Schema的生成
  * 将Json数据处理成JsonSchema
  * 朴素思路是使用stack进行处理，避免爆栈
  * 分为两大部分
  * 1.非终结JValue的展平与入栈
  * 2.简单JsonSchema合并为复杂Schema
  *
  * @param context SchemaContext,用于一些特殊设置
  * @author nbhd.aka.laplace
  * @since 2018-11-09
  * @version 0.1.0
  */
class SchemaGeneratorNonRec(implicit val context: SchemaContext) extends Serializable {
  private lazy val zeroSchema = ZeroSchema() //幺元
  private lazy val nullArraySchemaStub = ArraySchema(null) //arraySchema占位符
  private lazy val emptyObjectSchema = ObjectSchema() //objectSchema占位符
  implicit private val md = getMonoid(context) //monoid,用于JsonSchema操作

  /**
    * 将数据Json转化为JsonSchema的入口方法
    * 在本方法中仅做一些数据的校验
    *
    * @param json 数据Json
    * @return 转换生成的JsonSchema
    */
  def Json2Schema(json: JValue): Option[JsonSchema] = {
    if (json == null) return None;
    if (!(json.isInstanceOf[JObject] || json.isInstanceOf[JArray])) throw new InvalidJsonException("not a invaild json when Json2Schema，should be JObject or JArray");
    implicit lazy val stack = new mutable.Stack[(FieldKey, Either[JValue, JsonSchema])]
    Option(Json2SchemaInternal(json))
  }

  /**
    * 生成 JsonSchema 的方法
    * 核心逻辑见loopParseJsonSchema
    *
    * @param json  待处理的JValue
    * @param stack 装临时数据的栈
    * @return 生成好的JsonSchema
    */
  @inline
  private def Json2SchemaInternal(json: JValue)(implicit stack: mutable.Stack[(FieldKey, Either[JValue, JsonSchema])]): JsonSchema = {
    lazy val uuid = getUuid
    json match {
      case obj: JObject if (json.children.nonEmpty) => stack.push((FieldKey(uuid), Left(obj)))
      case arr: JArray if (arr.children.nonEmpty) => stack.push((FieldKey(uuid), Left(arr)))
      case _: JObject => return ObjectSchema()
      case arr: JArray => return ArraySchema(zeroSchema)


    }
    val (name, js) = loopParseJsonSchema
    if (name != uuid) throw new RuntimeException("parse failed")
    else js
  }

  /**
    * 尾递归处理解析生成JsonSchema
    * 观察一下Stack的类型签名可知
    * Either[JValue,JsonSchema]
    * JValue属于处于第一阶段 需要解析后入栈
    * JsonSchema属于第二阶段 语法解析器 进行Schema合成
    *
    * @param stack 装临时数据的栈
    * @return 生成的JsonSchema和key
    */
  @tailrec
  @inline
  private final def loopParseJsonSchema(implicit stack: mutable.Stack[(FieldKey, Either[JValue, JsonSchema])]): (String, JsonSchema) = {
    if (stack.isEmpty) throw new RuntimeException("cannot be null when loopParseJsonSchema ")
    if (stack.top._2.isLeft) { //是个JValue,进行第一阶段
      lazy val (name, left) = stack.pop() //出栈
    lazy val v = left.asInstanceOf[scala.util.Left[JValue, JsonSchema]].a // 取值 todo 这块设计的不好
      putNonTerminalJValueIntoStack(name, v) //放入栈中
      loopParseJsonSchema //继续迭代
    }
    else { //是个JsonSchema,进行第二阶段,需要进行语法解析
      if (stack.length == 1) { //栈中数为1，且是JsonSchema,说明生成好了 是方法出口
        val (name, either) = stack.pop(); //出栈
        return (name.key, either.asInstanceOf[scala.util.Right[JValue, JsonSchema]].b)
      }
      val popupSchemaLexical = popWhile[(FieldKey, Either[JValue, JsonSchema])](x => isSimpleLexical(x._2)) //获得朴素语素
      val (key, actionLexical) = stack.pop() //出栈
      actionLexical match {
        case Left(v: JValue) => pushList[(FieldKey, Either[JValue, JsonSchema])](popupSchemaLexical); putNonTerminalJValueIntoStack(key, v) //如果还是JValue，回到第一步
        case Right(schema) => {
          popupSchemaLexical.withFilter { case (name, _) => name.key == key }.foreach(stack.push(_)) //如果发现名称相同，说明是同一级的，不应该被处理，所以入栈
          val theChosenLexical = popupSchemaLexical.withFilter { case (ky, _) => ky.key != key.key }
          val popupSchemaLexicalKeyValue = theChosenLexical.map { case (name, either) => (name.key, either.asInstanceOf[scala.util.Right[JValue, JsonSchema]].b) }
          val popupSchemaLexicalValue = theChosenLexical.map { case (_, either) => either.asInstanceOf[scala.util.Right[JValue, JsonSchema]].b }

          schema match {
            case `nullArraySchemaStub` => stack.push((key, Right(ArraySchema(popupSchemaLexicalValue.suml)))) //array合并
            case `emptyObjectSchema` => stack.push((key, Right(ObjectSchema(popupSchemaLexicalKeyValue.toMap)))) //object合并
          }
        }
      }
      loopParseJsonSchema //继续递归

    }

  }

  /**
    * 是否是朴素语素
    * 类似于终结字符，不能再展开，只能等待被合并
    *
    * @param x 待判断的语素
    * @return true if is simple lexical
    */
  private def isSimpleLexical(x: Either[JValue, JsonSchema]): Boolean = x match {
    case Right(schema) if (schema != nullArraySchemaStub && schema != emptyObjectSchema) => true
    case _ => false
  }

  /**
    * 将JObject放入栈中
    *
    * @param name  名称
    * @param json  待放入的JObject
    * @param stack 栈
    */
  private def putObjectJValueIntoStack(name: FieldKey, json: JObject)(implicit stack: mutable.Stack[(FieldKey, Either[JValue, JsonSchema])]): Unit = {
    stack.push((name, Right(emptyObjectSchema)))
    json.obj.foreach {
      case (n, field) => field match {
        case _: JObject => stack.push((FieldKey(n), Left(field)))
        case _: JArray => stack.push((FieldKey(n), Left(field)))
        case _ => putTerminalJValueIntoStack(FieldKey(n), field)
      }
    }
  }

  /**
    * 处理JArray放入栈中
    *
    * @param name  名称
    * @param json  待放入的JArray
    * @param stack 栈
    */
  private def putArrayJValueIntoStack(name: => FieldKey, json: JArray)(implicit stack: mutable.Stack[(FieldKey, Either[JValue, JsonSchema])]): Unit = {
    def pushStub = stack.push((name, Right(nullArraySchemaStub)))

    val arr = json.arr
    lazy val uuid = getUuid
    arr match {
      case x :: _ => x match {
        case _: JObject => {
          pushStub;
          arr.foreach(obj => putObjectJValueIntoStack(FieldKey(uuid, true), obj.asInstanceOf[JObject]))
        }
        case _: JArray => pushStub; arr.foreach(obj => stack.push((FieldKey(uuid, true), Left(obj.asInstanceOf[JArray])))) //特别的，对于JArray(JArray)我们需要先只是投入stack，之后会在处理合并的时候自动处理
        case _ => stack.push((name, Right(ArraySchema(buildTerminalJsonSchema(x)))))
      }
      case Nil => stack.push((name, Right(ArraySchema(zeroSchema))))
    }
  }

  /**
    * 处理非终结型的JValue的入口方法
    *
    * @param name  名称
    * @param json  待处理的JValue
    * @param stack 栈
    */
  private def putNonTerminalJValueIntoStack(name: => FieldKey, json: => JValue)(implicit stack: mutable.Stack[(FieldKey, Either[JValue, JsonSchema])]): Unit = {
    json match {
      case obj: JObject => putObjectJValueIntoStack(name, obj)
      case arr: JArray => putArrayJValueIntoStack(name, arr)
    }
  }

  /**
    * 处理终结型的JValue的入口方法
    *
    * @param name  名称
    * @param json  待处理的JValue
    * @param stack 栈
    */
  private def putTerminalJValueIntoStack(name: => FieldKey, json: JValue)(implicit stack: mutable.Stack[(FieldKey, Either[JValue, JsonSchema])]): Unit = {
    lazy val jsonSchema = buildTerminalJsonSchema(json)
    stack.push((name, Right(jsonSchema))
    )
  }

  /**
    * 终结型的JValue -> JsonSchema
    *
    * @param json 待处理的JValue
    * @return
    */
  private def buildTerminalJsonSchema(json: JValue): JsonSchema = json match {
    case JString(v) => Annotations.annotateString(v)
    case JInt(v) => Annotations.annotateInteger(v)
    case JDecimal(v) => Annotations.annotateNumber(v)
    case JDouble(v) => Annotations.annotateNumber(v)
    case JBool(_) => BooleanSchema()
    case JNull => NullSchema()
    case JNothing => NullSchema()
  }

  /**
    * 辅助方法,在不满足条件之前一直出栈
    *
    * @param f     弹出条件
    * @param stack 栈
    * @tparam A 泛型
    * @return 出栈的元素
    */
  @inline
  private def popWhile[A](f: A => Boolean)(implicit stack: mutable.Stack[A]): List[A] = {
    //使用可变效率高
    val lb = new ListBuffer[A]()
    while (stack.nonEmpty && f(stack.top)) {
      lb.append(stack.pop())
    }
    lb.toList
  }

  /**
    * 辅助方法
    * 向一个Stack push一个list的元素
    *
    * @param list  待放入的List
    * @param stack 栈
    * @tparam A 泛型
    */
  @inline
  private def pushList[A](list: List[A])(implicit stack: mutable.Stack[A]): Unit = if (list.nonEmpty) list.foreach(stack.push(_))

  /**
    * 辅助方法 获取UUID
    *
    * @return UUID
    */
  @inline
  private def getUuid: String = UUID.randomUUID().toString

  /**
    * 处理"终结类型"
    * Annotations are properties of schema derived from one value
    * eg. "127.0.0.1" -> {maxLength: 9, format: ipv4, enum: ["127.0.0.1"]}
    * 33          -> {minimum: 33, maximum: 33, enum: [33]}
    */
  object Annotations {
    def suggestTimeFormat(string: String): Option[String] = {
      if (string.length > 10) { // TODO: find a better way to exclude truncated ISO 8601:2000 values
        try {
          DateTime.parse(string)
          Some("date-time")
        } catch {
          case e: IllegalArgumentException => None
        }
      } else None
    }

    def suggestUuidFormat(string: String): Option[String] = {
      try {
        UUID.fromString(string)
        Some("uuid")
      } catch {
        case e: IllegalArgumentException => None
      }
    }

    def suggestIpFormat(string: String): Option[String] = {
      val validator = new InetAddressValidator()
      if (validator.isValidInet4Address(string)) {
        Some("ipv4")
      }
      else if (validator.isValidInet6Address(string)) {
        Some("ipv6")
      }
      else {
        None
      }
    }

    def suggestUrlFormat(string: String): Option[String] = {
      val urlValidator = new UrlValidator()
      if (urlValidator.isValid(string)) {
        Some("uri")
      }
      else {
        None
      }
    }

    def suggestBase64Pattern(string: String): Option[String] = {
      context.quantity match {
        case Some(quantity) if quantity < 10 && string.length < 32 => None // don't apply suggestion on small instance set
        case _ => {
          val regex = "^([A-Za-z0-9+/]{4})*([A-Za-z0-9+/]{4}|[A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{2}==)$"
          if (string.matches(regex)) {
            Some(regex)
          }
          else None
        }
      }
    }

    private val formatSuggestions = List(suggestUuidFormat _, suggestTimeFormat _, suggestIpFormat _, suggestUrlFormat _)
    private val patternSuggestions = List(suggestBase64Pattern _)

    /**
      * Tries to guess format or pattern of the string
      * If nothing match return "none" format which must be reduced in further transformations
      *
      * @param value       is a string we need to recognize
      * @param suggestions list of functions can recognize format
      * @return some format or none if nothing suites
      */
    @tailrec
    def suggestAnnotation(value: String, suggestions: List[String => Option[String]]): Option[String] = {
      suggestions match {
        case Nil => None
        case suggestion :: tail => suggestion(value) match {
          case Some(format) => Some(format)
          case None => suggestAnnotation(value, tail)
        }
      }
    }

    /**
      * Construct enum only if it is in predefined enum set or enum
      * cardinality > 0
      *
      * @param enumValue JSON value
      * @return same value wrapped in Option
      */
    def constructEnum(enumValue: JValue): Option[List[JValue]] = {
      lazy val inEnum: Boolean = context.inOneOfEnums(enumValue)
      if (context.enumCardinality == 0 && context.enumSets.isEmpty) {
        None
      } else if (context.enumCardinality > 0 || inEnum) {
        Some(List(enumValue))
      } else {
        None
      }
    }

    // TODO: consider one method name with overloaded argument types
    /**
      * Adds properties to string field
      */
    def annotateString(value: String): StringSchema = {
      StringSchema(
        None,
        None,
        minLength = if (context.deriveLength) Some(value.length) else None,
        maxLength = if (context.deriveLength) Some(value.length) else None,
        None
      )
    }

    /**
      * Set value itself as minimum and maximum for future merge and reduce
      * Add itself to enum array
      */
    def annotateInteger(value: BigInt) =
      IntegerSchema(value.some, value.some, constructEnum(JInt(value)))

    /**
      * Set value itself as minimum. We haven't maximum bounds for numbers
      * Add itself to enum array
      */
    def annotateNumber(value: BigDecimal) =
      NumberSchema(value.toDouble.some, value.toDouble.some, constructEnum(JDouble(value.toDouble)))

    /**
      * Set value itself as minimum. We haven't maximum bounds for numbers
      * Add itself to enum array
      */
    def annotateNumber(value: Double) =
      NumberSchema(value.some, value.some, constructEnum(JDouble(value)))
  }

}

object SchemaGeneratorNonRec {
  def apply(implicit context: SchemaContext): SchemaGeneratorNonRec = new SchemaGeneratorNonRec

  case class FieldKey(key: String, isFromArray: Boolean = false)


}