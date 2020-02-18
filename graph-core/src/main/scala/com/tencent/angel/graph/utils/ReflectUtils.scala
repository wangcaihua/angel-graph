package com.tencent.angel.graph.utils

import java.util

import com.tencent.angel.graph.core.data._
import com.tencent.angel.ml.math2.vector._
import it.unimi.dsi.fastutil.ints._
import it.unimi.dsi.fastutil.longs._

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox

object ReflectUtils {
  private val mirror = runtimeMirror(ReflectUtils.getClass.getClassLoader)
  private val tb = mirror.mkToolBox()

  private val typeCache = new util.HashMap[String, Type]()

  private val SIMPLE = "([0-9.a-zA-Z]+)".r
  private val ARRAY = "Array\\[([0-9.a-zA-Z]+)]".r
  private val WITHTYPEPARAM = "([0-9.a-zA-Z]+)\\[([0-9.a-zA-Z]+)]".r
  private val WITHARRAYTYPEPARAM = "([0-9.a-zA-Z]+)\\[Array\\[([0-9.a-zA-Z]+)]]".r

  def newInstance(tpe: Type): Any = {
    val constructor = getMethod(tpe: Type, termNames.CONSTRUCTOR)
    val classMirror = mirror.reflectClass(tpe.typeSymbol.asClass)
    val methodMirror = classMirror.reflectConstructor(constructor)
    methodMirror()
  }

  def constructor(tpe: Type, argTypes: Type*): MethodMirror = {
    val constructor = getMethod(tpe, termNames.CONSTRUCTOR, argTypes: _*)

    val classMirror = mirror.reflectClass(tpe.typeSymbol.asClass)
    classMirror.reflectConstructor(constructor)
  }

  def instMirror(obj: Any): InstanceMirror = {
    if (obj != null) {
      mirror.reflect(obj)
    } else {
      throw new Exception("obj is null!")
    }
  }

  def field(obj: Any, field: TermSymbol): FieldMirror = {
    if (obj != null && field != null) {
      val inst = obj match {
        case inst: InstanceMirror => inst
        case o: Any => mirror.reflect(o)
      }

      inst.reflectField(field)
    } else {
      throw new Exception("inst or field or value is null !")
    }
  }

  def field(obj: Any, fieldName: String): FieldMirror = {
    if (obj != null) {
      val instMirror = obj match {
        case inst: InstanceMirror => inst
        case o: Any => mirror.reflect(o)
      }

      val field = instMirror.symbol.typeSignature.member(TermName(fieldName)).asTerm
      instMirror.reflectField(field)
    } else {
      throw new Exception("inst or field or value is null !")
    }
  }

  private def getMethod(tpe: Type, name: TermName, args: Type*): MethodSymbol = {
    tpe.member(name) match {
      case m: MethodSymbol => m
      case t: TermSymbol =>
        val alternatives = t.alternatives.filter(_.isMethod)
          .map(x => (x.asMethod, x.typeSignature)).collectFirst {
          case (ms, _: NullaryMethodType) if args.isEmpty => ms
          case (ms, MethodType(params, _)) if params.isEmpty && args.isEmpty => ms
          case (ms, MethodType(params, _)) if params.map(_.typeSignature) == args => ms
          case (ms, MethodType(params, _)) if filterTypeParams(params) == args => ms
          case (ms, PolyType(_, MethodType(params, _))) if filterTypeParams(params) == args => ms
        }
        if (alternatives.nonEmpty) {
          alternatives.get
        } else {
          throw new Exception(s"cannot find ${name.toString} !")
        }
      case _ =>
        throw new Exception(s"cannot find ${name.toString} !")
    }
  }

  private def filterTypeParams(params: List[Symbol]): List[Type] = {
    params.filter(ts => !ts.typeSignature.typeSymbol.toString.startsWith("type"))
      .map(_.typeSignature)
  }

  def method(obj: Any, name: String, args: Type*): MethodMirror = {
    if (obj != null) {
      val inst = obj match {
        case inst: InstanceMirror => inst
        case o: Any => mirror.reflect(o)
      }

      val tpe = inst.symbol.typeSignature
      val m = getMethod(tpe, TermName(name), args: _*)
      inst.reflectMethod(m)
    } else {
      throw new Exception("inst is null!")
    }
  }

  def getClassTag[T: ClassTag](obj: T): ClassTag[T] = implicitly[ClassTag[T]]

  def getReflectType(obj: Any): Type = {
    mirror.reflect(obj).symbol.toType
  }

  def getType[T: TypeTag](obj: T): Type = typeOf[T]

  def typeFromObject(obj: Any): Type = obj match {
    case _: Boolean => typeOf[Boolean]
    case _: Byte => typeOf[Byte]
    case _: Char => typeOf[Char]
    case _: Short => typeOf[Short]
    case _: Int => typeOf[Int]
    case _: Long => typeOf[Long]
    case _: Float => typeOf[Float]
    case _: Double => typeOf[Double]
    case _: String => typeOf[String]
    case _: NodeN => typeOf[NodeN]
    case _: NodeNW => typeOf[NodeNW]
    case _: NodeTN => typeOf[NodeTN]
    case _: NodeTNW => typeOf[NodeTNW]
    case _: IntDummyVector => typeOf[IntDummyVector]
    case _: IntIntVector => typeOf[IntIntVector]
    case _: IntLongVector => typeOf[IntLongVector]
    case _: IntFloatVector => typeOf[IntFloatVector]
    case _: IntDoubleVector => typeOf[IntDoubleVector]
    case _: LongDummyVector => typeOf[LongDummyVector]
    case _: LongIntVector => typeOf[LongIntVector]
    case _: LongLongVector => typeOf[LongLongVector]
    case _: LongFloatVector => typeOf[LongFloatVector]
    case _: LongDoubleVector => typeOf[LongDoubleVector]
    case _: Array[Boolean] => typeOf[Array[Boolean]]
    case _: Array[Byte] => typeOf[Array[Byte]]
    case _: Array[Char] => typeOf[Array[Char]]
    case _: Array[Short] => typeOf[Array[Short]]
    case _: Array[Int] => typeOf[Array[Int]]
    case _: Array[Long] => typeOf[Array[Long]]
    case _: Array[Float] => typeOf[Array[Float]]
    case _: Array[Double] => typeOf[Array[Double]]
    case _: Array[String] => typeOf[Array[String]]
    case array: Array[_] =>
      if (array.nonEmpty) {
        array.head match {
          case _: NodeN => typeOf[Array[NodeN]]
          case _: NodeNW => typeOf[Array[NodeNW]]
          case _: NodeTN => typeOf[Array[NodeTN]]
          case _: NodeTNW => typeOf[Array[NodeTNW]]
          case _: IntDummyVector => typeOf[Array[IntDummyVector]]
          case _: IntIntVector => typeOf[Array[IntIntVector]]
          case _: IntLongVector => typeOf[Array[IntLongVector]]
          case _: IntFloatVector => typeOf[Array[IntFloatVector]]
          case _: IntDoubleVector => typeOf[Array[IntDoubleVector]]
          case _: LongDummyVector => typeOf[Array[LongDummyVector]]
          case _: LongIntVector => typeOf[Array[LongIntVector]]
          case _: LongLongVector => typeOf[Array[LongLongVector]]
          case _: LongFloatVector => typeOf[Array[LongFloatVector]]
          case _: LongDoubleVector => typeOf[Array[LongDoubleVector]]
          case _ => getReflectType(obj)
        }
      } else {
        getReflectType(obj)
      }
    case _: Int2BooleanOpenHashMap => typeOf[Int2BooleanOpenHashMap]
    case _: Int2ByteOpenHashMap => typeOf[Int2ByteOpenHashMap]
    case _: Int2CharOpenHashMap => typeOf[Int2CharOpenHashMap]
    case _: Int2ShortOpenHashMap => typeOf[Int2ShortOpenHashMap]
    case _: Int2IntOpenHashMap => typeOf[Int2IntOpenHashMap]
    case _: Int2LongOpenHashMap => typeOf[Int2LongOpenHashMap]
    case _: Int2FloatOpenHashMap => typeOf[Int2FloatOpenHashMap]
    case _: Int2DoubleOpenHashMap => typeOf[Int2DoubleOpenHashMap]
    case _: Long2BooleanOpenHashMap => typeOf[Long2BooleanOpenHashMap]
    case _: Long2ByteOpenHashMap => typeOf[Long2ByteOpenHashMap]
    case _: Long2CharOpenHashMap => typeOf[Long2CharOpenHashMap]
    case _: Long2ShortOpenHashMap => typeOf[Long2ShortOpenHashMap]
    case _: Long2IntOpenHashMap => typeOf[Long2IntOpenHashMap]
    case _: Long2LongOpenHashMap => typeOf[Long2LongOpenHashMap]
    case _: Long2FloatOpenHashMap => typeOf[Long2FloatOpenHashMap]
    case _: Long2DoubleOpenHashMap => typeOf[Long2DoubleOpenHashMap]
    case map: Int2ObjectOpenHashMap[_] =>
      if (map.size() > 0) {
        val entry = map.int2ObjectEntrySet().fastIterator().next()
        entry.getValue match {
          case _: String => typeOf[Int2ObjectOpenHashMap[String]]
          case _: NodeN => typeOf[Int2ObjectOpenHashMap[NodeN]]
          case _: NodeNW => typeOf[Int2ObjectOpenHashMap[NodeNW]]
          case _: NodeTN => typeOf[Int2ObjectOpenHashMap[NodeTN]]
          case _: NodeTNW => typeOf[Int2ObjectOpenHashMap[NodeTNW]]
          case _: IntDummyVector => typeOf[Int2ObjectOpenHashMap[IntDummyVector]]
          case _: IntIntVector => typeOf[Int2ObjectOpenHashMap[IntIntVector]]
          case _: IntLongVector => typeOf[Int2ObjectOpenHashMap[IntLongVector]]
          case _: IntFloatVector => typeOf[Int2ObjectOpenHashMap[IntFloatVector]]
          case _: IntDoubleVector => typeOf[Int2ObjectOpenHashMap[IntDoubleVector]]
          case _: LongDummyVector => typeOf[Int2ObjectOpenHashMap[LongDummyVector]]
          case _: LongIntVector => typeOf[Int2ObjectOpenHashMap[LongIntVector]]
          case _: LongLongVector => typeOf[Int2ObjectOpenHashMap[LongLongVector]]
          case _: LongFloatVector => typeOf[Int2ObjectOpenHashMap[LongFloatVector]]
          case _: LongDoubleVector => typeOf[Int2ObjectOpenHashMap[LongDoubleVector]]
          case _: Array[Boolean] => typeOf[Int2ObjectOpenHashMap[Array[Boolean]]]
          case _: Array[Byte] => typeOf[Int2ObjectOpenHashMap[Array[Byte]]]
          case _: Array[Char] => typeOf[Int2ObjectOpenHashMap[Array[Char]]]
          case _: Array[Short] => typeOf[Int2ObjectOpenHashMap[Array[Short]]]
          case _: Array[Int] => typeOf[Int2ObjectOpenHashMap[Array[Int]]]
          case _: Array[Long] => typeOf[Int2ObjectOpenHashMap[Array[Long]]]
          case _: Array[Float] => typeOf[Int2ObjectOpenHashMap[Array[Float]]]
          case _: Array[Double] => typeOf[Int2ObjectOpenHashMap[Array[Double]]]
          case _: Array[String] => typeOf[Int2ObjectOpenHashMap[Array[String]]]
          case array: Array[_] =>
            if (array.nonEmpty) {
              array.head match {
                case _: String => typeOf[Int2ObjectOpenHashMap[Array[String]]]
                case _: NodeN => typeOf[Int2ObjectOpenHashMap[Array[NodeN]]]
                case _: NodeNW => typeOf[Int2ObjectOpenHashMap[Array[NodeNW]]]
                case _: NodeTN => typeOf[Int2ObjectOpenHashMap[Array[NodeTN]]]
                case _: NodeTNW => typeOf[Int2ObjectOpenHashMap[Array[NodeTNW]]]
                case _: IntDummyVector => typeOf[Int2ObjectOpenHashMap[Array[IntDummyVector]]]
                case _: IntIntVector => typeOf[Int2ObjectOpenHashMap[Array[IntIntVector]]]
                case _: IntLongVector => typeOf[Int2ObjectOpenHashMap[Array[IntLongVector]]]
                case _: IntFloatVector => typeOf[Int2ObjectOpenHashMap[Array[IntFloatVector]]]
                case _: IntDoubleVector => typeOf[Int2ObjectOpenHashMap[Array[IntDoubleVector]]]
                case _: LongDummyVector => typeOf[Int2ObjectOpenHashMap[Array[LongDummyVector]]]
                case _: LongIntVector => typeOf[Int2ObjectOpenHashMap[Array[LongIntVector]]]
                case _: LongLongVector => typeOf[Int2ObjectOpenHashMap[Array[LongLongVector]]]
                case _: LongFloatVector => typeOf[Int2ObjectOpenHashMap[Array[LongFloatVector]]]
                case _: LongDoubleVector => typeOf[Int2ObjectOpenHashMap[Array[LongDoubleVector]]]
                case _ => getReflectType(obj)
              }
            } else {
              getReflectType(obj)
            }
        }
      } else {
        getReflectType(obj)
      }
    case map: Long2ObjectOpenHashMap[_] =>
      if (map.size() > 0) {
        val entry = map.long2ObjectEntrySet().fastIterator().next()
        entry.getValue match {
          case _: String => typeOf[Long2ObjectOpenHashMap[String]]
          case _: NodeN => typeOf[Long2ObjectOpenHashMap[NodeN]]
          case _: NodeNW => typeOf[Long2ObjectOpenHashMap[NodeNW]]
          case _: NodeTN => typeOf[Long2ObjectOpenHashMap[NodeTN]]
          case _: NodeTNW => typeOf[Long2ObjectOpenHashMap[NodeTNW]]
          case _: IntDummyVector => typeOf[Long2ObjectOpenHashMap[IntDummyVector]]
          case _: IntIntVector => typeOf[Long2ObjectOpenHashMap[IntIntVector]]
          case _: IntLongVector => typeOf[Long2ObjectOpenHashMap[IntLongVector]]
          case _: IntFloatVector => typeOf[Long2ObjectOpenHashMap[IntFloatVector]]
          case _: IntDoubleVector => typeOf[Long2ObjectOpenHashMap[IntDoubleVector]]
          case _: LongDummyVector => typeOf[Long2ObjectOpenHashMap[LongDummyVector]]
          case _: LongIntVector => typeOf[Long2ObjectOpenHashMap[LongIntVector]]
          case _: LongLongVector => typeOf[Long2ObjectOpenHashMap[LongLongVector]]
          case _: LongFloatVector => typeOf[Long2ObjectOpenHashMap[LongFloatVector]]
          case _: LongDoubleVector => typeOf[Long2ObjectOpenHashMap[LongDoubleVector]]
          case _: Array[Boolean] => typeOf[Long2ObjectOpenHashMap[Array[Boolean]]]
          case _: Array[Byte] => typeOf[Long2ObjectOpenHashMap[Array[Byte]]]
          case _: Array[Char] => typeOf[Long2ObjectOpenHashMap[Array[Char]]]
          case _: Array[Short] => typeOf[Long2ObjectOpenHashMap[Array[Short]]]
          case _: Array[Int] => typeOf[Long2ObjectOpenHashMap[Array[Int]]]
          case _: Array[Long] => typeOf[Long2ObjectOpenHashMap[Array[Long]]]
          case _: Array[Float] => typeOf[Long2ObjectOpenHashMap[Array[Float]]]
          case _: Array[Double] => typeOf[Long2ObjectOpenHashMap[Array[Double]]]
          case _: Array[String] => typeOf[Long2ObjectOpenHashMap[Array[String]]]
          case array: Array[_] =>
            if (array.nonEmpty) {
              array.head match {
                case _: String => typeOf[Long2ObjectOpenHashMap[Array[String]]]
                case _: NodeN => typeOf[Long2ObjectOpenHashMap[Array[NodeN]]]
                case _: NodeNW => typeOf[Long2ObjectOpenHashMap[Array[NodeNW]]]
                case _: NodeTN => typeOf[Long2ObjectOpenHashMap[Array[NodeTN]]]
                case _: NodeTNW => typeOf[Long2ObjectOpenHashMap[Array[NodeTNW]]]
                case _: IntDummyVector => typeOf[Long2ObjectOpenHashMap[Array[IntDummyVector]]]
                case _: IntIntVector => typeOf[Long2ObjectOpenHashMap[Array[IntIntVector]]]
                case _: IntLongVector => typeOf[Long2ObjectOpenHashMap[Array[IntLongVector]]]
                case _: IntFloatVector => typeOf[Long2ObjectOpenHashMap[Array[IntFloatVector]]]
                case _: IntDoubleVector => typeOf[Long2ObjectOpenHashMap[Array[IntDoubleVector]]]
                case _: LongDummyVector => typeOf[Long2ObjectOpenHashMap[Array[LongDummyVector]]]
                case _: LongIntVector => typeOf[Long2ObjectOpenHashMap[Array[LongIntVector]]]
                case _: LongLongVector => typeOf[Long2ObjectOpenHashMap[Array[LongLongVector]]]
                case _: LongFloatVector => typeOf[Long2ObjectOpenHashMap[Array[LongFloatVector]]]
                case _: LongDoubleVector => typeOf[Long2ObjectOpenHashMap[Array[LongDoubleVector]]]
                case _ => getReflectType(obj)
              }
            } else {
              getReflectType(obj)
            }
        }
      } else {
        getReflectType(obj)
      }
    case _ => getReflectType(obj)
  }

  def typeFromString(typeStr: String): Type = typeCache.synchronized {
    if (typeCache.containsKey(typeStr)) {
      typeCache.get(typeStr)
    } else {
      val typ = try {
        typeStr match {
          case SIMPLE(value: String) =>
            val simpleType = if (value.contains(".")) value.split("\\.").last else value
            simpleType match {
              case "Int" => typeOf[Int]
              case "Long" => typeOf[Long]
              case "Float" => typeOf[Float]
              case "Double" => typeOf[Double]
              case "Boolean" => typeOf[Boolean]
              case "Char" => typeOf[Char]
              case "Short" => typeOf[Short]
              case "Byte" => typeOf[Byte]
              case "String" => typeOf[String]
              case "Int2IntOpenHashMap" => typeOf[Int2IntOpenHashMap]
              case "Int2LongOpenHashMap" => typeOf[Int2LongOpenHashMap]
              case "Int2FloatOpenHashMap" => typeOf[Int2FloatOpenHashMap]
              case "Int2DoubleOpenHashMap" => typeOf[Int2DoubleOpenHashMap]
              case "Int2BooleanOpenHashMap" => typeOf[Int2BooleanOpenHashMap]
              case "Int2CharOpenHashMap" => typeOf[Int2CharOpenHashMap]
              case "Int2ShortOpenHashMap" => typeOf[Int2ShortOpenHashMap]
              case "Int2ByteOpenHashMap" => typeOf[Int2ByteOpenHashMap]
              case "Long2IntOpenHashMap" => typeOf[Long2IntOpenHashMap]
              case "Long2LongOpenHashMap" => typeOf[Long2LongOpenHashMap]
              case "Long2FloatOpenHashMap" => typeOf[Long2FloatOpenHashMap]
              case "Long2DoubleOpenHashMap" => typeOf[Long2DoubleOpenHashMap]
              case "Long2BooleanOpenHashMap" => typeOf[Long2BooleanOpenHashMap]
              case "Long2CharOpenHashMap" => typeOf[Long2CharOpenHashMap]
              case "Long2ShortOpenHashMap" => typeOf[Long2ShortOpenHashMap]
              case "Long2ByteOpenHashMap" => typeOf[Long2ByteOpenHashMap]
              case "ANode" => typeOf[ANode]
              case "NodeN" => typeOf[NodeN]
              case "NodeNW" => typeOf[NodeNW]
              case "NodeTN" => typeOf[NodeTN]
              case "NodeTNW" => typeOf[NodeTNW]
              case "IntDummyVector" => typeOf[IntDummyVector]
              case "IntIntVector" => typeOf[IntIntVector]
              case "IntLongVector" => typeOf[IntLongVector]
              case "IntFloatVector" => typeOf[IntFloatVector]
              case "IntDoubleVector" => typeOf[IntDoubleVector]
              case "LongDummyVector" => typeOf[LongDummyVector]
              case "LongIntVector" => typeOf[LongIntVector]
              case "LongLongVector" => typeOf[LongLongVector]
              case "LongFloatVector" => typeOf[LongFloatVector]
              case "LongDoubleVector" => typeOf[LongDoubleVector]
            }
          case ARRAY(value: String) =>
            val elementType = if (value.contains(".")) value.split("\\.").last else value
            elementType match {
              case "Int" => typeOf[Array[Int]]
              case "Long" => typeOf[Array[Long]]
              case "Float" => typeOf[Array[Float]]
              case "Double" => typeOf[Array[Double]]
              case "Boolean" => typeOf[Array[Boolean]]
              case "Char" => typeOf[Array[Char]]
              case "Short" => typeOf[Array[Short]]
              case "Byte" => typeOf[Array[Byte]]
              case "String" => typeOf[Array[String]]
              case "ANode" => typeOf[Array[ANode]]
              case "NodeN" => typeOf[Array[NodeN]]
              case "NodeNW" => typeOf[Array[NodeNW]]
              case "NodeTN" => typeOf[Array[NodeTN]]
              case "NodeTNW" => typeOf[Array[NodeTNW]]
              case "IntDummyVector" => typeOf[Array[IntDummyVector]]
              case "IntIntVector" => typeOf[Array[IntIntVector]]
              case "IntLongVector" => typeOf[Array[IntLongVector]]
              case "IntFloatVector" => typeOf[Array[IntFloatVector]]
              case "IntDoubleVector" => typeOf[Array[IntDoubleVector]]
              case "LongDummyVector" => typeOf[Array[LongDummyVector]]
              case "LongIntVector" => typeOf[Array[LongIntVector]]
              case "LongLongVector" => typeOf[Array[LongLongVector]]
              case "LongFloatVector" => typeOf[Array[LongFloatVector]]
              case "LongDoubleVector" => typeOf[Array[LongDoubleVector]]
            }
          case WITHTYPEPARAM(outer: String, inner: String) =>
            val outerType = if (outer.contains(".")) outer.split("\\.").last else outer
            val innerType = if (inner.contains(".")) inner.split("\\.").last else inner
            outerType match {
              case "Int2ObjectOpenHashMap" =>
                innerType match {
                  case "ANode" => typeOf[Int2ObjectOpenHashMap[ANode]]
                  case "NodeN" => typeOf[Int2ObjectOpenHashMap[NodeN]]
                  case "NodeNW" => typeOf[Int2ObjectOpenHashMap[NodeNW]]
                  case "NodeTN" => typeOf[Int2ObjectOpenHashMap[NodeTN]]
                  case "NodeTNW" => typeOf[Int2ObjectOpenHashMap[NodeTNW]]
                  case "IntDummyVector" => typeOf[Int2ObjectOpenHashMap[IntDummyVector]]
                  case "IntIntVector" => typeOf[Int2ObjectOpenHashMap[IntIntVector]]
                  case "IntLongVector" => typeOf[Int2ObjectOpenHashMap[IntLongVector]]
                  case "IntFloatVector" => typeOf[Int2ObjectOpenHashMap[IntFloatVector]]
                  case "IntDoubleVector" => typeOf[Int2ObjectOpenHashMap[IntDoubleVector]]
                  case "LongDummyVector" => typeOf[Int2ObjectOpenHashMap[LongDummyVector]]
                  case "LongIntVector" => typeOf[Int2ObjectOpenHashMap[LongIntVector]]
                  case "LongLongVector" => typeOf[Int2ObjectOpenHashMap[LongLongVector]]
                  case "LongFloatVector" => typeOf[Int2ObjectOpenHashMap[LongFloatVector]]
                  case "LongDoubleVector" => typeOf[Int2ObjectOpenHashMap[LongDoubleVector]]
                }
              case "Long2ObjectOpenHashMap" =>
                innerType match {
                  case "ANode" => typeOf[Long2ObjectOpenHashMap[ANode]]
                  case "NodeN" => typeOf[Long2ObjectOpenHashMap[NodeN]]
                  case "NodeNW" => typeOf[Long2ObjectOpenHashMap[NodeNW]]
                  case "NodeTN" => typeOf[Long2ObjectOpenHashMap[NodeTN]]
                  case "NodeTNW" => typeOf[Long2ObjectOpenHashMap[NodeTNW]]
                  case "IntDummyVector" => typeOf[Long2ObjectOpenHashMap[IntDummyVector]]
                  case "IntIntVector" => typeOf[Long2ObjectOpenHashMap[IntIntVector]]
                  case "IntLongVector" => typeOf[Long2ObjectOpenHashMap[IntLongVector]]
                  case "IntFloatVector" => typeOf[Long2ObjectOpenHashMap[IntFloatVector]]
                  case "IntDoubleVector" => typeOf[Long2ObjectOpenHashMap[IntDoubleVector]]
                  case "LongDummyVector" => typeOf[Long2ObjectOpenHashMap[LongDummyVector]]
                  case "LongIntVector" => typeOf[Long2ObjectOpenHashMap[LongIntVector]]
                  case "LongLongVector" => typeOf[Long2ObjectOpenHashMap[LongLongVector]]
                  case "LongFloatVector" => typeOf[Long2ObjectOpenHashMap[LongFloatVector]]
                  case "LongDoubleVector" => typeOf[Long2ObjectOpenHashMap[LongDoubleVector]]
                }
            }
          case WITHARRAYTYPEPARAM(outer: String, inner: String) =>
            val outerType = if (outer.contains(".")) outer.split("\\.").last else outer
            val innerType = if (inner.contains(".")) inner.split("\\.").last else inner
            outerType match {
              case "Int2ObjectOpenHashMap" =>
                innerType match {
                  case "Int" => typeOf[Int2ObjectOpenHashMap[Array[Int]]]
                  case "Long" => typeOf[Int2ObjectOpenHashMap[Array[Long]]]
                  case "Float" => typeOf[Int2ObjectOpenHashMap[Array[Float]]]
                  case "Double" => typeOf[Int2ObjectOpenHashMap[Array[Double]]]
                  case "Boolean" => typeOf[Int2ObjectOpenHashMap[Array[Boolean]]]
                  case "Char" => typeOf[Int2ObjectOpenHashMap[Array[Char]]]
                  case "Short" => typeOf[Int2ObjectOpenHashMap[Array[Short]]]
                  case "Byte" => typeOf[Int2ObjectOpenHashMap[Array[Byte]]]
                  case "String" => typeOf[Int2ObjectOpenHashMap[Array[String]]]
                  case "ANode" => typeOf[Int2ObjectOpenHashMap[Array[ANode]]]
                  case "NodeN" => typeOf[Int2ObjectOpenHashMap[Array[NodeN]]]
                  case "NodeNW" => typeOf[Int2ObjectOpenHashMap[Array[NodeNW]]]
                  case "NodeTN" => typeOf[Int2ObjectOpenHashMap[Array[NodeTN]]]
                  case "NodeTNW" => typeOf[Int2ObjectOpenHashMap[Array[NodeTNW]]]
                  case "IntDummyVector" => typeOf[Int2ObjectOpenHashMap[Array[IntDummyVector]]]
                  case "IntIntVector" => typeOf[Int2ObjectOpenHashMap[Array[IntIntVector]]]
                  case "IntLongVector" => typeOf[Int2ObjectOpenHashMap[Array[IntLongVector]]]
                  case "IntFloatVector" => typeOf[Int2ObjectOpenHashMap[Array[IntFloatVector]]]
                  case "IntDoubleVector" => typeOf[Int2ObjectOpenHashMap[Array[IntDoubleVector]]]
                  case "LongDummyVector" => typeOf[Int2ObjectOpenHashMap[Array[LongDummyVector]]]
                  case "LongIntVector" => typeOf[Int2ObjectOpenHashMap[Array[LongIntVector]]]
                  case "LongLongVector" => typeOf[Int2ObjectOpenHashMap[Array[LongLongVector]]]
                  case "LongFloatVector" => typeOf[Int2ObjectOpenHashMap[Array[LongFloatVector]]]
                  case "LongDoubleVector" => typeOf[Int2ObjectOpenHashMap[Array[LongDoubleVector]]]
                }
              case "Long2ObjectOpenHashMap" =>
                innerType match {
                  case "Int" => typeOf[Long2ObjectOpenHashMap[Array[Int]]]
                  case "Long" => typeOf[Long2ObjectOpenHashMap[Array[Long]]]
                  case "Float" => typeOf[Long2ObjectOpenHashMap[Array[Float]]]
                  case "Double" => typeOf[Long2ObjectOpenHashMap[Array[Double]]]
                  case "Boolean" => typeOf[Long2ObjectOpenHashMap[Array[Boolean]]]
                  case "Char" => typeOf[Long2ObjectOpenHashMap[Array[Char]]]
                  case "Short" => typeOf[Long2ObjectOpenHashMap[Array[Short]]]
                  case "Byte" => typeOf[Long2ObjectOpenHashMap[Array[Byte]]]
                  case "String" => typeOf[Long2ObjectOpenHashMap[Array[String]]]
                  case "ANode" => typeOf[Long2ObjectOpenHashMap[Array[ANode]]]
                  case "NodeN" => typeOf[Long2ObjectOpenHashMap[Array[NodeN]]]
                  case "NodeNW" => typeOf[Long2ObjectOpenHashMap[Array[NodeNW]]]
                  case "NodeTN" => typeOf[Long2ObjectOpenHashMap[Array[NodeTN]]]
                  case "NodeTNW" => typeOf[Long2ObjectOpenHashMap[Array[NodeTNW]]]
                  case "IntDummyVector" => typeOf[Long2ObjectOpenHashMap[Array[IntDummyVector]]]
                  case "IntIntVector" => typeOf[Long2ObjectOpenHashMap[Array[IntIntVector]]]
                  case "IntLongVector" => typeOf[Long2ObjectOpenHashMap[Array[IntLongVector]]]
                  case "IntFloatVector" => typeOf[Long2ObjectOpenHashMap[Array[IntFloatVector]]]
                  case "IntDoubleVector" => typeOf[Long2ObjectOpenHashMap[Array[IntDoubleVector]]]
                  case "LongDummyVector" => typeOf[Long2ObjectOpenHashMap[Array[LongDummyVector]]]
                  case "LongIntVector" => typeOf[Long2ObjectOpenHashMap[Array[LongIntVector]]]
                  case "LongLongVector" => typeOf[Long2ObjectOpenHashMap[Array[LongLongVector]]]
                  case "LongFloatVector" => typeOf[Long2ObjectOpenHashMap[Array[LongFloatVector]]]
                  case "LongDoubleVector" => typeOf[Long2ObjectOpenHashMap[Array[LongDoubleVector]]]
                }
            }
        }
      } catch {
        case _: Exception =>
          val parsed = tb.parse(s"scala.reflect.runtime.universe.typeOf[$typeStr]")
          tb.eval(parsed).asInstanceOf[Type]
      }

      typeCache.put(typeStr, typ)
      typ
    }
  }

  def getFields(tpe: Type): List[TermSymbol] = {
    val buf = new ListBuffer[TermSymbol]()
    tpe.members.foreach {
      case field: TermSymbol if !field.isMethod && (field.isVal || field.isVal) =>
        val annotations = field.annotations
        if (annotations.isEmpty) {
          buf.append(field)
        } else {
          val trans = annotations.forall { ann => !ann.toString.equalsIgnoreCase("transient") }
          if (trans) {
            buf.append(field)
          }
        }
      case _ =>
    }

    buf.toList
  }
}
