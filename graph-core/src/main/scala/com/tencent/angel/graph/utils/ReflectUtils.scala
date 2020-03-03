package com.tencent.angel.graph.utils

import com.tencent.angel.graph.VertexId
import com.tencent.angel.graph.core.data._
import com.tencent.angel.ml.math2.vector._
import it.unimi.dsi.fastutil.ints._
import it.unimi.dsi.fastutil.longs._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox


object ReflectUtils {
  private val mirror = runtimeMirror(ReflectUtils.getClass.getClassLoader)
  private val tb = mirror.mkToolBox()

  private val Atomic = "([\\w.]+)".r
  private val ArrayType = "Array\\[([\\w.]+)]".r
  private val FastMap = "([\\w.]+)\\[([\\w.]+),([\\w.]+)]".r
  private val FastMapArray = "([\\w.]+)\\[([\\w.]+),Array\\[([\\w.]+)]]".r
  private val UnimiMap = "([\\w.]+)\\[([\\w.]+)]".r
  private val UnimiMapArray = "([\\w.]+)\\[Array[([\\w.]+)]]".r

  private val ObjCache = new mutable.HashMap[String, Any]()

  def newInstance(tpe: Type): Any = {
    val constructor = getMethod(tpe: Type, termNames.CONSTRUCTOR)
    val classMirror = mirror.reflectClass(tpe.typeSymbol.asClass)
    val methodMirror = classMirror.reflectConstructor(constructor)
    methodMirror()
  }

  def register(tpe: Type, obj: Any): Unit = ObjCache.synchronized {
    val tpeString = tpe.toString
    if (!ObjCache.contains(tpeString)) {
      ObjCache(tpeString) = obj
    }
  }

  def newFastHashMap(tpe: Type): FastHashMap[_, _] = ObjCache.synchronized {
    assert(GUtils.isSerFastHashMap(tpe))
    val tpeString = tpe.toString
    if (ObjCache.contains(tpeString)) {
      ObjCache(tpeString).asInstanceOf[FastHashMap[_, _]].emptyLike()
    } else {
      val parsed = tb.parse(s"new $tpeString")
      val inst = tb.eval(parsed).asInstanceOf[FastHashMap[_, _]]
      ObjCache(tpeString) = inst
      inst.emptyLike()
    }
  }

  def newLongKeyMap(tpe: Type): Long2ObjectOpenHashMap[_] = ObjCache.synchronized {
    assert(GUtils.isSerLongKeyMap(tpe))
    val tpeString = tpe.toString
    if (ObjCache.contains(tpeString)) {
      ObjCache(tpeString).asInstanceOf[Long2ObjectOpenHashMap[_]].clone()
    } else {
      val parsed = tb.parse(s"new $tpeString")
      val inst = tb.eval(parsed).asInstanceOf[Long2ObjectOpenHashMap[_]]
      ObjCache(tpeString) = inst
      inst.clone()
    }
  }

  def newIntKeyMap(tpe: Type): Int2ObjectOpenHashMap[_] = ObjCache.synchronized {
    assert(GUtils.isSerLongKeyMap(tpe))
    val tpeString = tpe.toString
    if (ObjCache.contains(tpeString)) {
      ObjCache(tpeString).asInstanceOf[Int2ObjectOpenHashMap[_]].clone()
    } else {
      val parsed = tb.parse(s"new $tpeString")
      val inst = tb.eval(parsed).asInstanceOf[Int2ObjectOpenHashMap[_]]
      ObjCache(tpeString) = inst
      inst.clone()
    }
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

  def getMethod(tpe: Type, name: TermName, args: Type*): MethodSymbol = {
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

  def getType[T: TypeTag](obj: T): Type = typeOf[T]

  def typeFromObject(obj: Any): Type = {
    mirror.reflect(obj).symbol.toType
  }

  def getTypeTag[T: TypeTag](obj: T): TypeTag[T] = {
    implicitly[TypeTag[T]]
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

  def typeFromString(tpeStr: String): Type = {
    try {
      tpeStr match {
        case Atomic(t) =>
          t.split("\\.").last match {
            case "Boolean" => typeOf[Boolean]
            case "Char" => typeOf[Char]
            case "Byte" => typeOf[Byte]
            case "Short" => typeOf[Short]
            case "Int" => typeOf[Int]
            case "Long" => typeOf[Long]
            case "Float" => typeOf[Float]
            case "Double" => typeOf[Double]
            case "String" => typeOf[String]
            case "VertexId" => typeOf[VertexId]
            case "Int2BooleanOpenHashMap" => typeOf[Int2BooleanOpenHashMap]
            case "Int2CharOpenHashMap" => typeOf[Int2CharOpenHashMap]
            case "Int2ByteOpenHashMap" => typeOf[Int2ByteOpenHashMap]
            case "Int2ShortOpenHashMap" => typeOf[Int2ShortOpenHashMap]
            case "Int2IntOpenHashMap" => typeOf[Int2IntOpenHashMap]
            case "Int2LongOpenHashMap" => typeOf[Int2LongOpenHashMap]
            case "Int2FloatOpenHashMap" => typeOf[Int2FloatOpenHashMap]
            case "Int2DoubleOpenHashMap" => typeOf[Int2DoubleOpenHashMap]
            case "Long2BooleanOpenHashMap" => typeOf[Long2BooleanOpenHashMap]
            case "Long2CharOpenHashMap" => typeOf[Long2CharOpenHashMap]
            case "Long2ByteOpenHashMap" => typeOf[Long2ByteOpenHashMap]
            case "Long2ShortOpenHashMap" => typeOf[Long2ShortOpenHashMap]
            case "Long2IntOpenHashMap" => typeOf[Long2IntOpenHashMap]
            case "Long2LongOpenHashMap" => typeOf[Long2LongOpenHashMap]
            case "Long2FloatOpenHashMap" => typeOf[Long2FloatOpenHashMap]
            case "Long2DoubleOpenHashMap" => typeOf[Long2DoubleOpenHashMap]
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
            case "NeighN" => typeOf[NeighN]
            case "NeighNW" => typeOf[NeighNW]
            case "NeighTN" => typeOf[NeighTN]
            case "NeighTNW" => typeOf[NeighTNW]
          }
        case ArrayType(t) =>
          t.split("\\.").last match {
            case "Boolean" => typeOf[Array[Boolean]]
            case "Char" => typeOf[Array[Char]]
            case "Byte" => typeOf[Array[Byte]]
            case "Short" => typeOf[Array[Short]]
            case "Int" => typeOf[Array[Int]]
            case "Long" => typeOf[Array[Long]]
            case "Float" => typeOf[Array[Float]]
            case "Double" => typeOf[Array[Double]]
            case "String" => typeOf[Array[String]]
            case "VertexId" => typeOf[Array[VertexId]]
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
            case "NeighN" => typeOf[Array[NeighN]]
            case "NeighNW" => typeOf[Array[NeighNW]]
            case "NeighTN" => typeOf[Array[NeighTN]]
            case "NeighTNW" => typeOf[Array[NeighTNW]]
          }
        case FastMap(fm, k, v) if fm.split("\\.").last.equalsIgnoreCase("FastHashMap") =>
          val key = k.split("\\.").last
          val value = v.split("\\.").last
          key match {
            case "Int" =>
              value match {
                case "Boolean" =>
                  ObjCache(tpeStr) = new FastHashMap[Int, Boolean]
                  typeOf[FastHashMap[Int, Boolean]]
                case "Char" =>
                  ObjCache(tpeStr) = new FastHashMap[Int, Char]
                  typeOf[FastHashMap[Int, Char]]
                case "Byte" =>
                  ObjCache(tpeStr) = new FastHashMap[Int, Byte]
                  typeOf[FastHashMap[Int, Byte]]
                case "Short" =>
                  ObjCache(tpeStr) = new FastHashMap[Int, Short]
                  typeOf[FastHashMap[Int, Short]]
                case "Int" =>
                  ObjCache(tpeStr) = new FastHashMap[Int, Int]
                  typeOf[FastHashMap[Int, Int]]
                case "Long" =>
                  ObjCache(tpeStr) = new FastHashMap[Int, Long]
                  typeOf[FastHashMap[Int, Long]]
                case "Float" =>
                  ObjCache(tpeStr) = new FastHashMap[Int, Float]
                  typeOf[FastHashMap[Int, Float]]
                case "Double" =>
                  ObjCache(tpeStr) = new FastHashMap[Int, Double]
                  typeOf[FastHashMap[Int, Double]]
                case "String" =>
                  ObjCache(tpeStr) = new FastHashMap[Int, String]
                  typeOf[FastHashMap[Int, String]]
                case "IntDummyVector" =>
                  ObjCache(tpeStr) = new FastHashMap[Int, IntDummyVector]
                  typeOf[FastHashMap[Int, IntDummyVector]]
                case "IntIntVector" =>
                  ObjCache(tpeStr) = new FastHashMap[Int, IntIntVector]
                  typeOf[FastHashMap[Int, IntIntVector]]
                case "IntLongVector" =>
                  ObjCache(tpeStr) = new FastHashMap[Int, IntLongVector]
                  typeOf[FastHashMap[Int, IntLongVector]]
                case "IntFloatVector" =>
                  ObjCache(tpeStr) = new FastHashMap[Int, IntFloatVector]
                  typeOf[FastHashMap[Int, IntFloatVector]]
                case "IntDoubleVector" =>
                  ObjCache(tpeStr) = new FastHashMap[Int, IntDoubleVector]
                  typeOf[FastHashMap[Int, IntDoubleVector]]
                case "LongDummyVector" =>
                  ObjCache(tpeStr) = new FastHashMap[Int, LongDummyVector]
                  typeOf[FastHashMap[Int, LongDummyVector]]
                case "LongIntVector" =>
                  ObjCache(tpeStr) = new FastHashMap[Int, LongIntVector]
                  typeOf[FastHashMap[Int, LongIntVector]]
                case "LongLongVector" =>
                  ObjCache(tpeStr) = new FastHashMap[Int, LongLongVector]
                  typeOf[FastHashMap[Int, LongLongVector]]
                case "LongFloatVector" =>
                  ObjCache(tpeStr) = new FastHashMap[Int, LongFloatVector]
                  typeOf[FastHashMap[Int, LongFloatVector]]
                case "LongDoubleVector" =>
                  ObjCache(tpeStr) = new FastHashMap[Int, LongDoubleVector]
                  typeOf[FastHashMap[Int, LongDoubleVector]]
                case "NeighN" =>
                  ObjCache(tpeStr) = new FastHashMap[Int, NeighN]
                  typeOf[FastHashMap[Int, NeighN]]
                case "NeighNW" =>
                  ObjCache(tpeStr) = new FastHashMap[Int, NeighNW]
                  typeOf[FastHashMap[Int, NeighNW]]
                case "NeighTN" =>
                  ObjCache(tpeStr) = new FastHashMap[Int, NeighTN]
                  typeOf[FastHashMap[Int, NeighTN]]
                case "NeighTNW" =>
                  ObjCache(tpeStr) = new FastHashMap[Int, NeighTNW]
                  typeOf[FastHashMap[Int, NeighTNW]]
              }
            case "Long" =>
              value match {
                case "Boolean" =>
                  ObjCache(tpeStr) = new FastHashMap[Long, Boolean]
                  typeOf[FastHashMap[Long, Boolean]]
                case "Char" =>
                  ObjCache(tpeStr) = new FastHashMap[Long, Char]
                  typeOf[FastHashMap[Long, Char]]
                case "Byte" =>
                  ObjCache(tpeStr) = new FastHashMap[Long, Byte]
                  typeOf[FastHashMap[Long, Byte]]
                case "Short" =>
                  ObjCache(tpeStr) = new FastHashMap[Long, Short]
                  typeOf[FastHashMap[Long, Short]]
                case "Int" =>
                  ObjCache(tpeStr) = new FastHashMap[Long, Int]
                  typeOf[FastHashMap[Long, Int]]
                case "Long" =>
                  ObjCache(tpeStr) = new FastHashMap[Long, Long]
                  typeOf[FastHashMap[Long, Long]]
                case "Float" =>
                  ObjCache(tpeStr) = new FastHashMap[Long, Float]
                  typeOf[FastHashMap[Long, Float]]
                case "Double" =>
                  ObjCache(tpeStr) = new FastHashMap[Long, Double]
                  typeOf[FastHashMap[Long, Double]]
                case "String" =>
                  ObjCache(tpeStr) = new FastHashMap[Long, String]
                  typeOf[FastHashMap[Long, String]]
                case "IntDummyVector" =>
                  ObjCache(tpeStr) = new FastHashMap[Long, IntDummyVector]
                  typeOf[FastHashMap[Long, IntDummyVector]]
                case "IntIntVector" =>
                  ObjCache(tpeStr) = new FastHashMap[Long, IntIntVector]
                  typeOf[FastHashMap[Long, IntIntVector]]
                case "IntLongVector" =>
                  ObjCache(tpeStr) = new FastHashMap[Long, IntLongVector]
                  typeOf[FastHashMap[Long, IntLongVector]]
                case "IntFloatVector" =>
                  ObjCache(tpeStr) = new FastHashMap[Long, IntFloatVector]
                  typeOf[FastHashMap[Long, IntFloatVector]]
                case "IntDoubleVector" =>
                  ObjCache(tpeStr) = new FastHashMap[Long, IntDoubleVector]
                  typeOf[FastHashMap[Long, IntDoubleVector]]
                case "LongDummyVector" =>
                  ObjCache(tpeStr) = new FastHashMap[Long, LongDummyVector]
                  typeOf[FastHashMap[Long, LongDummyVector]]
                case "LongIntVector" =>
                  ObjCache(tpeStr) = new FastHashMap[Long, LongIntVector]
                  typeOf[FastHashMap[Long, LongIntVector]]
                case "LongLongVector" =>
                  ObjCache(tpeStr) = new FastHashMap[Long, LongLongVector]
                  typeOf[FastHashMap[Long, LongLongVector]]
                case "LongFloatVector" =>
                  ObjCache(tpeStr) = new FastHashMap[Long, LongFloatVector]
                  typeOf[FastHashMap[Long, LongFloatVector]]
                case "LongDoubleVector" =>
                  ObjCache(tpeStr) = new FastHashMap[Long, LongDoubleVector]
                  typeOf[FastHashMap[Long, LongDoubleVector]]
                case "NeighN" =>
                  ObjCache(tpeStr) = new FastHashMap[Long, NeighN]
                  typeOf[FastHashMap[Long, NeighN]]
                case "NeighNW" =>
                  ObjCache(tpeStr) = new FastHashMap[Long, NeighNW]
                  typeOf[FastHashMap[Long, NeighNW]]
                case "NeighTN" =>
                  ObjCache(tpeStr) = new FastHashMap[Long, NeighTN]
                  typeOf[FastHashMap[Long, NeighTN]]
                case "NeighTNW" =>
                  ObjCache(tpeStr) = new FastHashMap[Long, NeighTNW]
                  typeOf[FastHashMap[Long, NeighTNW]]
              }
            case "VertexId" =>
              value match {
                case "Boolean" =>
                  ObjCache(tpeStr) = new FastHashMap[VertexId, Boolean]
                  typeOf[FastHashMap[VertexId, Boolean]]
                case "Char" =>
                  ObjCache(tpeStr) = new FastHashMap[VertexId, Char]
                  typeOf[FastHashMap[VertexId, Char]]
                case "Byte" =>
                  ObjCache(tpeStr) = new FastHashMap[VertexId, Byte]
                  typeOf[FastHashMap[VertexId, Byte]]
                case "Short" =>
                  ObjCache(tpeStr) = new FastHashMap[VertexId, Short]
                  typeOf[FastHashMap[VertexId, Short]]
                case "Int" =>
                  ObjCache(tpeStr) = new FastHashMap[VertexId, Int]
                  typeOf[FastHashMap[VertexId, Int]]
                case "Long" =>
                  ObjCache(tpeStr) = new FastHashMap[VertexId, Long]
                  typeOf[FastHashMap[VertexId, Long]]
                case "Float" =>
                  ObjCache(tpeStr) = new FastHashMap[VertexId, Float]
                  typeOf[FastHashMap[VertexId, Float]]
                case "Double" =>
                  ObjCache(tpeStr) = new FastHashMap[VertexId, Double]
                  typeOf[FastHashMap[VertexId, Double]]
                case "String" =>
                  ObjCache(tpeStr) = new FastHashMap[VertexId, String]
                  typeOf[FastHashMap[VertexId, String]]
                case "IntDummyVector" =>
                  ObjCache(tpeStr) = new FastHashMap[VertexId, IntDummyVector]
                  typeOf[FastHashMap[VertexId, IntDummyVector]]
                case "IntIntVector" =>
                  ObjCache(tpeStr) = new FastHashMap[VertexId, IntIntVector]
                  typeOf[FastHashMap[VertexId, IntIntVector]]
                case "IntLongVector" =>
                  ObjCache(tpeStr) = new FastHashMap[VertexId, IntLongVector]
                  typeOf[FastHashMap[VertexId, IntLongVector]]
                case "IntFloatVector" =>
                  ObjCache(tpeStr) = new FastHashMap[VertexId, IntFloatVector]
                  typeOf[FastHashMap[VertexId, IntFloatVector]]
                case "IntDoubleVector" =>
                  ObjCache(tpeStr) = new FastHashMap[VertexId, IntDoubleVector]
                  typeOf[FastHashMap[VertexId, IntDoubleVector]]
                case "LongDummyVector" =>
                  ObjCache(tpeStr) = new FastHashMap[VertexId, LongDummyVector]
                  typeOf[FastHashMap[VertexId, LongDummyVector]]
                case "LongIntVector" =>
                  ObjCache(tpeStr) = new FastHashMap[VertexId, LongIntVector]
                  typeOf[FastHashMap[VertexId, LongIntVector]]
                case "LongLongVector" =>
                  ObjCache(tpeStr) = new FastHashMap[VertexId, LongLongVector]
                  typeOf[FastHashMap[VertexId, LongLongVector]]
                case "LongFloatVector" =>
                  ObjCache(tpeStr) = new FastHashMap[VertexId, LongFloatVector]
                  typeOf[FastHashMap[VertexId, LongFloatVector]]
                case "LongDoubleVector" =>
                  ObjCache(tpeStr) = new FastHashMap[VertexId, LongDoubleVector]
                  typeOf[FastHashMap[VertexId, LongDoubleVector]]
                case "NeighN" =>
                  ObjCache(tpeStr) = new FastHashMap[VertexId, NeighN]
                  typeOf[FastHashMap[VertexId, NeighN]]
                case "NeighNW" =>
                  ObjCache(tpeStr) = new FastHashMap[VertexId, NeighNW]
                  typeOf[FastHashMap[VertexId, NeighNW]]
                case "NeighTN" =>
                  ObjCache(tpeStr) = new FastHashMap[VertexId, NeighTN]
                  typeOf[FastHashMap[VertexId, NeighTN]]
                case "NeighTNW" =>
                  ObjCache(tpeStr) = new FastHashMap[VertexId, NeighTNW]
                  typeOf[FastHashMap[VertexId, NeighTNW]]
              }
          }
        case FastMapArray(fm, k, v) if fm.split("\\.").last.equalsIgnoreCase("FastHashMap") =>
          val key = k.split("\\.").last
          val value = v.split("\\.").last
          key match {
            case "Int" =>
              value match {
                case "Boolean" =>
                  ObjCache(tpeStr) = new FastHashMap[Int, Array[Boolean]]
                  typeOf[FastHashMap[Int, Array[Boolean]]]
                case "Char" =>
                  ObjCache(tpeStr) = new FastHashMap[Int, Array[Char]]
                  typeOf[FastHashMap[Int, Array[Char]]]
                case "Byte" =>
                  ObjCache(tpeStr) = new FastHashMap[Int, Array[Byte]]
                  typeOf[FastHashMap[Int, Array[Byte]]]
                case "Short" =>
                  ObjCache(tpeStr) = new FastHashMap[Int, Array[Short]]
                  typeOf[FastHashMap[Int, Array[Short]]]
                case "Int" =>
                  ObjCache(tpeStr) = new FastHashMap[Int, Array[Int]]
                  typeOf[FastHashMap[Int, Array[Int]]]
                case "Long" =>
                  ObjCache(tpeStr) = new FastHashMap[Int, Array[Long]]
                  typeOf[FastHashMap[Int, Array[Long]]]
                case "Float" =>
                  ObjCache(tpeStr) = new FastHashMap[Int, Array[Float]]
                  typeOf[FastHashMap[Int, Array[Float]]]
                case "Double" =>
                  ObjCache(tpeStr) = new FastHashMap[Int, Array[Double]]
                  typeOf[FastHashMap[Int, Array[Double]]]
                case "String" =>
                  ObjCache(tpeStr) = new FastHashMap[Int, Array[String]]
                  typeOf[FastHashMap[Int, Array[String]]]
                case "IntDummyVector" =>
                  ObjCache(tpeStr) = new FastHashMap[Int, Array[IntDummyVector]]
                  typeOf[FastHashMap[Int, Array[IntDummyVector]]]
                case "IntIntVector" =>
                  ObjCache(tpeStr) = new FastHashMap[Int, Array[IntIntVector]]
                  typeOf[FastHashMap[Int, Array[IntIntVector]]]
                case "IntLongVector" =>
                  ObjCache(tpeStr) = new FastHashMap[Int, Array[IntLongVector]]
                  typeOf[FastHashMap[Int, Array[IntLongVector]]]
                case "IntFloatVector" =>
                  ObjCache(tpeStr) = new FastHashMap[Int, Array[IntFloatVector]]
                  typeOf[FastHashMap[Int, Array[IntFloatVector]]]
                case "IntDoubleVector" =>
                  ObjCache(tpeStr) = new FastHashMap[Int, Array[IntDoubleVector]]
                  typeOf[FastHashMap[Int, Array[IntDoubleVector]]]
                case "LongDummyVector" =>
                  ObjCache(tpeStr) = new FastHashMap[Int, Array[LongDummyVector]]
                  typeOf[FastHashMap[Int, Array[LongDummyVector]]]
                case "LongIntVector" =>
                  ObjCache(tpeStr) = new FastHashMap[Int, Array[LongIntVector]]
                  typeOf[FastHashMap[Int, Array[LongIntVector]]]
                case "LongLongVector" =>
                  ObjCache(tpeStr) = new FastHashMap[Int, Array[LongLongVector]]
                  typeOf[FastHashMap[Int, Array[LongLongVector]]]
                case "LongFloatVector" =>
                  ObjCache(tpeStr) = new FastHashMap[Int, Array[LongFloatVector]]
                  typeOf[FastHashMap[Int, Array[LongFloatVector]]]
                case "LongDoubleVector" =>
                  ObjCache(tpeStr) = new FastHashMap[Int, Array[LongDoubleVector]]
                  typeOf[FastHashMap[Int, Array[LongDoubleVector]]]
                case "NeighN" =>
                  ObjCache(tpeStr) = new FastHashMap[Int, Array[NeighN]]
                  typeOf[FastHashMap[Int, Array[NeighN]]]
                case "NeighNW" =>
                  ObjCache(tpeStr) = new FastHashMap[Int, Array[NeighNW]]
                  typeOf[FastHashMap[Int, Array[NeighNW]]]
                case "NeighTN" =>
                  ObjCache(tpeStr) = new FastHashMap[Int, Array[NeighTN]]
                  typeOf[FastHashMap[Int, Array[NeighTN]]]
                case "NeighTNW" =>
                  ObjCache(tpeStr) = new FastHashMap[Int, Array[NeighTNW]]
                  typeOf[FastHashMap[Int, Array[NeighTNW]]]
              }
            case "Long" =>
              value match {
                case "Boolean" =>
                  ObjCache(tpeStr) = new FastHashMap[Long, Array[Boolean]]
                  typeOf[FastHashMap[Long, Array[Boolean]]]
                case "Char" =>
                  ObjCache(tpeStr) = new FastHashMap[Long, Array[Char]]
                  typeOf[FastHashMap[Long, Array[Char]]]
                case "Byte" =>
                  ObjCache(tpeStr) = new FastHashMap[Long, Array[Byte]]
                  typeOf[FastHashMap[Long, Array[Byte]]]
                case "Short" =>
                  ObjCache(tpeStr) = new FastHashMap[Long, Array[Short]]
                  typeOf[FastHashMap[Long, Array[Short]]]
                case "Int" =>
                  ObjCache(tpeStr) = new FastHashMap[Long, Array[Int]]
                  typeOf[FastHashMap[Long, Array[Int]]]
                case "Long" =>
                  ObjCache(tpeStr) = new FastHashMap[Long, Array[Long]]
                  typeOf[FastHashMap[Long, Array[Long]]]
                case "Float" =>
                  ObjCache(tpeStr) = new FastHashMap[Long, Array[Float]]
                  typeOf[FastHashMap[Long, Array[Float]]]
                case "Double" =>
                  ObjCache(tpeStr) = new FastHashMap[Long, Array[Double]]
                  typeOf[FastHashMap[Long, Array[Double]]]
                case "String" =>
                  ObjCache(tpeStr) = new FastHashMap[Long, Array[String]]
                  typeOf[FastHashMap[Long, Array[String]]]
                case "IntDummyVector" =>
                  ObjCache(tpeStr) = new FastHashMap[Long, Array[IntDummyVector]]
                  typeOf[FastHashMap[Long, Array[IntDummyVector]]]
                case "IntIntVector" =>
                  ObjCache(tpeStr) = new FastHashMap[Long, Array[IntIntVector]]
                  typeOf[FastHashMap[Long, Array[IntIntVector]]]
                case "IntLongVector" =>
                  ObjCache(tpeStr) = new FastHashMap[Long, Array[IntLongVector]]
                  typeOf[FastHashMap[Long, Array[IntLongVector]]]
                case "IntFloatVector" =>
                  ObjCache(tpeStr) = new FastHashMap[Long, Array[IntFloatVector]]
                  typeOf[FastHashMap[Long, Array[IntFloatVector]]]
                case "IntDoubleVector" =>
                  ObjCache(tpeStr) = new FastHashMap[Long, Array[IntDoubleVector]]
                  typeOf[FastHashMap[Long, Array[IntDoubleVector]]]
                case "LongDummyVector" =>
                  ObjCache(tpeStr) = new FastHashMap[Long, Array[LongDummyVector]]
                  typeOf[FastHashMap[Long, Array[LongDummyVector]]]
                case "LongIntVector" =>
                  ObjCache(tpeStr) = new FastHashMap[Long, Array[LongIntVector]]
                  typeOf[FastHashMap[Long, Array[LongIntVector]]]
                case "LongLongVector" =>
                  ObjCache(tpeStr) = new FastHashMap[Long, Array[LongLongVector]]
                  typeOf[FastHashMap[Long, Array[LongLongVector]]]
                case "LongFloatVector" =>
                  ObjCache(tpeStr) = new FastHashMap[Long, Array[LongFloatVector]]
                  typeOf[FastHashMap[Long, Array[LongFloatVector]]]
                case "LongDoubleVector" =>
                  ObjCache(tpeStr) = new FastHashMap[Long, Array[LongDoubleVector]]
                  typeOf[FastHashMap[Long, Array[LongDoubleVector]]]
                case "NeighN" =>
                  ObjCache(tpeStr) = new FastHashMap[Long, Array[NeighN]]
                  typeOf[FastHashMap[Long, Array[NeighN]]]
                case "NeighNW" =>
                  ObjCache(tpeStr) = new FastHashMap[Long, Array[NeighNW]]
                  typeOf[FastHashMap[Long, Array[NeighNW]]]
                case "NeighTN" =>
                  ObjCache(tpeStr) = new FastHashMap[Long, Array[NeighTN]]
                  typeOf[FastHashMap[Long, Array[NeighTN]]]
                case "NeighTNW" =>
                  ObjCache(tpeStr) = new FastHashMap[Long, Array[NeighTNW]]
                  typeOf[FastHashMap[Long, Array[NeighTNW]]]
              }
            case "VertexId" =>
              value match {
                case "Boolean" =>
                  ObjCache(tpeStr) = new FastHashMap[VertexId, Array[Boolean]]
                  typeOf[FastHashMap[VertexId, Array[Boolean]]]
                case "Char" =>
                  ObjCache(tpeStr) = new FastHashMap[VertexId, Array[Char]]
                  typeOf[FastHashMap[VertexId, Array[Char]]]
                case "Byte" =>
                  ObjCache(tpeStr) = new FastHashMap[VertexId, Array[Byte]]
                  typeOf[FastHashMap[VertexId, Array[Byte]]]
                case "Short" =>
                  ObjCache(tpeStr) = new FastHashMap[VertexId, Array[Short]]
                  typeOf[FastHashMap[VertexId, Array[Short]]]
                case "Int" =>
                  ObjCache(tpeStr) = new FastHashMap[VertexId, Array[Int]]
                  typeOf[FastHashMap[VertexId, Array[Int]]]
                case "Long" =>
                  ObjCache(tpeStr) = new FastHashMap[VertexId, Array[Long]]
                  typeOf[FastHashMap[VertexId, Array[Long]]]
                case "Float" =>
                  ObjCache(tpeStr) = new FastHashMap[VertexId, Array[Float]]
                  typeOf[FastHashMap[VertexId, Array[Float]]]
                case "Double" =>
                  ObjCache(tpeStr) = new FastHashMap[VertexId, Array[Double]]
                  typeOf[FastHashMap[VertexId, Array[Double]]]
                case "String" =>
                  ObjCache(tpeStr) = new FastHashMap[VertexId, Array[String]]
                  typeOf[FastHashMap[VertexId, Array[String]]]
                case "IntDummyVector" =>
                  ObjCache(tpeStr) = new FastHashMap[VertexId, Array[IntDummyVector]]
                  typeOf[FastHashMap[VertexId, Array[IntDummyVector]]]
                case "IntIntVector" =>
                  ObjCache(tpeStr) = new FastHashMap[VertexId, Array[IntIntVector]]
                  typeOf[FastHashMap[VertexId, Array[IntIntVector]]]
                case "IntLongVector" =>
                  ObjCache(tpeStr) = new FastHashMap[VertexId, Array[IntLongVector]]
                  typeOf[FastHashMap[VertexId, Array[IntLongVector]]]
                case "IntFloatVector" =>
                  ObjCache(tpeStr) = new FastHashMap[VertexId, Array[IntFloatVector]]
                  typeOf[FastHashMap[VertexId, Array[IntFloatVector]]]
                case "IntDoubleVector" =>
                  ObjCache(tpeStr) = new FastHashMap[VertexId, Array[IntDoubleVector]]
                  typeOf[FastHashMap[VertexId, Array[IntDoubleVector]]]
                case "LongDummyVector" =>
                  ObjCache(tpeStr) = new FastHashMap[VertexId, Array[LongDummyVector]]
                  typeOf[FastHashMap[VertexId, Array[LongDummyVector]]]
                case "LongIntVector" =>
                  ObjCache(tpeStr) = new FastHashMap[VertexId, Array[LongIntVector]]
                  typeOf[FastHashMap[VertexId, Array[LongIntVector]]]
                case "LongLongVector" =>
                  ObjCache(tpeStr) = new FastHashMap[VertexId, Array[LongLongVector]]
                  typeOf[FastHashMap[VertexId, Array[LongLongVector]]]
                case "LongFloatVector" =>
                  ObjCache(tpeStr) = new FastHashMap[VertexId, Array[LongFloatVector]]
                  typeOf[FastHashMap[VertexId, Array[LongFloatVector]]]
                case "LongDoubleVector" =>
                  ObjCache(tpeStr) = new FastHashMap[VertexId, Array[LongDoubleVector]]
                  typeOf[FastHashMap[VertexId, Array[LongDoubleVector]]]
                case "NeighN" =>
                  ObjCache(tpeStr) = new FastHashMap[VertexId, Array[NeighN]]
                  typeOf[FastHashMap[VertexId, Array[NeighN]]]
                case "NeighNW" =>
                  ObjCache(tpeStr) = new FastHashMap[VertexId, Array[NeighNW]]
                  typeOf[FastHashMap[VertexId, Array[NeighNW]]]
                case "NeighTN" =>
                  ObjCache(tpeStr) = new FastHashMap[VertexId, Array[NeighTN]]
                  typeOf[FastHashMap[VertexId, Array[NeighTN]]]
                case "NeighTNW" =>
                  ObjCache(tpeStr) = new FastHashMap[VertexId, Array[NeighTNW]]
                  typeOf[FastHashMap[VertexId, Array[NeighTNW]]]
              }
          }
        case UnimiMap(o, v) if o.split("\\.").last.equalsIgnoreCase("Int2ObjectOpenHashMap") =>
          val value = v.split("\\.").last
          value match {
            case "String" => typeOf[Int2ObjectOpenHashMap[String]]
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
            case "NeighN" => typeOf[Int2ObjectOpenHashMap[NeighN]]
            case "NeighNW" => typeOf[Int2ObjectOpenHashMap[NeighNW]]
            case "NeighTN" => typeOf[Int2ObjectOpenHashMap[NeighTN]]
            case "NeighTNW" => typeOf[Int2ObjectOpenHashMap[NeighTNW]]
          }
        case UnimiMap(o, v) if o.split("\\.").last.equalsIgnoreCase("Long2ObjectOpenHashMap") =>
          val value = v.split("\\.").last
          value match {
            case "String" => typeOf[Long2ObjectOpenHashMap[String]]
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
            case "NeighN" => typeOf[Long2ObjectOpenHashMap[NeighN]]
            case "NeighNW" => typeOf[Long2ObjectOpenHashMap[NeighNW]]
            case "NeighTN" => typeOf[Long2ObjectOpenHashMap[NeighTN]]
            case "NeighTNW" => typeOf[Long2ObjectOpenHashMap[NeighTNW]]
          }
        case UnimiMapArray(o, v) if o.split("\\.").last.equalsIgnoreCase("Int2ObjectOpenHashMap") =>
          val value = v.split("\\.").last
          value match {
            case "String" => typeOf[Int2ObjectOpenHashMap[Array[String]]]
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
            case "NeighN" => typeOf[Int2ObjectOpenHashMap[Array[NeighN]]]
            case "NeighNW" => typeOf[Int2ObjectOpenHashMap[Array[NeighNW]]]
            case "NeighTN" => typeOf[Int2ObjectOpenHashMap[Array[NeighTN]]]
            case "NeighTNW" => typeOf[Int2ObjectOpenHashMap[Array[NeighTNW]]]
          }
        case UnimiMapArray(o, v) if o.split("\\.").last.equalsIgnoreCase("Long2ObjectOpenHashMap") =>
          val value = v.split("\\.").last
          value match {
            case "String" => typeOf[Long2ObjectOpenHashMap[Array[String]]]
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
            case "NeighN" => typeOf[Long2ObjectOpenHashMap[Array[NeighN]]]
            case "NeighNW" => typeOf[Long2ObjectOpenHashMap[Array[NeighNW]]]
            case "NeighTN" => typeOf[Long2ObjectOpenHashMap[Array[NeighTN]]]
            case "NeighTNW" => typeOf[Long2ObjectOpenHashMap[Array[NeighTNW]]]
          }
      }
    } catch {
      case _: Exception =>
        val tree = tb.parse(s"scala.reflect.runtime.universe.typeOf[$tpeStr] -> new $tpeStr")
        val tpe_Obj = tb.eval(tree).asInstanceOf[(Type, Any)]
        val tpe = tpe_Obj._1
        ObjCache(tpeStr) = tpe_Obj._2

        tpe
    }
  }
}
