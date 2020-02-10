package com.tencent.angel.graph.utils

import java.util

import it.unimi.dsi.fastutil.ints._
import it.unimi.dsi.fastutil.longs._
import com.tencent.angel.ml.math2.vector.Vector

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox

object ReflectUtils {
  private val mirror = runtimeMirror(ReflectUtils.getClass.getClassLoader)
  private val tb = mirror.mkToolBox()

  private val typeCache = new util.HashMap[String, Type]()

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

  def getType[T: TypeTag](obj: T): Type = typeOf[T]

  def getType(typeStr: String): Type = typeCache.synchronized {
    if (typeCache.containsKey(typeStr)) {
      typeCache.get(typeStr)
    } else {
      val parsed = tb.parse(s"scala.reflect.runtime.universe.typeOf[$typeStr]")
      val typ = tb.eval(parsed).asInstanceOf[Type]
      typeCache.put(typeStr, typ)
      typ
    }
  }

  def isPrimitive(tpe: Type): Boolean = tpe match {
    case t if t =:= typeOf[Boolean] => true
    case t if t =:= typeOf[Char] => true
    case t if t =:= typeOf[Byte] => true
    case t if t =:= typeOf[Short] => true
    case t if t =:= typeOf[Int] => true
    case t if t =:= typeOf[Long] => true
    case t if t =:= typeOf[Float] => true
    case t if t =:= typeOf[Double] => true
    case t if t =:= typeOf[String] => true
    case _ => false
  }

  def isPrimitiveArray(tpe: Type): Boolean = tpe match {
    case t if t.weak_<:<(typeOf[Array[_]]) && isPrimitive(tpe.typeArgs.head) => true
    case _ => false
  }

  def isIntKeyFastMap(tpe: Type): Boolean = tpe match {
    case t if t =:= typeOf[Int2BooleanOpenHashMap] => true
    case t if t =:= typeOf[Int2CharOpenHashMap] => true
    case t if t =:= typeOf[Int2ByteOpenHashMap] => true
    case t if t =:= typeOf[Int2ShortOpenHashMap] => true
    case t if t =:= typeOf[Int2IntOpenHashMap] => true
    case t if t =:= typeOf[Int2LongOpenHashMap] => true
    case t if t =:= typeOf[Int2FloatOpenHashMap] => true
    case t if t =:= typeOf[Int2DoubleOpenHashMap] => true
    case t if t.weak_<:<(typeOf[Int2ObjectOpenHashMap[_]]) => true
    case _ => false
  }

  def isLongKeyFastMap(tpe: Type): Boolean = tpe match {
    case t if t =:= typeOf[Long2BooleanOpenHashMap] => true
    case t if t =:= typeOf[Long2CharOpenHashMap] => true
    case t if t =:= typeOf[Long2ByteOpenHashMap] => true
    case t if t =:= typeOf[Long2ShortOpenHashMap] => true
    case t if t =:= typeOf[Long2IntOpenHashMap] => true
    case t if t =:= typeOf[Long2LongOpenHashMap] => true
    case t if t =:= typeOf[Long2FloatOpenHashMap] => true
    case t if t =:= typeOf[Long2DoubleOpenHashMap] => true
    case t if t.weak_<:<(typeOf[Long2ObjectOpenHashMap[_]]) => true
    case _ => false
  }

  def isFastMap(tpe: Type): Boolean = isIntKeyFastMap(tpe) || isLongKeyFastMap(tpe)

  def isVector(tpe: Type): Boolean = tpe.weak_<:<(typeOf[Vector])
}
