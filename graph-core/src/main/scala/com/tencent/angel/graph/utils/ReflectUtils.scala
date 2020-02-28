package com.tencent.angel.graph.utils

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox


object ReflectUtils {
  private val mirror = runtimeMirror(ReflectUtils.getClass.getClassLoader)
  private val tb = mirror.mkToolBox()

  private val ObjCache= new mutable.HashMap[String, Any]()

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
      ObjCache(tpeString).asInstanceOf[FastHashMap[_, _]].empty()
    } else {
      val parsed = tb.parse(s"new $tpeString")
      val inst = tb.eval(parsed).asInstanceOf[FastHashMap[_, _]]
      ObjCache(tpeString) = inst
      inst.empty()
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
}
