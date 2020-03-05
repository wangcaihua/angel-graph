package com.tencent.angel.graph.utils

import com.tencent.angel.graph.VertexId
import com.tencent.angel.graph.core.data.GData
import com.tencent.angel.ml.math2.vector.Vector
import it.unimi.dsi.fastutil.ints._
import it.unimi.dsi.fastutil.longs._

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._


object GUtils {
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

  def isPrimitiveArray(tpe: Type): Boolean = {
    val outerType = tpe.typeSymbol.asClass.toType
    val arrType = typeOf[Array[_]].typeSymbol.asClass.toType
    outerType =:= arrType && isPrimitive(tpe.typeArgs.head)
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
    case t =>
      val t1 = t.typeSymbol.asClass.toType
      val t2 = typeOf[Int2ObjectOpenHashMap[_]].typeSymbol.asClass.toType
      if (t1 =:= t2) {
        true
      } else {
        val t3 = typeOf[FastHashMap[_, _]].typeSymbol.asClass.toType
        if (t1 =:= t3 && tpe.typeArgs.head =:= typeOf[Int]) {
          true
        } else {
          false
        }
      }
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
    case t =>
      val t1 = t.typeSymbol.asClass.toType
      val t2 = typeOf[Long2ObjectOpenHashMap[_]].typeSymbol.asClass.toType
      if (t1 =:= t2) {
        true
      } else {
        val t3 = typeOf[FastHashMap[_, _]].typeSymbol.asClass.toType
        if (t1 =:= t3 && tpe.typeArgs.head =:= typeOf[Long]) {
          true
        } else {
          false
        }
      }
  }

  def getFastMapKeys(value: Any): Array[VertexId] = {
    typeOf[VertexId] match {
      case t if t =:= typeOf[Int] =>
        val array = value match {
          case v: FastHashMap[_, _] if v.keyTag == classOf[Int] =>
            v.asInstanceOf[FastHashMap[Int, _]].keyArray
          case v: Int2BooleanOpenHashMap => v.keySet().toIntArray
          case v: Int2CharOpenHashMap => v.keySet().toIntArray
          case v: Int2ByteOpenHashMap => v.keySet().toIntArray
          case v: Int2ShortOpenHashMap => v.keySet().toIntArray
          case v: Int2IntOpenHashMap => v.keySet().toIntArray
          case v: Int2LongOpenHashMap => v.keySet().toIntArray
          case v: Int2FloatOpenHashMap => v.keySet().toIntArray
          case v: Int2DoubleOpenHashMap => v.keySet().toIntArray
          case v: Int2ObjectOpenHashMap[_] => v.keySet().toIntArray
          case _ =>
            throw new Exception("error type!")
        }
        array.asInstanceOf[Array[VertexId]]
      case t if t =:= typeOf[Long] =>
        val array = value match {
          case v: FastHashMap[_, _] if v.keyTag == classOf[Long] =>
            v.asInstanceOf[FastHashMap[Long, _]].keyArray
          case v: Long2BooleanOpenHashMap => v.keySet().toLongArray
          case v: Long2CharOpenHashMap => v.keySet().toLongArray
          case v: Long2ByteOpenHashMap => v.keySet().toLongArray
          case v: Long2ShortOpenHashMap => v.keySet().toLongArray
          case v: Long2IntOpenHashMap => v.keySet().toLongArray
          case v: Long2LongOpenHashMap => v.keySet().toLongArray
          case v: Long2FloatOpenHashMap => v.keySet().toLongArray
          case v: Long2DoubleOpenHashMap => v.keySet().toLongArray
          case v: Long2ObjectOpenHashMap[_] => v.keySet().toLongArray
          case _ =>
            throw new Exception("error type!")
        }
        array.asInstanceOf[Array[VertexId]]
    }
  }

  def isFastMap(tpe: Type): Boolean = isIntKeyFastMap(tpe) || isLongKeyFastMap(tpe)

  def isSerIntKeyMap(tpe: Type): Boolean = {
    val t1 = tpe.typeSymbol.asClass.toType
    val t2 = typeOf[Int2ObjectOpenHashMap[_]].typeSymbol.asClass.toType
    val t3 = typeOf[FastHashMap[Int, _]].typeSymbol.asClass.toType
    if (t1 =:= t2) {
      tpe.typeArgs.head <:< typeOf[GData] || tpe.typeArgs.head <:< typeOf[Serializable] ||
        tpe.typeArgs.head <:< typeOf[Vector]
    } else if (t1 =:= t3) {
      val kt = tpe.typeArgs.head
      val vt = tpe.typeArgs(1)
      kt =:= typeOf[Int] && (vt <:< typeOf[GData] || vt <:< typeOf[Serializable] || vt <:< typeOf[Vector])
    } else {
      false
    }
  }

  def isSerLongKeyMap(tpe: Type): Boolean = {
    val t1 = tpe.typeSymbol.asClass.toType
    val t2 = typeOf[Long2ObjectOpenHashMap[_]].typeSymbol.asClass.toType
    val t3 = typeOf[FastHashMap[Long, _]].typeSymbol.asClass.toType

    if (t1 =:= t2) {
      tpe.typeArgs.head <:< typeOf[GData] || tpe.typeArgs.head <:< typeOf[Serializable] ||
        tpe.typeArgs.head <:< typeOf[Vector]
    } else if (t1 =:= t3) {
      val kt = tpe.typeArgs.head
      val vt = tpe.typeArgs(1)
      kt =:= typeOf[Long] && (vt <:< typeOf[GData] || vt <:< typeOf[Serializable] || vt <:< typeOf[Vector])
    } else {
      false
    }
  }

  def isSerFastHashMap(tpe: Type): Boolean = {
    val t1 = tpe.typeSymbol.asClass.toType
    val t2 = typeOf[FastHashMap[_, _]].typeSymbol.asClass.toType
    t1 =:= t2
  }

  def isVector(tpe: Type): Boolean = tpe <:< typeOf[Vector]

  def paramCheck(tpe: Type): Boolean = {
    tpe match {
      case t if GUtils.isPrimitive(t) => true
      case t if GUtils.isPrimitiveArray(t) => true
      case t if GUtils.isFastMap(t) => true
      case t => t <:< typeOf[GData] && t.typeSymbol.asClass.isCaseClass
    }
  }

  def mergeArray[V: ClassTag](arr1: Array[V], arr2: Array[V]): Array[V] = {
    if ((arr1 == null || arr1.isEmpty) && (arr2 == null || arr2.isEmpty)) {
      Array.empty[V]
    } else if (arr1 == null || arr1.isEmpty) {
      arr2
    } else if (arr2 == null || arr2.isEmpty) {
      arr1
    } else {
      val res = new Array[V](arr1.length + arr2.length)

      Array.copy(arr1, 0, res, 0, arr1.length)
      Array.copy(arr2, 0, res, arr1.length, arr2.length)

      res
    }
  }
}