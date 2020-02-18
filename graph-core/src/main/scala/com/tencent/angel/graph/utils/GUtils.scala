package com.tencent.angel.graph.utils

import com.tencent.angel.common.Serialize
import com.tencent.angel.graph.VertexId
import com.tencent.angel.graph.core.data.GData
import com.tencent.angel.ml.math2.vector.Vector
import it.unimi.dsi.fastutil.ints._
import it.unimi.dsi.fastutil.longs._

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
        false
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
        false
      }
  }

  def getFastMapKeys(value: Any): Array[VertexId] = {
    typeOf[VertexId] match {
      case t if t =:= typeOf[Int] =>
        val (size, iterator) = value match {
          case v: Int2BooleanOpenHashMap => (v.size(), v.keySet().iterator())
          case v: Int2CharOpenHashMap => (v.size(), v.keySet().iterator())
          case v: Int2ByteOpenHashMap => (v.size(), v.keySet().iterator())
          case v: Int2ShortOpenHashMap => (v.size(), v.keySet().iterator())
          case v: Int2IntOpenHashMap => (v.size(), v.keySet().iterator())
          case v: Int2LongOpenHashMap => (v.size(), v.keySet().iterator())
          case v: Int2FloatOpenHashMap => (v.size(), v.keySet().iterator())
          case v: Int2DoubleOpenHashMap => (v.size(), v.keySet().iterator())
          case v: Int2ObjectOpenHashMap[_] => (v.size(), v.keySet().iterator())
          case _ =>
            throw new Exception("error type!")
        }
        Array.tabulate[VertexId](size)(_ => iterator.nextInt().asInstanceOf[VertexId])
      case t if t =:= typeOf[Long] =>
        val (size, iterator) = value match {
          case v: Long2BooleanOpenHashMap => (v.size(), v.keySet().iterator())
          case v: Long2CharOpenHashMap => (v.size(), v.keySet().iterator())
          case v: Long2ByteOpenHashMap => (v.size(), v.keySet().iterator())
          case v: Long2ShortOpenHashMap => (v.size(), v.keySet().iterator())
          case v: Long2IntOpenHashMap => (v.size(), v.keySet().iterator())
          case v: Long2LongOpenHashMap => (v.size(), v.keySet().iterator())
          case v: Long2FloatOpenHashMap => (v.size(), v.keySet().iterator())
          case v: Long2DoubleOpenHashMap => (v.size(), v.keySet().iterator())
          case v: Long2ObjectOpenHashMap[_] => (v.size(), v.keySet().iterator())
          case _ =>
            throw new Exception("error type!")
        }
        Array.tabulate[VertexId](size)(_ => iterator.nextLong().asInstanceOf[VertexId])
    }
  }

  def isFastMap(tpe: Type): Boolean = isIntKeyFastMap(tpe) || isLongKeyFastMap(tpe)

  def isSerIntKeyMap(tpe: Type): Boolean = {
    val t1 = tpe.typeSymbol.asClass.toType
    val t2 = typeOf[Int2ObjectOpenHashMap[_]].typeSymbol.asClass.toType
    t1 =:= t2 && tpe.typeArgs.head <:< typeOf[Serialize]
  }

  def isSerLongKeyMap(tpe: Type): Boolean = {
    val t1 = tpe.typeSymbol.asClass.toType
    val t2 = typeOf[Long2ObjectOpenHashMap[_]].typeSymbol.asClass.toType
    t1 =:= t2 && tpe.typeArgs.head <:< typeOf[Serialize]
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
}