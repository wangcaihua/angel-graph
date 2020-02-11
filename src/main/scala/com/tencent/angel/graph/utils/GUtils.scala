package com.tencent.angel.graph.utils

import it.unimi.dsi.fastutil.ints._
import it.unimi.dsi.fastutil.longs._
import com.tencent.angel.ml.math2.vector.Vector

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

  def getIntKeys(value: Any): Array[Int] = {
    val (size, iterator) = value match {
      case v: Int2BooleanOpenHashMap => (v.size(), v.keySet().iterator())
      case v: Int2CharOpenHashMap =>  (v.size(), v.keySet().iterator())
      case v: Int2ByteOpenHashMap =>  (v.size(), v.keySet().iterator())
      case v: Int2ShortOpenHashMap =>  (v.size(), v.keySet().iterator())
      case v: Int2IntOpenHashMap =>  (v.size(), v.keySet().iterator())
      case v: Int2LongOpenHashMap =>  (v.size(), v.keySet().iterator())
      case v: Int2FloatOpenHashMap =>  (v.size(), v.keySet().iterator())
      case v: Int2DoubleOpenHashMap =>  (v.size(), v.keySet().iterator())
      case v: Int2ObjectOpenHashMap[_] =>  (v.size(), v.keySet().iterator())
      case _ =>
        throw new Exception("error type!")
    }

    Array.tabulate[Int](size)(_ => iterator.nextInt())
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

  def getLongKeys(value: Any): Array[Long] = {
    val (size, iterator) = value match {
      case v: Long2BooleanOpenHashMap => (v.size(), v.keySet().iterator())
      case v: Long2CharOpenHashMap =>  (v.size(), v.keySet().iterator())
      case v: Long2ByteOpenHashMap =>  (v.size(), v.keySet().iterator())
      case v: Long2ShortOpenHashMap =>  (v.size(), v.keySet().iterator())
      case v: Long2IntOpenHashMap =>  (v.size(), v.keySet().iterator())
      case v: Long2LongOpenHashMap =>  (v.size(), v.keySet().iterator())
      case v: Long2FloatOpenHashMap =>  (v.size(), v.keySet().iterator())
      case v: Long2DoubleOpenHashMap =>  (v.size(), v.keySet().iterator())
      case v: Long2ObjectOpenHashMap[_] =>  (v.size(), v.keySet().iterator())
      case _ =>
        throw new Exception("error type!")
    }

    Array.tabulate[Long](size)(_ => iterator.nextLong())
  }

  def isFastMap(tpe: Type): Boolean = isIntKeyFastMap(tpe) || isLongKeyFastMap(tpe)

  def isVector(tpe: Type): Boolean = tpe.weak_<:<(typeOf[Vector])
}