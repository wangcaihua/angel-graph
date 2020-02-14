package com.tencent.angel.graph.core.psf.common

import com.tencent.angel.graph.utils.ReflectUtils
import it.unimi.dsi.fastutil.ints._
import it.unimi.dsi.fastutil.longs._

import scala.reflect.runtime.universe._
import scala.language.existentials

object Merger {
  def mergeFastMap(typ:Type, list: List[Any]): Any = {
    typ match {
      case t if t =:= typeOf[Int2BooleanOpenHashMap] =>
        val maps = list.asInstanceOf[List[Int2BooleanOpenHashMap]]
        val res = new Int2BooleanOpenHashMap(maps.map(_.size()).sum)
        maps.foreach(res.putAll)
        res
      case t if t =:= typeOf[Int2ByteOpenHashMap] =>
        val maps = list.asInstanceOf[List[Int2ByteOpenHashMap]]
        val res = new Int2ByteOpenHashMap(maps.map(_.size()).sum)
        maps.foreach(res.putAll)
        res
      case t if t =:= typeOf[Int2CharOpenHashMap] =>
        val maps = list.asInstanceOf[List[Int2CharOpenHashMap]]
        val res = new Int2CharOpenHashMap(maps.map(_.size()).sum)
        maps.foreach(res.putAll)
        res
      case t if t =:= typeOf[Int2ShortOpenHashMap] =>
        val maps = list.asInstanceOf[List[Int2ShortOpenHashMap]]
        val res = new Int2ShortOpenHashMap(maps.map(_.size()).sum)
        maps.foreach(res.putAll)
        res
      case t if t =:= typeOf[Int2IntOpenHashMap] =>
        val maps = list.asInstanceOf[List[Int2IntOpenHashMap]]
        val res = new Int2IntOpenHashMap(maps.map(_.size()).sum)
        maps.foreach(res.putAll)
        res
      case t if t =:= typeOf[Int2LongOpenHashMap] =>
        val maps = list.asInstanceOf[List[Int2LongOpenHashMap]]
        val res = new Int2LongOpenHashMap(maps.map(_.size()).sum)
        maps.foreach(res.putAll)
        res
      case t if t =:= typeOf[Int2FloatOpenHashMap] =>
        val maps = list.asInstanceOf[List[Int2FloatOpenHashMap]]
        val res = new Int2FloatOpenHashMap(maps.map(_.size()).sum)
        maps.foreach(res.putAll)
        res
      case t if t =:= typeOf[Int2DoubleOpenHashMap] =>
        val maps = list.asInstanceOf[List[Int2DoubleOpenHashMap]]
        val res = new Int2DoubleOpenHashMap(maps.map(_.size()).sum)
        maps.foreach(res.putAll)
        res
      case t if t.weak_<:<(typeOf[Int2ObjectOpenHashMap[_]]) =>
        val maps = list.asInstanceOf[List[Int2ObjectOpenHashMap[_]]]
        val size = maps.map(_.size()).sum
        val res = ReflectUtils.constructor(typ, typeOf[Int])(size)
          .asInstanceOf[Int2ObjectOpenHashMap[_]]

        val put = ReflectUtils.method(res, "put", typeOf[Int])

        maps.foreach{ map =>
          val iter = map.int2ObjectEntrySet().fastIterator()
          while (iter.hasNext) {
            val entry = iter.next()
            put(entry.getIntKey, entry.getValue)
          }
        }
        res

      case t if t =:= typeOf[Long2BooleanOpenHashMap] =>
        val maps = list.asInstanceOf[List[Long2BooleanOpenHashMap]]
        val res = new Long2BooleanOpenHashMap(maps.map(_.size()).sum)
        maps.foreach(res.putAll)
        res
      case t if t =:= typeOf[Long2ByteOpenHashMap] =>
        val maps = list.asInstanceOf[List[Long2ByteOpenHashMap]]
        val res = new Long2ByteOpenHashMap(maps.map(_.size()).sum)
        maps.foreach(res.putAll)
        res
      case t if t =:= typeOf[Long2CharOpenHashMap] =>
        val maps = list.asInstanceOf[List[Long2CharOpenHashMap]]
        val res = new Long2CharOpenHashMap(maps.map(_.size()).sum)
        maps.foreach(res.putAll)
        res
      case t if t =:= typeOf[Long2ShortOpenHashMap] =>
        val maps = list.asInstanceOf[List[Long2ShortOpenHashMap]]
        val res = new Long2ShortOpenHashMap(maps.map(_.size()).sum)
        maps.foreach(res.putAll)
        res
      case t if t =:= typeOf[Long2IntOpenHashMap] =>
        val maps = list.asInstanceOf[List[Long2IntOpenHashMap]]
        val res = new Long2IntOpenHashMap(maps.map(_.size()).sum)
        maps.foreach(res.putAll)
        res
      case t if t =:= typeOf[Long2LongOpenHashMap] =>
        val maps = list.asInstanceOf[List[Long2LongOpenHashMap]]
        val res = new Long2LongOpenHashMap(maps.map(_.size()).sum)
        maps.foreach(res.putAll)
        res
      case t if t =:= typeOf[Long2FloatOpenHashMap] =>
        val maps = list.asInstanceOf[List[Long2FloatOpenHashMap]]
        val res = new Long2FloatOpenHashMap(maps.map(_.size()).sum)
        maps.foreach(res.putAll)
        res
      case t if t =:= typeOf[Long2DoubleOpenHashMap] =>
        val maps = list.asInstanceOf[List[Long2DoubleOpenHashMap]]
        val res = new Long2DoubleOpenHashMap(maps.map(_.size()).sum)
        maps.foreach(res.putAll)
        res
      case t if t.weak_<:<(typeOf[Long2ObjectOpenHashMap[_]]) =>
        val maps = list.asInstanceOf[List[Long2ObjectOpenHashMap[_]]]
        val size = maps.map(_.size()).sum
        val res = ReflectUtils.constructor(typ, typeOf[Int])(size)
          .asInstanceOf[Long2ObjectOpenHashMap[_]]

        val put = ReflectUtils.method(res, "put", typeOf[Long])

        maps.foreach{ map =>
          val iter = map.long2ObjectEntrySet().fastIterator()
          while (iter.hasNext) {
            val entry = iter.next()
            put(entry.getLongKey, entry.getValue)
          }
        }

        res
      case _ =>
        throw new Exception("the in put type is not primitive map!")
    }
  }

}