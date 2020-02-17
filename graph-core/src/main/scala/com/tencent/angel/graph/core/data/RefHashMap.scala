package com.tencent.angel.graph.core.data


import it.unimi.dsi.fastutil.HashCommon
import it.unimi.dsi.fastutil.ints._
import it.unimi.dsi.fastutil.longs._

import scala.reflect.runtime.universe._


class IntKeyRefHashMap[V: TypeTag](mask: Int, n: Int, containsNullKey: Boolean,
                                   private val keys: Array[Int], private val values: Array[V]) {
  private val tpe = typeOf[V]

  private def getValues[T]: Array[T] = values.asInstanceOf[Array[T]]

  def find(key: Int): Int = {
    if (key == 0) {
      if (containsNullKey) n else -(n + 1)
    } else {
      var pos = HashCommon.mix(key) & mask
      var curr = keys(pos)

      if (curr == key) {
        pos
      } else {
        while (curr != key && curr != 0) {
          pos = (pos + 1) & mask
          curr = keys(pos)
        }

        if (curr == key) pos else -(pos + 1)
      }
    }
  }

  def put(key: Int, value: V): this.type = {
    values(find(key)) = value

    this
  }

  def putAll(keys: Array[Int], valueArr: Array[V]): this.type = {
    keys.zip(valueArr).foreach { case (key, value) =>
      values(find(key)) = value
    }

    this
  }

  def putAll[U: TypeTag](map: U): this.type = {
    map match {
      case m: Int2BooleanOpenHashMap if tpe =:= typeOf[Boolean] =>
        val values: Array[Boolean] = getValues[Boolean]
        val iter = m.int2BooleanEntrySet().fastIterator()
        while (iter.hasNext) {
          val entry = iter.next()
          values(find(entry.getIntKey)) = entry.getBooleanValue
        }
      case m: Int2ByteOpenHashMap if tpe =:= typeOf[Byte] =>
        val values: Array[Byte] = getValues[Byte]
        val iter = m.int2ByteEntrySet().fastIterator()
        while (iter.hasNext) {
          val entry = iter.next()
          values(find(entry.getIntKey)) = entry.getByteValue
        }
      case m: Int2CharOpenHashMap if tpe =:= typeOf[Char] =>
        val values: Array[Char] = getValues[Char]
        val iter = m.int2CharEntrySet().fastIterator()
        while (iter.hasNext) {
          val entry = iter.next()
          values(find(entry.getIntKey)) = entry.getCharValue
        }
      case m: Int2ShortOpenHashMap if tpe =:= typeOf[Short] =>
        val values: Array[Short] = getValues[Short]
        val iter = m.int2ShortEntrySet().fastIterator()
        while (iter.hasNext) {
          val entry = iter.next()
          values(find(entry.getIntKey)) = entry.getShortValue
        }
      case m: Int2IntOpenHashMap if tpe =:= typeOf[Int] =>
        val values: Array[Int] = getValues[Int]
        val iter = m.int2IntEntrySet().fastIterator()
        while (iter.hasNext) {
          val entry = iter.next()
          values(find(entry.getIntKey)) = entry.getIntValue
        }
      case m: Int2LongOpenHashMap if tpe =:= typeOf[Long] =>
        val values: Array[Long] = getValues[Long]
        val iter = m.int2LongEntrySet().fastIterator()
        while (iter.hasNext) {
          val entry = iter.next()
          values(find(entry.getIntKey)) = entry.getLongValue
        }
      case m: Int2FloatOpenHashMap if tpe =:= typeOf[Float] =>
        val values: Array[Float] = getValues[Float]
        val iter = m.int2FloatEntrySet().fastIterator()
        while (iter.hasNext) {
          val entry = iter.next()
          values(find(entry.getIntKey)) = entry.getFloatValue
        }
      case m: Int2DoubleOpenHashMap if tpe =:= typeOf[Double] =>
        val values: Array[Double] = getValues[Double]
        val iter = m.int2DoubleEntrySet().fastIterator()
        while (iter.hasNext) {
          val entry = iter.next()
          values(find(entry.getIntKey)) = entry.getDoubleValue
        }
      case m: Int2ObjectOpenHashMap[_] if typeOf[V] =:= typeOf[U].typeArgs.head =>
        val iter = m.int2ObjectEntrySet().fastIterator()
        while (iter.hasNext) {
          val entry = iter.next()
          values(find(entry.getIntKey)) = entry.getValue.asInstanceOf[V]
        }
    }

    this
  }

  def get(key: Int): V = {
    values(find(key))
  }

  def foreach(func: (Int, V) => Unit): Unit = {
    (0 until n).foreach { idx =>
      if (keys(idx) != 0) {
        func(keys(idx), values(idx))
      }
    }

    if (containsNullKey) {
      func(0, values(n))
    }
  }

  def foreachKey(func: Int => Unit): Unit = {
    (0 until n).foreach { idx =>
      if (keys(idx) != 0) {
        func(keys(idx))
      }
    }

    if (containsNullKey) {
      func(0)
    }
  }

  def foreachValue(func: V => Unit): Unit = {
    (0 until n).foreach { idx =>
      if (keys(idx) != 0) {
        func(values(idx))
      }
    }

    if (containsNullKey) {
      func(values(n))
    }
  }

  def entryIterator(): Iterator[(Int, V)] = {

    new Iterator[(Int, V)] {
      private var pos = 0

      override def hasNext: Boolean = {
        while (pos < n && keys(pos) == 0) {
          pos += 1
        }

        if (pos < n) {
          true
        } else if (pos == n && containsNullKey) {
          true
        } else {
          false
        }
      }

      override def next(): (Int, V) = {
        pos += 1
        keys(pos - 1) -> values(pos - 1)
      }
    }
  }

  def KeyIterator(): Iterator[Int] = {

    new Iterator[Int] {
      private var pos = 0

      override def hasNext: Boolean = {
        while (pos < n && keys(pos) == 0) {
          pos += 1
        }

        if (pos < n) {
          true
        } else if (pos == n && containsNullKey) {
          true
        } else {
          false
        }
      }

      override def next(): Int = {
        pos += 1
        keys(pos - 1)
      }
    }
  }

  def valueIterator(): Iterator[V] = {

    new Iterator[V] {
      private var pos = 0

      override def hasNext: Boolean = {
        while (pos < n && keys(pos) == 0) {
          pos += 1
        }

        if (pos < n) {
          true
        } else if (pos == n && containsNullKey) {
          true
        } else {
          false
        }
      }

      override def next(): V = {
        pos += 1
        values(pos - 1)
      }
    }
  }
}


class LongKeyRefHashMap[V: TypeTag](mask: Int, n: Int, containsNullKey: Boolean,
                                    private val keys: Array[Long], private val values: Array[V]) {
  private val tpe = typeOf[V]

  private def getValues[T]: Array[T] = values.asInstanceOf[Array[T]]

  def find(key: Long): Int = {
    if (key == 0) {
      if (containsNullKey) n else -(n + 1)
    } else {
      var pos = (HashCommon.mix(key) & mask).toInt
      var curr = keys(pos)

      if (curr == key) {
        pos
      } else {
        while (curr != key && curr != 0) {
          pos = (pos + 1) & mask
          curr = keys(pos)
        }

        if (curr == key) pos else -(pos + 1)
      }
    }
  }

  def put(key: Long, value: V): this.type = {
    values(find(key)) = value

    this
  }

  def putAll(keys: Array[Long], valueArr: Array[V]): this.type = {
    keys.zip(valueArr).foreach { case (key, value) =>
      values(find(key)) = value
    }

    this
  }

  def putAll[U: TypeTag](map: U): this.type = {
    map match {
      case m: Long2BooleanOpenHashMap if tpe =:= typeOf[Boolean] =>
        val values: Array[Boolean] = getValues[Boolean]
        val iter = m.long2BooleanEntrySet().fastIterator()
        while (iter.hasNext) {
          val entry = iter.next()
          values(find(entry.getLongKey)) = entry.getBooleanValue
        }
      case m: Long2ByteOpenHashMap if tpe =:= typeOf[Byte] =>
        val values: Array[Byte] = getValues[Byte]
        val iter = m.long2ByteEntrySet().fastIterator()
        while (iter.hasNext) {
          val entry = iter.next()
          values(find(entry.getLongKey)) = entry.getByteValue
        }
      case m: Long2CharOpenHashMap if tpe =:= typeOf[Char] =>
        val values: Array[Char] = getValues[Char]
        val iter = m.long2CharEntrySet().fastIterator()
        while (iter.hasNext) {
          val entry = iter.next()
          values(find(entry.getLongKey)) = entry.getCharValue
        }
      case m: Long2ShortOpenHashMap if tpe =:= typeOf[Short] =>
        val values: Array[Short] = getValues[Short]
        val iter = m.long2ShortEntrySet().fastIterator()
        while (iter.hasNext) {
          val entry = iter.next()
          values(find(entry.getLongKey)) = entry.getShortValue
        }
      case m: Long2IntOpenHashMap if tpe =:= typeOf[Int] =>
        val values: Array[Int] = getValues[Int]
        val iter = m.long2IntEntrySet().fastIterator()
        while (iter.hasNext) {
          val entry = iter.next()
          values(find(entry.getLongKey)) = entry.getIntValue
        }
      case m: Long2LongOpenHashMap if tpe =:= typeOf[Long] =>
        val values: Array[Long] = getValues[Long]
        val iter = m.long2LongEntrySet().fastIterator()
        while (iter.hasNext) {
          val entry = iter.next()
          values(find(entry.getLongKey)) = entry.getLongValue
        }
      case m: Long2FloatOpenHashMap if tpe =:= typeOf[Float] =>
        val values: Array[Float] = getValues[Float]
        val iter = m.long2FloatEntrySet().fastIterator()
        while (iter.hasNext) {
          val entry = iter.next()
          values(find(entry.getLongKey)) = entry.getFloatValue
        }
      case m: Long2DoubleOpenHashMap if tpe =:= typeOf[Double] =>
        val values: Array[Double] = getValues[Double]
        val iter = m.long2DoubleEntrySet().fastIterator()
        while (iter.hasNext) {
          val entry = iter.next()
          values(find(entry.getLongKey)) = entry.getDoubleValue
        }
      case m: Long2ObjectOpenHashMap[_] if typeOf[V] =:= typeOf[U].typeArgs.head =>
        val iter = m.long2ObjectEntrySet().fastIterator()
        while (iter.hasNext) {
          val entry = iter.next()
          values(find(entry.getLongKey)) = entry.getValue.asInstanceOf[V]
        }
    }

    this
  }

  def get(key: Long): V = {
    values(find(key))
  }

  def foreach(func: (Long, V) => Unit): Unit = {
    (0 until n).foreach { idx =>
      if (keys(idx) != 0) {
        func(keys(idx), values(idx))
      }
    }

    if (containsNullKey) {
      func(0L, values(n))
    }
  }

  def foreachKey(func: Long => Unit): Unit = {
    (0 until n).foreach { idx =>
      if (keys(idx) != 0) {
        func(keys(idx))
      }
    }

    if (containsNullKey) {
      func(0L)
    }
  }

  def foreachValue(func: V => Unit): Unit = {
    (0 until n).foreach { idx =>
      if (keys(idx) != 0) {
        func(values(idx))
      }
    }

    if (containsNullKey) {
      func(values(n))
    }
  }

  def entryIterator(): Iterator[(Long, V)] = {

    new Iterator[(Long, V)] {
      private var pos = 0

      override def hasNext: Boolean = {
        while (pos < n && keys(pos) == 0) {
          pos += 1
        }

        if (pos < n) {
          true
        } else if (pos == n && containsNullKey) {
          true
        } else {
          false
        }
      }

      override def next(): (Long, V) = {
        pos += 1
        keys(pos - 1) -> values(pos - 1)
      }
    }
  }

  def KeyIterator(): Iterator[Long] = {

    new Iterator[Long] {
      private var pos = 0

      override def hasNext: Boolean = {
        while (pos < n && keys(pos) == 0) {
          pos += 1
        }

        if (pos < n) {
          true
        } else if (pos == n && containsNullKey) {
          true
        } else {
          false
        }
      }

      override def next(): Long = {
        pos += 1
        keys(pos - 1)
      }
    }
  }

  def valueIterator(): Iterator[V] = {

    new Iterator[V] {
      private var pos = 0

      override def hasNext: Boolean = {
        while (pos < n && keys(pos) == 0) {
          pos += 1
        }

        if (pos < n) {
          true
        } else if (pos == n && containsNullKey) {
          true
        } else {
          false
        }
      }

      override def next(): V = {
        pos += 1
        values(pos - 1)
      }
    }
  }
}
