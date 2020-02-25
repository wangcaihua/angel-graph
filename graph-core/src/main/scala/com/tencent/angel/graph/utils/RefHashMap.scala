package com.tencent.angel.graph.utils

import scala.reflect._
import scala.{specialized => spec}

class RefHashMap[@spec(Int, Long) K: ClassTag, V: ClassTag](mask: Int, n: Int, containsNullKey: Boolean,
                                                            private val keys: Array[K], private val values: Array[V]) {

  import com.tencent.angel.graph.utils.HashCommon._

  private var size: Int = 0
  private val bitSet = new BitSet(keys.length)

  private lazy val nullKey: K = classTag[K] match {
    case kt if kt == ClassTag.Int => 0.asInstanceOf[K]
    case kt if kt == ClassTag.Long => 0L.asInstanceOf[K]
  }

  private lazy val defaultValue: V = classTag[V] match {
    case vt if vt == ClassTag.Boolean => false.asInstanceOf[V]
    case vt if vt == ClassTag.Char => Char.MinValue.asInstanceOf[V]
    case vt if vt == ClassTag.Byte => 0.toByte.asInstanceOf[V]
    case vt if vt == ClassTag.Short => 0.toShort.asInstanceOf[V]
    case vt if vt == ClassTag.Int => 0.asInstanceOf[V]
    case vt if vt == ClassTag.Long => 0.toLong.asInstanceOf[V]
    case vt if vt == ClassTag.Float => 0.toFloat.asInstanceOf[V]
    case vt if vt == ClassTag.Double => 0.toDouble.asInstanceOf[V]
    case _ => null.asInstanceOf[V]
  }

  private lazy val hash: Hasher[K] = classTag[K] match {
    case kt if kt == ClassTag.Long =>
      (new LongHasher).asInstanceOf[Hasher[K]]
    case kt if kt == ClassTag.Int =>
      (new IntHasher).asInstanceOf[Hasher[K]]
    case _ =>
      new Hasher[K]
  }

  private def getPos(key: K): Int = {
    if (key == 0) {
      if (containsNullKey) n else -(n + 1)
    } else {
      var pos = hash(key) & mask
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

  def containsKey(k: K): Boolean = {
    val pos = getPos(k)
    pos >= 0 && bitSet.get(pos)
  }

  /** Get the value for a given key */
  def apply(k: K): V = {
    val pos = getPos(k)

    if (pos >= 0 && bitSet.get(pos)) {
      values(pos)
    } else {
      throw new Exception(s"Cannot find key $k")
    }
  }

  def get(k: K): V = apply(k)

  /** Get the value for a given key, or returns elseValue if it doesn't exist. */
  def getOrElse(k: K, elseValue: V): V = {
    val pos = getPos(k)
    if (pos >= 0 && bitSet.get(pos)) values(pos) else elseValue
  }

  /** Set the value for a key */
  def update(k: K, v: V): this.type = {
    val pos = getPos(k)
    if (pos >= 0) {
      if (bitSet.get(pos)) {
        values(pos) = v
      } else {
        size += 1
        bitSet.set(pos)
        values(pos) = v
      }
    } else {
      throw new Exception("Cannot add a now key in RefHashMap!")
    }

    this
  }

  def put(k: K, v: V): this.type = update(k, v)

  def putAll(keyArr: Array[K], valueArr: Array[V]): this.type = {
    keyArr.zip(valueArr).foreach { case (key, value) => update(key, value) }

    this
  }

  def putMerge(k: K, v: V, mergeF: (V, V) => V): this.type = {
    val pos = getPos(k)
    if (pos >= 0) {
      if (bitSet.get(pos)) {
        values(pos) = mergeF(values(pos), v)
      } else {
        size += 1
        bitSet.set(pos)
        values(pos) = v
      }
    } else {
      throw new Exception("Cannot add a now key in RefHashMap!")
    }

    this
  }

  def changeValue(k: K, defaultValue: => V, mergeValue: V => V): V = {
    val pos = getPos(k)
    if (pos >= 0) {
      if (bitSet.get(pos)) {
        values(pos) = mergeValue(values(pos))
      } else {
        size += 1
        bitSet.set(pos)
        values(pos) = defaultValue
      }

      values(pos)
    } else {
      throw new Exception("Cannot add a now key in RefHashMap!")
    }
  }

  def remove(k: K): V = {
    val pos = getPos(k)

    if (pos >= 0 && bitSet.get(pos)) {
      val preValue = values(pos)
      values(pos) = defaultValue
      size -= 1
      bitSet.unset(pos)

      preValue
    } else {
      throw new Exception(s"Cannot find key $k")
    }
  }

  def clear(): this.type = {
    size = 0
    var pos = 0
    while (pos < n) {
      if (keys(pos) != 0 && bitSet.get(pos)) {
        values(pos) = defaultValue
      }
      pos += 1
    }

    if (containsNullKey) {
      values(n) = defaultValue
      bitSet.get(n)
    }

    bitSet.clear()

    this
  }

  def foreach(func: (K, V) => Unit): Unit = {
    var pos = 0
    while (pos < n) {
      if (keys(pos) != 0 && bitSet.get(pos)) {
        func(keys(pos), values(pos))
      }
      pos += 1
    }

    if (containsNullKey) {
      func(nullKey, values(n))
    }
  }

  def foreachKey(func: K => Unit): Unit = {
    var pos = 0
    while (pos < n) {
      if (keys(pos) != 0 && bitSet.get(pos)) {
        func(keys(pos))
      }
      pos += 1
    }

    if (containsNullKey && bitSet.get(n)) {
      func(nullKey)
    }
  }

  def foreachValue(func: V => Unit): Unit = {
    var pos = 0
    while (pos < n) {
      if (keys(pos) != 0 && bitSet.get(pos)) {
        func(values(pos))
      }
      pos += 1
    }

    if (containsNullKey && bitSet.get(n)) {
      func(values(n))
    }
  }

  def iterator: Iterator[(K, V)] = new Iterator[(K, V)] {
    private var pos = 0

    def hasNext: Boolean = {
      while (pos <= n && !bitSet.get(pos)) {
        pos = pos + 1
      }

      if (pos <= n) {
        true
      } else {
        false
      }
    }

    def next(): (K, V) = {
      pos += 1
      keys(pos - 1) -> values(pos - 1)
    }
  }

  def KeyIterator(): Iterator[K] = {

    new Iterator[K] {
      private var pos = 0

      override def hasNext: Boolean = {
        while (pos <= n && !bitSet.get(pos)) {
          pos = pos + 1
        }

        if (pos <= n) {
          true
        } else {
          false
        }
      }

      override def next(): K = {
        pos += 1
        keys(pos - 1)
      }
    }
  }

  def valueIterator(): Iterator[V] = {

    new Iterator[V] {
      private var pos = 0

      override def hasNext: Boolean = {
        while (pos <= n && !bitSet.get(pos)) {
          pos = pos + 1
        }

        if (pos <= n) {
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