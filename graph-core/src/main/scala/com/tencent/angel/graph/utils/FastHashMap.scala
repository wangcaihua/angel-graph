package com.tencent.angel.graph.utils


import scala.reflect._
import scala.{specialized => spec}


class FastHashMap[@spec(Int, Long) K: ClassTag, V: ClassTag](expected: Int, f: Float)
  extends Serializable {

  import com.tencent.angel.graph.utils.HashCommon._

  private var keys: Array[K] = _
  private var values: Array[V] = _
  private var containsNullKey: Boolean = false
  private var n: Int = arraySize(expected, f)
  private var mask: Int = n - 1
  private var maxFill: Int = calMaxFill(n, f)
  var size: Int = 0

  private val keyTag = classTag[K]
  private lazy val nullKey: K = keyTag match {
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
  }

  keys = new Array[K](n + 1)
  values = new Array[V](n + 1)

  private lazy val hash: Hasher[K] = {
    if (keyTag == ClassTag.Long) {
      (new LongHasher).asInstanceOf[Hasher[K]]
    } else if (keyTag == ClassTag.Int) {
      (new IntHasher).asInstanceOf[Hasher[K]]
    } else {
      new Hasher[K]
    }
  }

  def this(initCapacity: Int) = this(initCapacity, HashCommon.DEFAULT_LOAD_FACTOR)

  def this() = this(HashCommon.DEFAULT_INITIAL_SIZE, HashCommon.DEFAULT_LOAD_FACTOR)

  private def realSize: Int = if (containsNullKey) size - 1 else size

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

  private def rehash(newN: Int) {
    val newMask: Int = newN - 1 // Note that this is used by the hashing macro
    val newKeys: Array[K] = new Array[K](newN + 1)
    val newValues: Array[V] = new Array[V](newN + 1)

    var (i: Int, pos: Int, j: Int) = (n - 1, 0, realSize)
    if (containsNullKey) {
      newValues(newN) = values(n)
    }

    while (j != 0) {
      while (keys(i) == 0) {
        i -= 1
      }

      pos = hash(keys(i)) & newMask
      while (newKeys(pos) != 0) {
        pos = (pos + 1) & newMask
      }

      newKeys(pos) = keys(i)
      newValues(pos) = values(i)

      j -= 1
    }

    this.n = newN
    this.mask = n - 1
    this.maxFill = calMaxFill(n, f)
    this.keys = newKeys
    this.values = newValues
  }

  private def insert(pos: Int, k: K, v: V) {
    if (pos == n) containsNullKey = true
    keys(pos) = k
    values(pos) = v
    size += 1
    if (size >= maxFill) rehash(arraySize(size + 1, f))
  }

  def containKey(k: K): Boolean = getPos(k) >= 0

  /** Get the value for a given key */
  def apply(k: K): V = {
    val pos = getPos(k)

    if (pos >= 0) {
      values(pos)
    } else {
      throw new Exception(s"Cannot find key $k")
    }
  }

  def get(k: K): V = apply(k)

  /** Get the value for a given key, or returns elseValue if it doesn't exist. */
  def getOrElse(k: K, elseValue: V): V = {
    val pos = getPos(k)
    if (pos >= 0) values(pos) else elseValue
  }

  /** Set the value for a key */
  def update(k: K, v: V): this.type = {
    val pos = getPos(k)
    if (pos >= 0) {
      values(pos) = v
    } else {
      insert(-pos - 1, k, v)
    }

    this
  }

  def put(k: K, v: V): this.type = update(k, v)

  def putAll(keyArr: Array[K], valueArr: Array[V]): this.type = {
    keyArr.zip(valueArr).foreach { case (key, value) =>
      val pos = getPos(key)
      if (pos < 0) {
        insert(-pos - 1, key, value)
      } else {
        values(pos) = value
      }
    }

    this
  }

  def putMerge(k: K, v: V, mergeF: (V, V) => V): this.type = {
    val pos = getPos(k)
    if (pos < 0) {
      insert(-pos - 1, k, v)
    } else {
      values(pos) = mergeF(values(pos), v)
    }

    this
  }

  def changeValue(k: K, defaultValue: => V, mergeValue: V => V): V = {
    val pos = getPos(k)
    if (pos < 0) {
      val newValue = defaultValue
      insert(-pos - 1, k, newValue)
      newValue
    } else {
      val newValue = mergeValue(values(pos))
      values(pos) = newValue
      newValue
    }
  }

  def remove(k: K): V = {
    val pos = getPos(k)

    if (pos >= 0) {
      val preValue = values(pos)
      if (pos == n) {
        containsNullKey = false
      } else {
        keys(pos) = nullKey
      }
      values(pos) = defaultValue
      size -= 1
      preValue
    } else {
      throw new Exception(s"Cannot find key $k")
    }
  }

  def clear(): this.type = {
    size = 0
    var pos = 0
    while (pos < n) {
      if (keys(pos) != 0) {
        keys(pos) = nullKey
        values(pos) = defaultValue
      }
      pos += 1
    }

    if (containsNullKey) {
      values(n) = defaultValue
      containsNullKey = false
    }

    this
  }

  def foreach(func: (K, V) => Unit): Unit = {
    var pos = 0
    while (pos < n) {
      if (keys(pos) != 0) {
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
      if (keys(pos) != 0) {
        func(keys(pos))
      }
      pos += 1
    }

    if (containsNullKey) {
      func(nullKey)
    }
  }

  def foreachValue(func: V => Unit): Unit = {
    var pos = 0
    while (pos < n) {
      if (keys(pos) != 0) {
        func(values(pos))
      }
      pos += 1
    }

    if (containsNullKey) {
      func(values(n))
    }
  }

  def iterator: Iterator[(K, V)] = new Iterator[(K, V)] {
    private var pos = 0

    def hasNext: Boolean = {
      while (pos < n && keys(pos) == 0) {
        pos = pos + 1
      }

      if (pos < n) {
        true
      } else if (pos == n && containsNullKey) {
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