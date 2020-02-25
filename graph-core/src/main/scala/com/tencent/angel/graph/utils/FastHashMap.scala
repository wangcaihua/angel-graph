package com.tencent.angel.graph.utils


import scala.reflect._
import scala.{specialized => spec}


class FastHashMap[@spec(Int, Long) K: ClassTag, @spec(Byte, Int, Long, Float, Double) V: ClassTag](expected: Int, f: Float)
  extends Serializable {

  import com.tencent.angel.graph.utils.HashCommon._

  private lazy val keyTag = implicitly[ClassTag[K]].runtimeClass
  private lazy val valueTag = implicitly[ClassTag[V]].runtimeClass

  private var keys: Array[K] = _
  private var values: Array[V] = _
  private var containsNullKey: Boolean = false
  private var n: Int = arraySize(expected, f)
  private var mask: Int = n - 1
  private var maxFill: Int = calMaxFill(n, f)
  private var minN: Int = n
  private var numElements: Int = 0


  private lazy val nullKey: K = keyTag match {
    case kt if kt == classOf[Int] => 0.asInstanceOf[K]
    case kt if kt == classOf[Long] => 0.toLong.asInstanceOf[K]
  }
  private lazy val defaultValue: V = valueTag match {
    case vt if vt == classOf[Boolean] => false.asInstanceOf[V]
    case vt if vt == classOf[Char] => Char.MinValue.asInstanceOf[V]
    case vt if vt == classOf[Byte] => 0.toByte.asInstanceOf[V]
    case vt if vt == classOf[Short] => 0.toShort.asInstanceOf[V]
    case vt if vt == classOf[Int] => 0.asInstanceOf[V]
    case vt if vt == classOf[Long] => 0.toLong.asInstanceOf[V]
    case vt if vt == classOf[Float] => 0.toFloat.asInstanceOf[V]
    case vt if vt == classOf[Double] => 0.toDouble.asInstanceOf[V]
  }

  keys = new Array[K](n + 1)
  values = new Array[V](n + 1)

  private lazy val hash: Hasher[K] = {
    if (keyTag == classOf[Long]) {
      (new LongHasher).asInstanceOf[Hasher[K]]
    } else if (keyTag == classOf[Int]) {
      (new IntHasher).asInstanceOf[Hasher[K]]
    } else {
      new Hasher[K]
    }
  }

  def this(initCapacity: Int) = this(initCapacity, HashCommon.DEFAULT_LOAD_FACTOR)

  def this() = this(HashCommon.DEFAULT_INITIAL_SIZE, HashCommon.DEFAULT_LOAD_FACTOR)

  def size(): Int = numElements

  private def realSize: Int = if (containsNullKey) numElements - 1 else numElements

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

    var (i: Int, pos: Int, j: Int) = (n, 0, realSize)
    while (j > 0) {
      i -= 1
      while (i >= 0 && keys(i) == 0) {
        i -= 1
      }

      pos = hash(keys(i)) & newMask
      if (newKeys(pos) != 0) {
        pos = (pos + 1) & newMask
        while (newKeys(pos) != 0) {
          pos = (pos + 1) & newMask
        }
      }

      newKeys(pos) = keys(i)
      newValues(pos) = values(i)

      j -= 1
    }

    newValues(newN) = values(n)

    this.n = newN
    this.mask = newN - 1
    this.maxFill = calMaxFill(newN, f)
    this.keys = newKeys
    this.values = newValues
  }

  private def insert(pos: Int, k: K, v: V) {
    if (pos == n) containsNullKey = true
    keys(pos) = k
    values(pos) = v
    numElements += 1
    if (numElements - 1 >= maxFill) {
      rehash(arraySize(numElements + 1, f))
    }
  }

  private def removeNullEntry(): V = {
    containsNullKey = false
    val oldValue = values(n)
    numElements -= 1
    if (n > minN && numElements < maxFill / 4 && n > DEFAULT_INITIAL_SIZE)
      rehash(n / 2)

    oldValue
  }

  private def shiftKeys(p: Int): Unit = {
    var (last, slot, curr, pos) = (0, 0, nullKey, p)
    while (true) {
      last = pos
      pos = (pos + 1) & mask
      var flag = true
      while(flag) {
        curr = keys(pos)
        if (curr == 0) {
          keys(last) = nullKey
          return
        }

        slot = hash(curr) & mask
        val tmp = if (last <= pos) last >= slot || slot > pos else last >= slot && slot > pos
        if (tmp) {
          flag = false
        } else {
          pos = (pos + 1) & mask
        }
      }

      keys(last) = curr
      values(last) = values(pos)
    }
  }

  private def removeEntry(pos: Int): V = {
    val oldValue = values(pos)
    numElements -= 1
    shiftKeys(pos)
    if (n > minN && numElements < maxFill / 4 && n > DEFAULT_INITIAL_SIZE)
      rehash(n / 2)

    oldValue
  }

  def containsKey(k: K): Boolean = {
    var contain = false

    if (k == 0) {
      if (containsNullKey) {
        contain = true
      }
    } else {
      var pos = hash(k) & mask
      var curr = keys(pos)
      var flag = false

      while (!flag) {
        if (curr == 0) {
          contain = false
          flag = true
        } else if (curr == k) {
          contain = true
          flag = true
        } else {
          pos = (pos + 1) & mask
          curr = keys(pos)
        }
      }
    }

    contain
  }

  def apply(k: K): V = {
    val pos = getPos(k)

    if (pos >= 0) {
      values(pos)
    } else {
      throw new Exception(s"Cannot find key $k")
    }
  }

  def get(k: K): V = apply(k)

  def getOrElse(k: K, elseValue: V): V = {
    val pos = getPos(k)
    if (pos >= 0) values(pos) else elseValue
  }

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
    var preValue = defaultValue

    if (k == 0) {
      if (containsNullKey) {
        preValue = removeNullEntry()
      }
    } else {
      var pos = hash(k) & mask
      var curr = keys(pos)
      var flag = false

      while (!flag) {
        if (curr == 0) {
          flag = true
        } else if (curr == k) {
          preValue = removeEntry(pos)
          flag = true
        } else {
          pos = (pos + 1) & mask
          curr = keys(pos)
        }
      }
    }

    preValue
  }

  def clear(): this.type = {
    numElements = 0
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