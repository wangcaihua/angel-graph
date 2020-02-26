package com.tencent.angel.graph.utils


import com.tencent.angel.graph.VertexId
import it.unimi.dsi.fastutil.ints._
import it.unimi.dsi.fastutil.longs._

import scala.reflect._
import scala.{specialized => spec}

class FastHashMap[@spec(Int, Long) K: ClassTag, @spec V: ClassTag](expected: Int, private var f: Float)
  extends Serializable {

  import com.tencent.angel.graph.utils.HashCommon._

  protected lazy val keyTag: Class[_] = implicitly[ClassTag[K]].runtimeClass
  protected lazy val valueTag: Class[_] = implicitly[ClassTag[V]].runtimeClass

  private var keys: Array[K] = _
  private var values: Array[V] = _
  private var containsNullKey: Boolean = false
  private var n: Int = arraySize(expected, f)
  private var mask: Int = n - 1
  private var maxFill: Int = calMaxFill(n, f)
  private var minN: Int = n
  private var numElements: Int = 0

  protected lazy val nullKey: K = keyTag match {
    case kt if kt == classOf[Int] => 0.asInstanceOf[K]
    case kt if kt == classOf[Long] => 0.toLong.asInstanceOf[K]
  }
  protected lazy val defaultValue: V = valueTag match {
    case vt if vt == classOf[Boolean] => false.asInstanceOf[V]
    case vt if vt == classOf[Char] => Char.MinValue.asInstanceOf[V]
    case vt if vt == classOf[Byte] => 0.toByte.asInstanceOf[V]
    case vt if vt == classOf[Short] => 0.toShort.asInstanceOf[V]
    case vt if vt == classOf[Int] => 0.asInstanceOf[V]
    case vt if vt == classOf[Long] => 0.toLong.asInstanceOf[V]
    case vt if vt == classOf[Float] => 0.toFloat.asInstanceOf[V]
    case vt if vt == classOf[Double] => 0.toDouble.asInstanceOf[V]
    case _ => null.asInstanceOf[V]
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

  def this(keys: Array[K], values: Array[V], containsNullKey: Boolean, n: Int, f: Float, numElements: Int) = {
    this()

    this.keys = keys
    this.values = values
    this.containsNullKey = containsNullKey
    this.n = n
    this.f = f
    this.mask = n - 1
    this.maxFill = calMaxFill(n, f)
    this.minN = n
    this.numElements = numElements
  }

  protected def getInnerKeys: Array[K] = keys

  protected def getInnerValues: Array[V] = values

  protected def getInnerN: Int = n

  protected def getInnerF: Float = f

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
      while (flag) {
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

  def mapValues[U: ClassTag](func: V => U): FastHashMap[K, U] = {
    //val newKeys = new Array[K](keys.length)
    val newValues = new Array[U](keys.length)

    keys.zipWithIndex.foreach { case (key, idx) =>
      if (key != 0) {
        //newKeys(idx) = key
        newValues(idx) = func(values(idx))
      }
    }

    if (containsNullKey) {
      newValues(n) = func(values(n))
    }

    new FastHashMap[K, U](keys, newValues, containsNullKey, n, f, numElements)
  }

  def toUnimi[T]: T = {
    val unimiMap = (keyTag, valueTag) match {
      case (kt, vt) if kt == classOf[Int] && vt == classOf[Boolean] =>
        new Int2BooleanOpenHashMap()
      case (kt, vt) if kt == classOf[Int] && vt == classOf[Char] =>
        new Int2CharOpenHashMap()
      case (kt, vt) if kt == classOf[Int] && vt == classOf[Byte] =>
        new Int2ByteOpenHashMap()
      case (kt, vt) if kt == classOf[Int] && vt == classOf[Short] =>
        new Int2ShortOpenHashMap()
      case (kt, vt) if kt == classOf[Int] && vt == classOf[Int] =>
        new Int2IntOpenHashMap()
      case (kt, vt) if kt == classOf[Int] && vt == classOf[Long] =>
        new Int2LongOpenHashMap()
      case (kt, vt) if kt == classOf[Int] && vt == classOf[Float] =>
        new Int2FloatOpenHashMap()
      case (kt, vt) if kt == classOf[Int] && vt == classOf[Double] =>
        new Int2DoubleOpenHashMap()
      case (kt, _) if kt == classOf[Int] =>
        new Int2ObjectOpenHashMap[V]()
      case (kt, vt) if kt == classOf[Long] && vt == classOf[Boolean] =>
        new Long2BooleanOpenHashMap()
      case (kt, vt) if kt == classOf[Long] && vt == classOf[Char] =>
        new Long2CharOpenHashMap()
      case (kt, vt) if kt == classOf[Long] && vt == classOf[Byte] =>
        new Long2ByteOpenHashMap()
      case (kt, vt) if kt == classOf[Long] && vt == classOf[Short] =>
        new Long2ShortOpenHashMap()
      case (kt, vt) if kt == classOf[Long] && vt == classOf[Int] =>
        new Long2IntOpenHashMap()
      case (kt, vt) if kt == classOf[Long] && vt == classOf[Long] =>
        new Long2LongOpenHashMap()
      case (kt, vt) if kt == classOf[Long] && vt == classOf[Float] =>
        new Long2FloatOpenHashMap()
      case (kt, vt) if kt == classOf[Long] && vt == classOf[Double] =>
        new Long2DoubleOpenHashMap()
      case (kt, _) if kt == classOf[Long] =>
        new Long2ObjectOpenHashMap[V]()
    }

    FastHashMap.toUnimi[K, V](keys, values, containsNullKey, n, f,
      numElements, unimiMap).asInstanceOf[T]
  }

  def asIdMaps: (FastHashMap[VertexId, Int], Array[VertexId]) = {
    assert(keyTag == classOf[VertexId])

    val local2global = new Array[VertexId](size())

    val temp = if (valueTag == classOf[Int]) {
      values.asInstanceOf[Array[Int]]
    } else {
      new Array[Int](keys.length)
    }

    var reIdx = 0
    keys.zipWithIndex.foreach {
      case (key, idx) if key != nullKey =>
        temp(idx) = reIdx
        local2global(reIdx) = key.asInstanceOf[VertexId]
        reIdx += 1
    }

    if (containsNullKey && values(n) != defaultValue) {
      temp(n) = reIdx
      local2global(reIdx) = 0
    }

    if (valueTag == classOf[Int]) {
      this.asInstanceOf[FastHashMap[VertexId, Int]] -> local2global
    } else {
      new FastHashMap[VertexId, Int](keys.asInstanceOf[Array[VertexId]], temp,
        containsNullKey, n, f, numElements) -> local2global
    }
  }
}

object FastHashMap {

  import com.tencent.angel.graph.utils.HashCommon._

  def getIdMaps(empty: Any): (FastHashMap[VertexId, Int], Array[VertexId]) = {
    val keys: Array[VertexId] = getField[Array[VertexId]](empty, "key")
    val values: Array[Int] = getField[Array[Int]](empty, "value")
    val containsNullKey: Boolean = getField[Boolean](empty, "containsNullKey")
    val n: Int = getField[Int](empty, "n")
    val f: Float = getField[Int](empty, "f")
    val numElements: Int = getField[Int](empty, "size")

    new FastHashMap[VertexId, Int](keys, values, containsNullKey, n, f, numElements).asIdMaps
  }

  def fromUnimi[K: ClassTag, V: ClassTag](empty: Any): FastHashMap[K, V] = {
    val keys: Array[K] = getField[Array[K]](empty, "key")
    val values: Array[V] = getField[Array[V]](empty, "value")
    val containsNullKey: Boolean = getField[Boolean](empty, "containsNullKey")
    val n: Int = getField[Int](empty, "n")
    val f: Float = getField[Int](empty, "f")
    val numElements: Int = getField[Int](empty, "size")

    new FastHashMap[K, V](keys, values, containsNullKey, n, f, numElements)
  }

  def toUnimi[K: ClassTag, V: ClassTag](keys: Array[K], values: Array[V], containsNullKey: Boolean,
                                        n: Int, f: Float, numElements: Int, unimiMap: Any): Any = {
    setField(unimiMap, "key", keys)
    setField(unimiMap, "value", values)
    setField(unimiMap, "containsNullKey", containsNullKey)
    setField(unimiMap, "n", n)
    setField(unimiMap, "f", f)
    setField(unimiMap, "size", numElements)
    setField(unimiMap, "mask", n - 1)
    setField(unimiMap, "maxFill", calMaxFill(n, f))
    setField(unimiMap, "minN", n)

    unimiMap
  }

  private def getField[T](obj: Any, name: String): T = {
    val clz: Class[_] = obj.getClass
    val field = clz.getDeclaredField(name)
    field.setAccessible(true)
    field.get(obj).asInstanceOf[T]
  }

  private def setField(obj: Any, name: String, value: Any): Unit = {
    val clz: Class[_] = obj.getClass
    val field = clz.getDeclaredField(name)
    field.setAccessible(true)
    field.set(obj, value)
  }
}