package com.tencent.angel.graph.utils

import com.tencent.angel.graph.core.data.GData
import com.tencent.angel.ml.math2.vector._
import io.netty.buffer.ByteBuf

import scala.reflect._
import scala.reflect.runtime.universe._
import scala.{specialized => spec}

class FastHashMap[@spec(Int, Long) K: ClassTag, @spec V: ClassTag : TypeTag](expected: Int, private var f: Float)
  extends GData with Serializable {

  import com.tencent.angel.graph.utils.HashCommon._

  lazy val keyTag: Class[_] = implicitly[ClassTag[K]].runtimeClass
  lazy val valueTag: Class[_] = implicitly[ClassTag[V]].runtimeClass

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

  def mapValues[U: ClassTag : TypeTag](func: V => U): FastHashMap[K, U] = {
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

  def keyArray: Array[K] = {
    val temp = new Array[K](numElements)
    var idx = 0
    keys.foreach { k =>
      if (k != nullKey) {
        temp(idx) = k
        idx += 1
      }
    }

    temp
  }

  def valueArray: Array[V] = {
    val temp = new Array[V](numElements)
    var idx = 0
    values.zipWithIndex.foreach { case (v, i) =>
      if (keys(i) != nullKey) {
        temp(idx) = v
        idx += 1
      }
    }

    if (containsNullKey) {
      temp(idx) = values(n)
    }

    temp
  }

  def emptyLike(): FastHashMap[K, V] = new FastHashMap[K, V]

  def emptyLike(initSize: Int): FastHashMap[K, V] = new FastHashMap[K, V](initSize)

  def merge(other: FastHashMap[K, V]): this.type = {
    val comSize = numElements + other.size()
    if (comSize - 1 >= maxFill) {
      rehash(arraySize(comSize + 1, f))
    }

    other.foreach{ case (k, v) => update(k, v) }

    this
  }

  def merge(other: FastHashMap[K, V], mergeF: (V, V) => V): this.type = {
    val comSize = numElements + other.size()
    if (comSize - 1 >= maxFill) {
      rehash(arraySize(comSize + 1, f))
    }

    other.foreach{ case (k, v) =>
      if (containsKey(k)) {
        update(k, mergeF(apply(k), v))
      } else {
        update(k, v)
      }
    }

    this
  }

  override def serialize(byteBuf: ByteBuf): Unit = {
    byteBuf.writeInt(size())
    if (keyTag == classOf[Int]) {
      valueTag match {
        case vclz if vclz == classOf[Boolean] =>
          iterator.foreach { case (k: Int, v: Boolean) => byteBuf.writeInt(k).writeBoolean(v) }
        case vclz if vclz == classOf[Char] =>
          iterator.foreach { case (k: Int, v: Char) => byteBuf.writeInt(k).writeChar(v) }
        case vclz if vclz == classOf[Byte] =>
          iterator.foreach { case (k: Int, v: Byte) => byteBuf.writeInt(k).writeByte(v) }
        case vclz if vclz == classOf[Short] =>
          iterator.foreach { case (k: Int, v: Short) => byteBuf.writeInt(k).writeShort(v) }
        case vclz if vclz == classOf[Int] =>
          iterator.foreach { case (k: Int, v: Int) => byteBuf.writeInt(k).writeInt(v) }
        case vclz if vclz == classOf[Long] =>
          iterator.foreach { case (k: Int, v: Long) => byteBuf.writeInt(k).writeLong(v) }
        case vclz if vclz == classOf[Float] =>
          iterator.foreach { case (k: Int, v: Float) => byteBuf.writeInt(k).writeFloat(v) }
        case vclz if vclz == classOf[Double] =>
          iterator.foreach { case (k: Int, v: Double) => byteBuf.writeInt(k).writeDouble(v) }
        case vclz if vclz == classOf[String] =>
          iterator.foreach { case (k: Int, v: String) =>
            byteBuf.writeInt(k)
            val bytes = v.getBytes()
            byteBuf.writeInt(bytes.length).writeBytes(bytes)
          }
        case vclz if vclz.isArray =>
          iterator.foreach { case (k: Int, v: Array[_]) =>
            byteBuf.writeInt(k)
            SerDe.serArr(v, byteBuf)
          }
        case vclz if classOf[GData].isAssignableFrom(vclz) =>
          iterator.foreach { case (k: Int, v: GData) =>
            byteBuf.writeInt(k)
            v.serialize(byteBuf)
          }
        case vclz if classOf[Vector].isAssignableFrom(vclz) =>
          iterator.foreach { case (k: Int, v: Vector) =>
            byteBuf.writeInt(k)
            SerDe.serVector(v, byteBuf)
          }
        case vclz if classOf[Serializable].isAssignableFrom(vclz) =>
          iterator.foreach { case (k: Int, v: Serializable) =>
            byteBuf.writeInt(k)
            SerDe.javaSerialize(v, byteBuf)
          }
        case _ =>
          throw new Exception("serialize error")
      }
    } else {
      valueTag match {
        case vclz if vclz == classOf[Boolean] =>
          iterator.foreach { case (k: Long, v: Boolean) => byteBuf.writeLong(k).writeBoolean(v) }
        case vclz if vclz == classOf[Char] =>
          iterator.foreach { case (k: Long, v: Char) => byteBuf.writeLong(k).writeChar(v) }
        case vclz if vclz == classOf[Byte] =>
          iterator.foreach { case (k: Long, v: Byte) => byteBuf.writeLong(k).writeByte(v) }
        case vclz if vclz == classOf[Short] =>
          iterator.foreach { case (k: Long, v: Short) => byteBuf.writeLong(k).writeShort(v) }
        case vclz if vclz == classOf[Int] =>
          iterator.foreach { case (k: Long, v: Int) => byteBuf.writeLong(k).writeInt(v) }
        case vclz if vclz == classOf[Long] =>
          iterator.foreach { case (k: Long, v: Long) => byteBuf.writeLong(k).writeLong(v) }
        case vclz if vclz == classOf[Float] =>
          iterator.foreach { case (k: Long, v: Float) => byteBuf.writeLong(k).writeFloat(v) }
        case vclz if vclz == classOf[Double] =>
          iterator.foreach { case (k: Long, v: Double) => byteBuf.writeLong(k).writeDouble(v) }
        case vclz if vclz == classOf[String] =>
          iterator.foreach { case (k: Long, v: String) =>
            byteBuf.writeLong(k)
            val bytes = v.getBytes()
            byteBuf.writeInt(bytes.length).writeBytes(bytes)
          }
        case vclz if vclz.isArray =>
          iterator.foreach { case (k: Long, v: Array[_]) =>
            byteBuf.writeLong(k)
            SerDe.serArr(v, byteBuf)
          }
        case vclz if classOf[GData].isAssignableFrom(vclz) =>
          iterator.foreach { case (k: Long, v: GData) =>
            byteBuf.writeLong(k)
            v.serialize(byteBuf)
          }
        case vclz if classOf[Vector].isAssignableFrom(vclz) =>
          iterator.foreach { case (k: Long, v: Vector) =>
            byteBuf.writeLong(k)
            SerDe.serVector(v, byteBuf)
          }
        case vclz if classOf[Serializable].isAssignableFrom(vclz) =>
          iterator.foreach { case (k: Long, v: Serializable) =>
            byteBuf.writeLong(k)
            SerDe.javaSerialize(v, byteBuf)
          }
        case _ =>
          throw new Exception("serialize error")
      }
    }
  }

  override def deserialize(byteBuf: ByteBuf): Unit = {
    // init for insert
    val mapSize = byteBuf.readInt()
    n = arraySize(mapSize, f)
    mask = n - 1
    maxFill = calMaxFill(n, f)
    minN = n
    keys = new Array[K](n + 1)
    values = new Array[V](n + 1)

    // insert data from put
    if (keyTag == classOf[Int]) {
      valueTag match {
        case vclz if vclz == classOf[Boolean] =>
          (0 until mapSize).foreach { _ =>
            val key = byteBuf.readInt()
            val value = byteBuf.readBoolean()
            update(key.asInstanceOf[K], value.asInstanceOf[V])
          }
        case vclz if vclz == classOf[Char] =>
          (0 until mapSize).foreach { _ =>
            val key = byteBuf.readInt()
            val value = byteBuf.readChar()
            update(key.asInstanceOf[K], value.asInstanceOf[V])
          }
        case vclz if vclz == classOf[Byte] =>
          (0 until mapSize).foreach { _ =>
            val key = byteBuf.readInt()
            val value = byteBuf.readByte()
            update(key.asInstanceOf[K], value.asInstanceOf[V])
          }
        case vclz if vclz == classOf[Short] =>
          (0 until mapSize).foreach { _ =>
            val key = byteBuf.readInt()
            val value = byteBuf.readShort()
            update(key.asInstanceOf[K], value.asInstanceOf[V])
          }
        case vclz if vclz == classOf[Int] =>
          (0 until mapSize).foreach { _ =>
            val key = byteBuf.readInt()
            val value = byteBuf.readInt()
            update(key.asInstanceOf[K], value.asInstanceOf[V])
          }
        case vclz if vclz == classOf[Long] =>
          (0 until mapSize).foreach { _ =>
            val key = byteBuf.readInt()
            val value = byteBuf.readLong()
            update(key.asInstanceOf[K], value.asInstanceOf[V])
          }
        case vclz if vclz == classOf[Float] =>
          (0 until mapSize).foreach { _ =>
            val key = byteBuf.readInt()
            val value = byteBuf.readFloat()
            update(key.asInstanceOf[K], value.asInstanceOf[V])
          }
        case vclz if vclz == classOf[Double] =>
          (0 until mapSize).foreach { _ =>
            val key = byteBuf.readInt()
            val value = byteBuf.readDouble()
            update(key.asInstanceOf[K], value.asInstanceOf[V])
          }
        case vclz if vclz == classOf[String] =>
          (0 until mapSize).foreach { _ =>
            val key = byteBuf.readInt()
            val len = byteBuf.readInt()
            val bytes = new Array[Byte](len)
            byteBuf.readBytes(bytes)
            val value = new String(bytes)
            update(key.asInstanceOf[K], value.asInstanceOf[V])
          }
        case vclz if vclz == classOf[Array[Boolean]] =>
          (0 until mapSize).foreach { _ =>
            val key = byteBuf.readInt()
            val value = SerDe.arrFromBuffer[Boolean](byteBuf)
            put(key.asInstanceOf[K], value.asInstanceOf[V])
          }
        case vclz if vclz == classOf[Array[Char]] =>
          (0 until mapSize).foreach { _ =>
            val key = byteBuf.readInt()
            val value = SerDe.arrFromBuffer[Char](byteBuf)
            put(key.asInstanceOf[K], value.asInstanceOf[V])
          }
        case vclz if vclz == classOf[Array[Byte]] =>
          (0 until mapSize).foreach { _ =>
            val key = byteBuf.readInt()
            val value = SerDe.arrFromBuffer[Byte](byteBuf)
            put(key.asInstanceOf[K], value.asInstanceOf[V])
          }
        case vclz if vclz == classOf[Array[Short]] =>
          (0 until mapSize).foreach { _ =>
            val key = byteBuf.readInt()
            val value = SerDe.arrFromBuffer[Short](byteBuf)
            put(key.asInstanceOf[K], value.asInstanceOf[V])
          }
        case vclz if vclz == classOf[Array[Int]] =>
          (0 until mapSize).foreach { _ =>
            val key = byteBuf.readInt()
            val value = SerDe.arrFromBuffer[Int](byteBuf)
            put(key.asInstanceOf[K], value.asInstanceOf[V])
          }
        case vclz if vclz == classOf[Array[Long]] =>
          (0 until mapSize).foreach { _ =>
            val key = byteBuf.readInt()
            val value = SerDe.arrFromBuffer[Long](byteBuf)
            put(key.asInstanceOf[K], value.asInstanceOf[V])
          }
        case vclz if vclz == classOf[Array[Float]] =>
          (0 until mapSize).foreach { _ =>
            val key = byteBuf.readInt()
            val value = SerDe.arrFromBuffer[Float](byteBuf)
            put(key.asInstanceOf[K], value.asInstanceOf[V])
          }
        case vclz if vclz == classOf[Array[Double]] =>
          (0 until mapSize).foreach { _ =>
            val key = byteBuf.readInt()
            val value = SerDe.arrFromBuffer[Double](byteBuf)
            put(key.asInstanceOf[K], value.asInstanceOf[V])
          }
        case vclz if classOf[GData].isAssignableFrom(vclz) =>
          (0 until mapSize).foreach { _ =>
            val key = byteBuf.readInt()
            val value = ReflectUtils.newInstance(typeOf[V])
              .asInstanceOf[GData]
            value.deserialize(byteBuf)
            put(key.asInstanceOf[K], value.asInstanceOf[V])
          }
        case vclz if classOf[Vector].isAssignableFrom(vclz) =>
          (0 until mapSize).foreach { _ =>
            val key = byteBuf.readInt()
            val value = SerDe.vectorFromBuffer(typeOf[V], byteBuf)
            put(key.asInstanceOf[K], value.asInstanceOf[V])
          }
        case vclz if classOf[Serializable].isAssignableFrom(vclz) =>
          (0 until mapSize).foreach { _ =>
            val key = byteBuf.readInt()
            val value = SerDe.javaDeserialize[V](byteBuf)
            put(key.asInstanceOf[K], value)
          }
        case _ =>
      }
    } else {
      valueTag match {
        case vclz if vclz == classOf[Boolean] =>
          (0 until mapSize).foreach { _ =>
            val key = byteBuf.readLong()
            val value = byteBuf.readBoolean()
            put(key.asInstanceOf[K], value.asInstanceOf[V])
          }
        case vclz if vclz == classOf[Char] =>
          (0 until mapSize).foreach { _ =>
            val key = byteBuf.readLong()
            val value = byteBuf.readChar()
            put(key.asInstanceOf[K], value.asInstanceOf[V])
          }
        case vclz if vclz == classOf[Byte] =>
          (0 until mapSize).foreach { _ =>
            val key = byteBuf.readLong()
            val value = byteBuf.readByte()
            put(key.asInstanceOf[K], value.asInstanceOf[V])
          }
        case vclz if vclz == classOf[Short] =>
          (0 until mapSize).foreach { _ =>
            val key = byteBuf.readLong()
            val value = byteBuf.readShort()
            put(key.asInstanceOf[K], value.asInstanceOf[V])
          }
        case vclz if vclz == classOf[Int] =>
          (0 until mapSize).foreach { _ =>
            val key = byteBuf.readLong()
            val value = byteBuf.readInt()
            put(key.asInstanceOf[K], value.asInstanceOf[V])
          }
        case vclz if vclz == classOf[Long] =>
          (0 until mapSize).foreach { _ =>
            val key = byteBuf.readLong()
            val value = byteBuf.readLong()
            put(key.asInstanceOf[K], value.asInstanceOf[V])
          }
        case vclz if vclz == classOf[Float] =>
          (0 until mapSize).foreach { _ =>
            val key = byteBuf.readLong()
            val value = byteBuf.readFloat()
            put(key.asInstanceOf[K], value.asInstanceOf[V])
          }
        case vclz if vclz == classOf[Double] =>
          (0 until mapSize).foreach { _ =>
            val key = byteBuf.readLong()
            val value = byteBuf.readDouble()
            put(key.asInstanceOf[K], value.asInstanceOf[V])
          }
        case vclz if vclz == classOf[String] =>
          (0 until mapSize).foreach { _ =>
            val key = byteBuf.readLong()
            val len = byteBuf.readInt()
            val bytes = new Array[Byte](len)
            byteBuf.readBytes(bytes)
            val value = new String(bytes)
            update(key.asInstanceOf[K], value.asInstanceOf[V])
          }
        case vclz if vclz == classOf[Array[Boolean]] =>
          (0 until mapSize).foreach { _ =>
            val key = byteBuf.readLong()
            val value = SerDe.arrFromBuffer[Boolean](byteBuf)
            put(key.asInstanceOf[K], value.asInstanceOf[V])
          }
        case vclz if vclz == classOf[Array[Char]] =>
          (0 until mapSize).foreach { _ =>
            val key = byteBuf.readLong()
            val value = SerDe.arrFromBuffer[Char](byteBuf)
            put(key.asInstanceOf[K], value.asInstanceOf[V])
          }
        case vclz if vclz == classOf[Array[Byte]] =>
          (0 until mapSize).foreach { _ =>
            val key = byteBuf.readLong()
            val value = SerDe.arrFromBuffer[Byte](byteBuf)
            put(key.asInstanceOf[K], value.asInstanceOf[V])
          }
        case vclz if vclz == classOf[Array[Short]] =>
          (0 until mapSize).foreach { _ =>
            val key = byteBuf.readLong()
            val value = SerDe.arrFromBuffer[Short](byteBuf)
            put(key.asInstanceOf[K], value.asInstanceOf[V])
          }
        case vclz if vclz == classOf[Array[Int]] =>
          (0 until mapSize).foreach { _ =>
            val key = byteBuf.readLong()
            val value = SerDe.arrFromBuffer[Int](byteBuf)
            put(key.asInstanceOf[K], value.asInstanceOf[V])
          }
        case vclz if vclz == classOf[Array[Long]] =>
          (0 until mapSize).foreach { _ =>
            val key = byteBuf.readLong()
            val value = SerDe.arrFromBuffer[Long](byteBuf)
            put(key.asInstanceOf[K], value.asInstanceOf[V])
          }
        case vclz if vclz == classOf[Array[Float]] =>
          (0 until mapSize).foreach { _ =>
            val key = byteBuf.readLong()
            val value = SerDe.arrFromBuffer[Float](byteBuf)
            put(key.asInstanceOf[K], value.asInstanceOf[V])
          }
        case vclz if vclz == classOf[Array[Double]] =>
          (0 until mapSize).foreach { _ =>
            val key = byteBuf.readLong()
            val value = SerDe.arrFromBuffer[Double](byteBuf)
            put(key.asInstanceOf[K], value.asInstanceOf[V])
          }
        case vclz if classOf[GData].isAssignableFrom(vclz) =>
          (0 until mapSize).foreach { _ =>
            val key = byteBuf.readLong()
            val value = ReflectUtils.newInstance(typeOf[V])
              .asInstanceOf[GData]
            value.deserialize(byteBuf)
            put(key.asInstanceOf[K], value.asInstanceOf[V])
          }
        case vclz if classOf[Vector].isAssignableFrom(vclz) =>
          (0 until mapSize).foreach { _ =>
            val key = byteBuf.readLong()
            val value = SerDe.vectorFromBuffer(typeOf[V], byteBuf)
            put(key.asInstanceOf[K], value.asInstanceOf[V])
          }
        case vclz if classOf[Serializable].isAssignableFrom(vclz) =>
          (0 until mapSize).foreach { _ =>
            val key = byteBuf.readLong()
            val value = SerDe.javaDeserialize[V](byteBuf)
            put(key.asInstanceOf[K], value)
          }
        case _ =>
      }
    }
  }

  override def bufferLen(): Int = {
    var len = 4
    if (keyTag == classOf[Int]) {
      valueTag match {
        case vclz if vclz == classOf[Boolean] =>
          len += size() * (SerDe.boolSize + 4)
        case vclz if vclz == classOf[Char] =>
          len += size() * (SerDe.charSize + 4)
        case vclz if vclz == classOf[Byte] =>
          len += size() * 5
        case vclz if vclz == classOf[Short] =>
          len += size() * (SerDe.shortSize + 4)
        case vclz if vclz == classOf[Int] =>
          len += size() * 8
        case vclz if vclz == classOf[Long] =>
          len += size() * 12
        case vclz if vclz == classOf[Float] =>
          len += size() * 8
        case vclz if vclz == classOf[Double] =>
          len += size() * 12
        case vclz if vclz == classOf[String] =>
          iterator.foreach { case (_, v: String) =>
            val bytes = v.getBytes()
            len += 8 + bytes.length
          }
        case vclz if vclz.isArray =>
          iterator.foreach { case (_, v) =>
            len += 4 + SerDe.serArrBufSize(v)
          }
        case vclz if classOf[GData].isAssignableFrom(vclz) =>
          iterator.foreach { case (_, v: GData) =>
            len += v.bufferLen() + 4
          }
        case vclz if classOf[Vector].isAssignableFrom(vclz) =>
          iterator.foreach { case (_: Int, v: Vector) =>
            len += 4 + SerDe.serVectorBufSize(v)
          }
        case vclz if classOf[Serializable].isAssignableFrom(vclz) =>
          iterator.foreach { case (_, v: Serializable) =>
            len += SerDe.javaSerBufferSize(v) + 4
          }
        case _ =>
      }
    } else {
      valueTag match {
        case vclz if vclz == classOf[Boolean] =>
          len += size() * (SerDe.boolSize + 8)
        case vclz if vclz == classOf[Char] =>
          len += size() * (SerDe.charSize + 8)
        case vclz if vclz == classOf[Byte] =>
          len += size() * 9
        case vclz if vclz == classOf[Short] =>
          len += size() * (SerDe.shortSize + 8)
        case vclz if vclz == classOf[Int] =>
          len += size() * 12
        case vclz if vclz == classOf[Long] =>
          len += size() * 16
        case vclz if vclz == classOf[Float] =>
          len += size() * 12
        case vclz if vclz == classOf[Double] =>
          len += size() * 16
        case vclz if vclz == classOf[String] =>
          iterator.foreach { case (_, v: String) =>
            val bytes = v.getBytes()
            len += 12 + bytes.length
          }
        case vclz if vclz.isArray =>
          iterator.foreach { case (_, v) =>
            len += 8 + SerDe.serArrBufSize(v)
          }
        case vclz if classOf[GData].isAssignableFrom(vclz) =>
          iterator.foreach { case (_, v: GData) =>
            len += v.bufferLen() + 8
          }
        case vclz if classOf[Vector].isAssignableFrom(vclz) =>
          iterator.foreach { case (_: Int, v: Vector) =>
            len += 8 + SerDe.serVectorBufSize(v)
          }
        case vclz if classOf[Serializable].isAssignableFrom(vclz) =>
          iterator.foreach { case (_, v: Serializable) =>
            len += SerDe.javaSerBufferSize(v) + 8
          }
        case _ =>
      }
    }

    len
  }
}

object FastHashMap {

  import com.tencent.angel.graph.utils.HashCommon._

  def fromUnimi[K: ClassTag, V: ClassTag : TypeTag](empty: Any): FastHashMap[K, V] = {
    val keys: Array[K] = getField[Array[K]](empty, "key")
    val values: Array[V] = getField[Array[V]](empty, "value")
    val containsNullKey: Boolean = getField[Boolean](empty, "containsNullKey")
    val n: Int = getField[Int](empty, "n")
    val f: Float = getField[Float](empty, "f")
    val numElements: Int = getField[Int](empty, "size")

    new FastHashMap[K, V](keys, values, containsNullKey, n, f, numElements)
  }

  private[FastHashMap] def toUnimi[K: ClassTag, V: ClassTag](keys: Array[K], values: Array[V], containsNullKey: Boolean,
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