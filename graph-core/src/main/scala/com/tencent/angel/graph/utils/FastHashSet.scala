package com.tencent.angel.graph.utils

import scala.reflect._
import scala.{specialized => spec}


class FastHashSet[@spec(Int, Long) T: ClassTag](expected: Int, f: Float) extends Serializable {

  import com.tencent.angel.graph.utils.HashCommon._

  private var data: Array[T] = _
  private var containsNullKey: Boolean = false
  private var n: Int = arraySize(expected, f)
  private var mask: Int = n - 1
  private var maxFill: Int = calMaxFill(n, f)
  var size: Int = 0
  data = new Array[T](n + 1)

  private val nullKey: T = classTag[T] match {
    case kt if kt == ClassTag.Int => 0.asInstanceOf[T]
    case kt if kt == ClassTag.Long => 0L.asInstanceOf[T]
  }

  private val hash: Hasher[T] = classTag[T] match {
    case kt if kt == ClassTag.Long =>
      (new LongHasher).asInstanceOf[Hasher[T]]
    case kt if kt == ClassTag.Int =>
      (new IntHasher).asInstanceOf[Hasher[T]]
    case _ =>
      new Hasher[T]
  }

  def this(initCapacity: Int) = this(initCapacity, 0.7f)

  def this() = this(64)

  private def realSize: Int = if (containsNullKey) size - 1 else size

  private def getPos(key: T): Int = {
    if (key == 0) {
      if (containsNullKey) n else -(n + 1)
    } else {
      var pos = hash(key) & mask
      var curr = data(pos)

      if (curr == key) {
        pos
      } else {
        while (curr != key && curr != 0) {
          pos = (pos + 1) & mask
          curr = data(pos)
        }

        if (curr == key) pos else -(pos + 1)
      }
    }
  }

  private def rehash(newN: Int) {
    val newMask: Int = newN - 1 // Note that this is used by the hashing macro
    val newData: Array[T] = new Array[T](newN + 1)

    var (i: Int, pos: Int, j: Int) = (n - 1, 0, realSize)
    while (j != 0) {
      while (data(i) == 0) {
        i -= 1
      }

      pos = hash(data(i)) & newMask
      while (newData(pos) != 0) {
        pos = (pos + 1) & newMask
      }

      newData(pos) = data(i)

      j -= 1
    }

    this.n = newN
    this.mask = n - 1
    this.maxFill = calMaxFill(n, f)
    this.data = newData
  }

  private def insert(pos: Int, k: T) {
    if (pos == n) containsNullKey = true
    data(pos) = k
    size += 1
    if (size >= maxFill) rehash(arraySize(size + 1, f))
  }

  def contains(k: T): Boolean = getPos(k) >= 0

  def getBitSet: BitSet = {
    val bitSet = new BitSet(n + 1)
    if (containsNullKey) {
      bitSet.set(n)
    }

    var i = 0
    while (i < n) {
      if (data(i) != 0) {
        bitSet.set(i)
      }
      i += 1
    }

    bitSet
  }

  def add(k: T): this.type = {
    val pos = getPos(k)
    if (pos >= 0) {
      data(pos) = k
    } else {
      insert(-pos - 1, k)
    }

    this
  }

  def remove(k: T): this.type = {
    val pos = getPos(k)

    if (pos >= 0 && pos != n) {
      data(pos) = nullKey
    } else if (pos == n) {
      containsNullKey = false
    } else {
      throw new Exception(s"Cannot find key $k")
    }

    this
  }

  def clear(): this.type = {
    size = 0
    var pos = 0
    while (pos < n) {
      if (data(pos) != 0) {
        data(pos) = nullKey
      }
      pos += 1
    }

    if (containsNullKey) {
      containsNullKey = false
    }

    this
  }

  def union(other: FastHashSet[T]): FastHashSet[T] = {
    val iterator = other.iterator
    while (iterator.hasNext) {
      add(iterator.next())
    }

    this
  }

  def diff(other: FastHashSet[T]): FastHashSet[T] = {
    null
  }

  def intersection(other: FastHashSet[T]): FastHashSet[T] = {
    null
  }

  def addWithoutResize(k: T): this.type = {
    val pos = getPos(k)
    if (pos >= 0) {
      data(pos) = k
    } else if (-pos - 1 < n) {
      size += 1
      data(-pos - 1) = k
    } else {
      throw new Exception("addWithoutResize error, data full!")
    }

    this
  }

  /** Return the value at the specified position. */
  def getValue(pos: Int): T = data(pos)

  def iterator: Iterator[T] = new Iterator[T] {
    private var pos = 0

    override def hasNext: Boolean = {
      while (pos < n && data(pos) == 0) {
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

    override def next(): T = {
      pos += 1
      data(pos - 1)
    }
  }

  /** Return the value at the specified position. */
  def getValueSafe(pos: Int): T = data(pos)

}

