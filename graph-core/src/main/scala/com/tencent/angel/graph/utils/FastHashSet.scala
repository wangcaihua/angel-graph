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
  private var minN: Int = n
  private var numElements: Int = 0

  private lazy val keyTag = implicitly[ClassTag[T]].runtimeClass
  private lazy val nullKey: T = keyTag match {
    case kt if kt == classOf[Int] => 0.asInstanceOf[T]
    case kt if kt == classOf[Long] => 0.toLong.asInstanceOf[T]
  }

  data = new Array[T](n + 1)

  private lazy val hash: Hasher[T] = {
    if (keyTag == classOf[Long]) {
      (new LongHasher).asInstanceOf[Hasher[T]]
    } else if (keyTag == classOf[Int]) {
      (new IntHasher).asInstanceOf[Hasher[T]]
    } else {
      new Hasher[T]
    }
  }

  def this(initCapacity: Int) = this(initCapacity, HashCommon.DEFAULT_LOAD_FACTOR)

  def this() = this(HashCommon.DEFAULT_INITIAL_SIZE, HashCommon.DEFAULT_LOAD_FACTOR)

  def size(): Int = numElements

  private def realSize: Int = if (containsNullKey) numElements - 1 else numElements

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
    val newKeys: Array[T] = new Array[T](newN + 1)

    var (i: Int, pos: Int, j: Int) = (n, 0, realSize)
    while (j > 0) {
      i -= 1
      while (i >= 0 && data(i) == 0) {
        i -= 1
      }

      pos = hash(data(i)) & newMask
      if (newKeys(pos) != 0) {
        pos = (pos + 1) & newMask
        while (newKeys(pos) != 0) {
          pos = (pos + 1) & newMask
        }
      }

      newKeys(pos) = data(i)

      j -= 1
    }

    this.n = newN
    this.mask = newN - 1
    this.maxFill = calMaxFill(newN, f)
    this.data = newKeys
  }

  private def insert(pos: Int, k: T) {
    if (pos == n) containsNullKey = true
    data(pos) = k
    numElements += 1
    if (numElements - 1 >= maxFill) {
      rehash(arraySize(numElements + 1, f))
    }
  }

  private def removeNullEntry(): Unit = {
    containsNullKey = false
    numElements -= 1
    if (n > minN && numElements < maxFill / 4 && n > DEFAULT_INITIAL_SIZE)
      rehash(n / 2)
  }

  private def shiftKeys(p: Int): Unit = {
    var (last, slot, curr, pos) = (0, 0, nullKey, p)
    while (true) {
      last = pos
      pos = (pos + 1) & mask
      var flag = true
      while(flag) {
        curr = data(pos)
        if (curr == 0) {
          data(last) = nullKey
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

      data(last) = curr
    }
  }

  private def removeEntry(pos: Int): Unit = {
    numElements -= 1
    shiftKeys(pos)
    if (n > minN && numElements < maxFill / 4 && n > DEFAULT_INITIAL_SIZE)
      rehash(n / 2)
  }

  def contains(k: T): Boolean = {
    var contain = false

    if (k == 0) {
      if (containsNullKey) {
        contain = true
      }
    } else {
      var pos = hash(k) & mask
      var curr = data(pos)
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
          curr = data(pos)
        }
      }
    }

    contain
  }

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
    if (k == 0) {
      if (containsNullKey) {
        removeNullEntry()
      }
    } else {
      var pos = hash(k) & mask
      var curr = data(pos)
      var flag = false

      while (!flag) {
        if (curr == 0) {
          flag = true
        } else if (curr == k) {
          removeEntry(pos)
          flag = true
        } else {
          pos = (pos + 1) & mask
          curr = data(pos)
        }
      }
    }

    this
  }

  def clear(): this.type = {
    numElements = 0
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
      numElements += 1
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

  def toArray: Array[T] = {
    val temp = new Array[T](numElements)
    var idx = 0
    data.foreach { k =>
      if (k != nullKey) {
        temp(idx) = k
        idx += 1
      }
    }

    temp
  }
}

