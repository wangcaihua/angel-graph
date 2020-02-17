package com.tencent.angel.graph.core.data

import java.util

import scala.reflect.ClassTag


class RefHashMapManager(ref: AnyRef, matrixId: Int, partitionId: Int) {
  private val clz = ref.getClass

  private val maskField = {
    val field = clz.getDeclaredField("mask")
    field.setAccessible(true)
    field
  }

  private val nField = {
    val field = clz.getDeclaredField("n")
    field.setAccessible(true)
    field
  }

  private val containsNullKeyField = {
    val field = clz.getDeclaredField("containsNullKey")
    field.setAccessible(true)
    field
  }

  private val keyField = {
    val field = clz.getDeclaredField("key")
    field.setAccessible(true)
    field
  }

  private val valueMap: util.HashMap[String, Any] = new util.HashMap[String, Any]()

  val mask: Int = maskField.get(ref).asInstanceOf[Int]

  val n: Int = nField.get(ref).asInstanceOf[Int]

  val containsNullKey: Boolean = containsNullKeyField.get(ref).asInstanceOf[Boolean]

  val innerSize: Int = keyField.get(ref).asInstanceOf[Array[_]].length

  def addValues[V](name: String, values: Array[V]): this.type = valueMap.synchronized {
    assert(innerSize == values.length)
    valueMap.put(name, values)

    this
  }

  def addValues[V: ClassTag](name: String): this.type = valueMap.synchronized {
    val values = new Array[V](innerSize)
    valueMap.put(name, values)

    this
  }

  def removeArray(name: String): Unit = valueMap.synchronized {
    if (valueMap.containsKey(name)) {
      valueMap.remove(name)
    }
  }

  def getIntKeyHashMap[V](name: String): IntKeyRefHashMap[V] = {
    if (!valueMap.containsKey(name)) {
      throw new Exception(s"cannot find $name in valueMap !")
    }

    assert(mask == maskField.get(ref).asInstanceOf[Int] &&
      n == nField.get(ref).asInstanceOf[Int] &&
      containsNullKey == containsNullKeyField.get(ref).asInstanceOf[Boolean] &&
      innerSize == keyField.get(ref).asInstanceOf[Array[_]].length)

    val keys = keyField.get(ref).asInstanceOf[Array[Int]]
    val values = valueMap.get(name).asInstanceOf[Array[V]]
    new IntKeyRefHashMap[V](mask, n, containsNullKey, keys, values)
  }

  def getLongKeyHashMap[V](name: String): LongKeyRefHashMap[V] = {
    if (!valueMap.containsKey(name)) {
      throw new Exception(s"cannot find $name in valueMap !")
    }

    assert(mask == maskField.get(ref).asInstanceOf[Int] &&
      n == nField.get(ref).asInstanceOf[Int] &&
      containsNullKey == containsNullKeyField.get(ref).asInstanceOf[Boolean] &&
      innerSize == keyField.get(ref).asInstanceOf[Array[_]].length)

    val keys = keyField.get(ref).asInstanceOf[Array[Long]]
    val values = valueMap.get(name).asInstanceOf[Array[V]]
    new LongKeyRefHashMap[V](mask, n, containsNullKey, keys, values)
  }
}