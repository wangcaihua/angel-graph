package com.tencent.angel.graph.utils


import scala.reflect.ClassTag


abstract class SortDataFormat[K, Buffer] {
  def newKey(): K = null.asInstanceOf[K]

  protected def getKey(data: Buffer, pos: Int): K

  def getKey(data: Buffer, pos: Int, reuse: K): K = {
    getKey(data, pos)
  }

  def swap(data: Buffer, pos0: Int, pos1: Int): Unit

  def copyElement(src: Buffer, srcPos: Int, dst: Buffer, dstPos: Int): Unit

  def copyRange(src: Buffer, srcPos: Int, dst: Buffer, dstPos: Int, length: Int): Unit

  def allocate(length: Int): Buffer
}


class KVArraySortDataFormat[K, T <: AnyRef : ClassTag] extends SortDataFormat[K, Array[T]] {

  override def getKey(data: Array[T], pos: Int): K = data(2 * pos).asInstanceOf[K]

  override def swap(data: Array[T], pos0: Int, pos1: Int) {
    val tmpKey = data(2 * pos0)
    val tmpVal = data(2 * pos0 + 1)
    data(2 * pos0) = data(2 * pos1)
    data(2 * pos0 + 1) = data(2 * pos1 + 1)
    data(2 * pos1) = tmpKey
    data(2 * pos1 + 1) = tmpVal
  }

  override def copyElement(src: Array[T], srcPos: Int, dst: Array[T], dstPos: Int) {
    dst(2 * dstPos) = src(2 * srcPos)
    dst(2 * dstPos + 1) = src(2 * srcPos + 1)
  }

  override def copyRange(src: Array[T], srcPos: Int, dst: Array[T], dstPos: Int, length: Int) {
    System.arraycopy(src, 2 * srcPos, dst, 2 * dstPos, 2 * length)
  }

  override def allocate(length: Int): Array[T] = {
    new Array[T](2 * length)
  }
}