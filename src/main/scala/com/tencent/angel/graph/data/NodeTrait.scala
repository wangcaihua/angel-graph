package com.tencent.angel.graph.data

import java.io.{DataInputStream, DataOutputStream}


import com.tencent.angel.ps.storage.vector.element.IElement
import io.netty.buffer.ByteBuf


trait NodeTrait extends IElement {
  def getNeighbors(args: Int*): Array[Int]

  def sampleOne(args: Int*): Int

  def sampleK(args: Int*): Array[Int]

  def hasNeighbor(args: Int*): Boolean

  override def serialize(byteBuf: ByteBuf): Unit

  override def deserialize(byteBuf: ByteBuf): Unit

  override def bufferLen(): Int

  override def serialize(dataOutputStream: DataOutputStream): Unit = ???

  override def deserialize(dataInputStream: DataInputStream): Unit = ???

  override def dataLen(): Int = ???

  override def deepClone(): AnyRef = ???
}


