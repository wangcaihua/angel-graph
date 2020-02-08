package com.tencent.angel.graph.data

import com.tencent.angel.graph.utils.ReflectTrait
import io.netty.buffer.ByteBuf

import scala.reflect.runtime.universe._

class TypedNode extends NodeTrait {
  override def getNeighbors(args: Int*): Array[Int] = ???

  override def sampleOne(args: Int*): Int = ???

  override def sampleK(args: Int*): Array[Int] = ???

  override def hasNeighbor(args: Int*): Boolean = ???

  override def serialize(byteBuf: ByteBuf): Unit = {
    TypedNode.getRObject(this).serialize(byteBuf)
  }

  override def deserialize(byteBuf: ByteBuf): Unit = {
    TypedNode.getRObject(this).deserialize(byteBuf)
  }

  override def bufferLen(): Int = {
    TypedNode.getRObject(this).bufferLen()
  }
}

object TypedNode extends ReflectTrait {
  protected override val tp: Type = typeOf[TypedNode]
}
