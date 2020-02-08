package com.tencent.angel.graph.data

import com.tencent.angel.graph.utils.{ReflectTrait, RObject}

import scala.reflect.runtime.universe._
import io.netty.buffer.ByteBuf

class UnTypedNode extends NodeTrait {
  private val astr = Array("12345456", "2345")
  val bb = true
  val bs = "b".getBytes()(0)
  val cc = "c".toCharArray.head
  val str = "12345456"
  val abo = Array(true, false)
  val abs = "cbc".getBytes
  val ac = "abc".toCharArray
  val ii = 123
  val ll = 123L
  val ff = 123.0f
  val dd = 123.0
  private val ai = Array(123, 234)
  @transient val al = Array(123L, 234L)
  val af = Array(123.0f, 234.0f)
  val ad = Array(123.0, 234.0)

  override def getNeighbors(args: Int*): Array[Int] = ???

  override def sampleOne(args: Int*): Int = ???

  override def sampleK(args: Int*): Array[Int] = ???

  override def hasNeighbor(args: Int*): Boolean = ???

  override def serialize(byteBuf: ByteBuf): Unit = {
    UnTypedNode.getRObject(this).serialize(byteBuf)
  }

  override def deserialize(byteBuf: ByteBuf): Unit = {
    UnTypedNode.getRObject(this).deserialize(byteBuf)
  }

  override def bufferLen(): Int = {
    UnTypedNode.getRObject(this).bufferLen()
  }
}

object UnTypedNode extends ReflectTrait {
  override val tp: Type = typeOf[UnTypedNode]
}