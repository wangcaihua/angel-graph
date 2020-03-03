package com.tencent.angel

import java.nio.ByteBuffer

import com.tencent.angel.graph.utils.FastHashSet

import scala.reflect.ClassTag
import scala.util.Random

package object graph {
  private val rand = new Random()

  type VertexId = Long
  type VertexType = Short
  type WgtTpe = Float
  type PartitionID = Int

  type VertexSet = FastHashSet[VertexId]

  val defaultWeight: WgtTpe = 0.0f

  def getDefaultEdgeAttr[ED: ClassTag]: ED = {
    val any = implicitly[ClassTag[ED]].runtimeClass match {
      case ed if ed == classOf[Long] => 0L
      case ed if ed == classOf[Float] => 0.0f
      case ed if ed == classOf[Int] => 0
      case ed if ed == classOf[Double] => 0.0
      case ed if ed == classOf[Boolean] => false
      case ed if ed == classOf[Char] => Char.MinValue
      case ed if ed == classOf[Byte] => 0.toByte
      case ed if ed == classOf[Short] => 0.toShort
    }

    any.asInstanceOf[ED]
  }

  def randWeight: WgtTpe = rand.nextFloat()

  implicit class ToVertexId(vid: String) {
    def toVertexId: VertexId = vid.toLong
  }

  implicit class ToWeight(wgt: String){
    def toWgtTpe: WgtTpe = wgt.toFloat
  }

  implicit class ToVertexType(wgt: String){
    def toVertexType: VertexType = wgt.toShort
  }

  implicit class ToEdgeAttr(attr: String) {
    def toEdgeAttr[ED: ClassTag]: ED = {
      val any = implicitly[ClassTag[ED]].runtimeClass match {
        case ed if ed == classOf[Long] => attr.toLong
        case ed if ed == classOf[Float] => attr.toFloat
        case ed if ed == classOf[Int] => attr.toInt
        case ed if ed == classOf[Double] => attr.toDouble
        case ed if ed == classOf[Boolean] => attr.toBoolean
        case ed if ed == classOf[Char] => attr.toCharArray.head
        case ed if ed == classOf[Byte] => attr.toByte
        case ed if ed == classOf[Short] => attr.toShort
      }

      any.asInstanceOf[ED]
    }
  }

  // srcType, dstType, weight
  implicit class TypedEdgeAttribute(attr: Long) {
    private val buf = ByteBuffer.allocate(8)
    buf.putLong(attr)
    buf.flip()

    var srcType: VertexType =  buf.getShort()

    var dstType: VertexType = buf.getShort()

    var weight: WgtTpe = buf.getFloat

    def toAttr: Long = {
      buf.flip()
      buf.putShort(srcType).putShort(dstType).putFloat(weight)
      buf.flip()
      buf.getLong()
    }

  }

  // srcWeight, dstWeight
  implicit class DoubleWeightEdgeAttribute(attr: Long) {
    private val buf = ByteBuffer.allocate(8)
    buf.putLong(attr)
    buf.flip()

    var srcWeight: WgtTpe = buf.getFloat()

    var dstWeight: WgtTpe = buf.getFloat()

    def toAttr: Long = {
      buf.flip()
      buf.putFloat(srcWeight).putFloat(dstWeight)
      buf.flip()
      buf.getLong()
    }

  }

  class EdgeAttributeBuilder {
    private val buf = ByteBuffer.allocate(8)

    def putSrcType(src: VertexType): this.type = {
      buf.position(0)
      buf.putShort(src)

      this
    }

    def putSrcType(src: String): this.type = {
      buf.position(0)
      buf.putShort(src.toShort)

      this
    }

    def putDstType(dst: VertexType): this.type = {
      buf.position(2)
      buf.putShort(dst)

      this
    }

    def putDstType(dst: String): this.type = {
      buf.position(2)
      buf.putShort(dst.toShort)

      this
    }

    def putWeight(wgt: WgtTpe): this.type = {
      buf.position(4)
      buf.putFloat(wgt)

      this
    }

    def putWeight(wgt: String): this.type = {
      buf.position(4)
      buf.putFloat(wgt.toFloat)

      this
    }

    def put(srcType: String, dstType: String): this.type = {
      buf.position(0)
      buf.putShort(srcType.toShort)
      buf.putShort(dstType.toShort)
      this
    }

    def put(srcType: String, dstType: String, weight:String): this.type = {
      buf.position(0)
      buf.putShort(srcType.toShort)
      buf.putShort(dstType.toShort)
      buf.putFloat(weight.toFloat)

      this
    }

    def put(srcType: VertexType, dstType: VertexType, weight:WgtTpe): this.type = {
      buf.position(0)
      buf.putShort(srcType)
      buf.putShort(dstType)
      buf.putFloat(weight)

      this
    }

    def put(srcType: VertexType, dstType: VertexType): this.type = {
      buf.position(0)
      buf.putShort(srcType)
      buf.putShort(dstType)
      this
    }

    def build: Long = {
      // padding
      val padding = 8 - buf.position()
      if (padding > 0){
        val emptyByte = 0.toByte
        (0 until padding).foreach(_ => buf.put(emptyByte))
      }

      buf.flip()
      val attr = buf.getLong
      buf.flip()
      attr
    }
  }
}