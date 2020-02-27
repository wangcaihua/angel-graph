package com.tencent.angel

import java.nio.ByteBuffer

import com.tencent.angel.graph.utils.FastHashSet

import scala.util.Random

package object graph {
  private val rand = new Random()

  type VertexId = Long
  type VertexType = Short
  type WgtTpe = Float
  type PartitionID = Int

  type VertexSet = FastHashSet[VertexId]

  val defaultWeight: WgtTpe = 0.0f

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

  implicit class EdgeAttribute(attr: Long) {
    private val buf = ByteBuffer.allocate(8)
    buf.putLong(attr)
    buf.flip()

    var srcType: VertexType = buf.getShort()

    var dstType: VertexType = buf.getShort()

    var weight: WgtTpe = buf.getFloat

    def toAttr: Long = {
      buf.flip()
      buf.putShort(srcType).putShort(dstType).putFloat(weight)
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