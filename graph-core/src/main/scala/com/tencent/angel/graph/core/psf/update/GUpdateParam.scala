package com.tencent.angel.graph.core.psf.update

import java.util

import com.tencent.angel.PartitionKey
import com.tencent.angel.graph.VertexId
import com.tencent.angel.graph.core.psf.common.{NonSplitter, RangeSplitter, Singular, Splitter}
import com.tencent.angel.graph.core.psf.utils.ParamSerDe
import com.tencent.angel.graph.utils.{GUtils, SerDe}
import com.tencent.angel.ml.matrix.psf.update.base.{PartitionUpdateParam, UpdateParam}
import com.tencent.angel.psagent.PSAgentContext
import io.netty.buffer.ByteBuf

import scala.reflect.runtime.universe._

class GUpdateParam[T: TypeTag](mId: Int, uClock: Boolean, params: T, operation: Int)
  extends UpdateParam(mId, uClock) {

  private val tpe = typeOf[T]

  override def split(): util.List[PartitionUpdateParam] = {
    val parts: util.List[PartitionKey] = PSAgentContext.get.getMatrixMetaManager
      .getPartitions(matrixId)

    if (tpe =:= typeOf[Array[VertexId]]) {
      val splitters = RangeSplitter.getSplit(params.asInstanceOf[Array[VertexId]], parts)
      val splits = new util.ArrayList[PartitionUpdateParam](splitters.size)

      splitters.foreach { splitter =>
        val pp = new GPartitionUpdateParam(matrixId, splitter.part, updateClock,
          splitter, tpe, params, operation)
        splits.add(pp)
      }

      splits
    } else if (GUtils.isFastMap(tpe)) {
      val splitters = RangeSplitter.getSplit(GUtils.getFastMapKeys(params), parts)

      val splits = new util.ArrayList[PartitionUpdateParam](splitters.size)
      splitters.foreach { splitter =>
        val pp = new GPartitionUpdateParam(matrixId, splitter.part, updateClock,
          splitter, tpe, params, operation)
        splits.add(pp)
      }

      splits
    } else if (tpe <:< typeOf[Singular]) {
      val splits = new util.ArrayList[PartitionUpdateParam](1)
      val idx = params.asInstanceOf[Singular].partition
      val pp = new GPartitionUpdateParam(matrixId, parts.get(idx), updateClock,
        NonSplitter(), tpe, params, operation)
      splits.add(pp)
      splits
    } else {
      try {
        val splits = new util.ArrayList[PartitionUpdateParam](parts.size)
        (0 until parts.size()).foreach { idx =>
          val pp = new GPartitionUpdateParam(matrixId, parts.get(idx), updateClock,
            NonSplitter(), tpe, params, operation)
          splits.add(pp)
        }

        splits
      } catch {
        case e: Exception => throw e
      }
    }
  }
}

object GUpdateParam {
  def empty(matrixId: Int, updateClock: Boolean, operation: Int): GUpdateParam[Byte] = {
    new GUpdateParam[Byte](matrixId, updateClock, 0.toByte, operation)
  }

  def empty(matrixId: Int, operation: Int): GUpdateParam[Byte] = {
    new GUpdateParam[Byte](matrixId, false, 0.toByte, operation)
  }

  def apply[T: TypeTag](mId: Int, uClock: Boolean, params: T, operation: Int): GUpdateParam[T] = {
    new GUpdateParam[T](mId, uClock, params, operation)
  }

  def apply[T: TypeTag](mId: Int, params: T, operation: Int): GUpdateParam[T] = {
    new GUpdateParam[T](mId, false, params, operation)
  }
}

class GPartitionUpdateParam(mId: Int, part: PartitionKey, uClock: Boolean,
                            splitter: Splitter, var tpe: Type, var params: Any, var operation: Any)
  extends PartitionUpdateParam(mId, part, uClock) {

  def this() = this(0, null, false, null, null, null, null)

  override def serialize(buf: ByteBuf): Unit = {
    super.serialize(buf)
    ParamSerDe.serializeSplit(splitter, tpe, params, buf)

    val dataObj = UpdateOp.get(operation.asInstanceOf[Int])
    buf.writeInt(dataObj.length).writeBytes(dataObj)
  }

  override def deserialize(buf: ByteBuf): Unit = {
    super.deserialize(buf)
    val (t, p) = ParamSerDe.deserializeSplit(buf)

    tpe = t
    params = p
    operation = SerDe.javaDeserialize[UpdateOp](buf)
  }

  override def bufferLen(): Int = {
    var len = super.bufferLen()
    len += ParamSerDe.bufferLenSplit(splitter, tpe, params)
    len += UpdateOp.get(operation.asInstanceOf[Int]).length + 4

    len
  }
}