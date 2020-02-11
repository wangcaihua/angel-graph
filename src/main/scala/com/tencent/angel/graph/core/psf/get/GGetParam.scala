package com.tencent.angel.graph.core.psf.get

import java.util

import com.tencent.angel.PartitionKey
import com.tencent.angel.common.Serialize
import com.tencent.angel.graph.core.psf.common.{NonSplitter, RangeSplitter, Splitter}
import com.tencent.angel.graph.core.psf.utils.ParamSerDe
import com.tencent.angel.graph.utils.GUtils

import scala.reflect.runtime.universe._
import com.tencent.angel.ml.matrix.psf.get.base.{GetParam, PartitionGetParam}
import com.tencent.angel.psagent.PSAgentContext
import io.netty.buffer.ByteBuf

class GGetParam[T: TypeTag](mId: Int, params: T) extends GetParam(mId) {
  
  override def split(): util.List[PartitionGetParam] = {
    val tpe = typeOf[T]
    val parts: util.List[PartitionKey] = PSAgentContext.get.getMatrixMetaManager
      .getPartitions(matrixId)

    if (GUtils.isPrimitive(tpe) || tpe <:< typeOf[Serialize]) {
      val splits = new util.ArrayList[PartitionGetParam](parts.size())
      (0 until parts.size()).foreach { idx =>
        val pp = new GPartitionGetParam(matrixId, parts.get(idx),
          NonSplitter(), tpe, params)

        splits.set(idx, pp)
      }

      splits
    } else if (tpe =:= typeOf[Array[Int]]) {
      val splitters = RangeSplitter.getSplit[Int](params.asInstanceOf[Array[Int]], parts)

      val splits = new util.ArrayList[PartitionGetParam](splitters.size)
      splitters.foreach{ splitter =>
        val pp = new GPartitionGetParam(matrixId, splitter.part, splitter, tpe, params)
        splits.add(pp)
      }

      splits
    } else if (tpe =:= typeOf[Array[Long]]) {
      val splitters = RangeSplitter.getSplit[Long](params.asInstanceOf[Array[Long]], parts)

      val splits = new util.ArrayList[PartitionGetParam](splitters.size)
      splitters.foreach { splitter =>
        val pp = new GPartitionGetParam(matrixId, splitter.part, splitter, tpe, params)
        splits.add(pp)
      }

      splits
    } else if (GUtils.isIntKeyFastMap(tpe)) {
      val splitters = RangeSplitter.getSplit[Int](GUtils.getIntKeys(params), parts)

      val splits = new util.ArrayList[PartitionGetParam](splitters.size)
      splitters.foreach{ splitter =>
        val pp = new GPartitionGetParam(matrixId, splitter.part, splitter, tpe, params)
        splits.add(pp)
      }

      splits
    } else if (GUtils.isLongKeyFastMap(tpe)) {
      val splitters = RangeSplitter.getSplit[Long](GUtils.getLongKeys(params), parts)

      val splits = new util.ArrayList[PartitionGetParam](splitters.size)
      splitters.foreach{ splitter =>
        val pp = new GPartitionGetParam(matrixId, splitter.part, splitter, tpe, params)
        splits.add(pp)
      }

      splits
    } else {
      throw new Exception("split error, params type is not supported!")
    }
  }
}

object GGetParam {
  def empty(matrixId: Int): GGetParam[Byte] = {
    new GGetParam[Byte](matrixId, 0.toByte)
  }

  def apply[T: TypeTag](mId: Int, params: T) = {
    new GGetParam[T](mId, params)
  }
}


class GPartitionGetParam(mId: Int, pKey: PartitionKey,
                         @transient splitter: Splitter, var tpe: Type, var params: Any)
  extends PartitionGetParam(mId, pKey) {

  def this() = this(0, null.asInstanceOf[PartitionKey],
    null.asInstanceOf[Splitter], null.asInstanceOf[Type], null.asInstanceOf[Any])

  override def serialize(buf: ByteBuf): Unit = {
    super.serialize(buf)
    ParamSerDe.serializeSplit(splitter, tpe, params, buf)
  }

  override def deserialize(buf: ByteBuf): Unit = {
    super.deserialize(buf)
    val (t, p) = ParamSerDe.deserializeSplit(buf)

    tpe = t
    params = p
  }

  override def bufferLen(): Int = {
    super.bufferLen() + ParamSerDe.bufferLenSplit(splitter, tpe, params)
  }
}
