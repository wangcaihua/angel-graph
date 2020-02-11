package com.tencent.angel.graph.core.psf.update

import java.util

import com.tencent.angel.PartitionKey
import com.tencent.angel.common.Serialize
import com.tencent.angel.graph.core.psf.common.{NonSplitter, RangeSplitter, Splitter}
import com.tencent.angel.graph.core.psf.utils.ParamSerDe
import com.tencent.angel.graph.utils.GUtils
import com.tencent.angel.ml.matrix.psf.update.base.{PartitionUpdateParam, UpdateParam}
import com.tencent.angel.psagent.PSAgentContext
import io.netty.buffer.ByteBuf

import scala.reflect.runtime.universe._

class GUpdateParam[T: TypeTag](mId: Int, uClock: Boolean, params: T)
  extends UpdateParam(mId, uClock) {

  override def split(): util.List[PartitionUpdateParam] = {
    val tpe = typeOf[T]
    val parts: util.List[PartitionKey] = PSAgentContext.get.getMatrixMetaManager
      .getPartitions(matrixId)

    if (GUtils.isPrimitive(tpe) || tpe <:< typeOf[Serialize]) {
      val splits = new util.ArrayList[PartitionUpdateParam](parts.size())
      (0 until parts.size()).foreach { idx =>
        val pp = new GPartitionUpdateParam(matrixId, parts.get(idx), updateClock,
          NonSplitter(), tpe, params)

        splits.set(idx, pp)
      }

      splits
    } else if (tpe =:= typeOf[Array[Int]]) {
      val splitters = RangeSplitter.getSplit[Int](params.asInstanceOf[Array[Int]], parts)

      val splits = new util.ArrayList[PartitionUpdateParam](splitters.size)
      splitters.foreach{ splitter =>
        val pp = new GPartitionUpdateParam(matrixId, splitter.part, updateClock, splitter, tpe, params)
        splits.add(pp)
      }

      splits
    } else if (tpe =:= typeOf[Array[Long]]) {
      val splitters = RangeSplitter.getSplit[Long](params.asInstanceOf[Array[Long]], parts)

      val splits = new util.ArrayList[PartitionUpdateParam](splitters.size)
      splitters.foreach { splitter =>
        val pp = new GPartitionUpdateParam(matrixId, splitter.part, updateClock, splitter, tpe, params)
        splits.add(pp)
      }

      splits
    } else if (GUtils.isIntKeyFastMap(tpe)) {
      val splitters = RangeSplitter.getSplit[Int](GUtils.getIntKeys(params), parts)

      val splits = new util.ArrayList[PartitionUpdateParam](splitters.size)
      splitters.foreach{ splitter =>
        val pp = new GPartitionUpdateParam(matrixId, splitter.part, updateClock, splitter, tpe, params)
        splits.add(pp)
      }

      splits
    } else if (GUtils.isLongKeyFastMap(tpe)) {
      val splitters = RangeSplitter.getSplit[Long](GUtils.getLongKeys(params), parts)

      val splits = new util.ArrayList[PartitionUpdateParam](splitters.size)
      splitters.foreach{ splitter =>
        val pp = new GPartitionUpdateParam(matrixId, splitter.part, updateClock, splitter, tpe, params)
        splits.add(pp)
      }

      splits
    } else {
      throw new Exception("split error, params type is not supported!")
    }
  }
}

object GUpdateParam {
  def empty(matrixId: Int, updateClock: Boolean): GUpdateParam[Byte] = {
    new GUpdateParam[Byte](matrixId, updateClock, 0.toByte)
  }

  def empty(matrixId: Int): GUpdateParam[Byte] = {
    new GUpdateParam[Byte](matrixId, false, 0.toByte)
  }

  def apply[T: TypeTag](mId: Int, uClock: Boolean, params: T) = {
    new GUpdateParam[T](mId, uClock, params)
  }

  def apply[T: TypeTag](mId: Int, params: T) = {
    new GUpdateParam[T](mId, false, params)
  }
}

class GPartitionUpdateParam(mId: Int, part: PartitionKey, uClock: Boolean,
                            @transient splitter: Splitter, var tpe: Type, var params: Any)
  extends PartitionUpdateParam(mId, part, uClock) {

  def this() = this(0, null.asInstanceOf[PartitionKey], false,
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