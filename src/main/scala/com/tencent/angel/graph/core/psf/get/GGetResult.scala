package com.tencent.angel.graph.core.psf.get

import com.tencent.angel.graph.utils.{GUtils, ReflectUtils, SerDe}
import com.tencent.angel.ml.matrix.psf.get.base.{GetResult, PartitionGetResult}
import io.netty.buffer.ByteBuf
import com.tencent.angel.ml.math2.vector._

import scala.reflect.runtime.universe._

case class GGetResult(value: Any) extends GetResult

class GPartitionGetResult(var tpe: Type, var pResult: Any, var mergeFuncId: Int) extends PartitionGetResult {

  def this() = this(null.asInstanceOf[Type], null.asInstanceOf[Any], -1)

  override def serialize(byteBuf: ByteBuf): Unit = {
    SerDe.serPrimitive(tpe.toString, byteBuf)

    tpe match {
      case t if GUtils.isPrimitive(tpe) => SerDe.serPrimitive(pResult, byteBuf)
      case t if GUtils.isPrimitiveArray(tpe) => SerDe.serArr(tpe.typeArgs.head, pResult, byteBuf)
      case t if GUtils.isFastMap(tpe) => SerDe.serFastMap(pResult, byteBuf)
      case t if GUtils.isVector(tpe) => SerDe.serVector(pResult.asInstanceOf[Vector], byteBuf)
      case _ => SerDe.serialize(pResult, ReflectUtils.getFields(tpe), byteBuf)
    }

    byteBuf.writeInt(mergeFuncId)
  }

  override def bufferLen(): Int = {
    val tpeLen = SerDe.serPrimitiveBufSize(tpe.toString)

    val dataLen = tpe match {
      case t if GUtils.isPrimitive(tpe) => SerDe.serPrimitiveBufSize(pResult)
      case t if GUtils.isPrimitiveArray(tpe) => SerDe.serArrBufSize(tpe.typeArgs.head, pResult)
      case t if GUtils.isFastMap(tpe) => SerDe.serFastMapBufSize(pResult)
      case t if GUtils.isVector(tpe) => SerDe.serVectorBufSize(pResult.asInstanceOf[Vector])
      case _ => SerDe.bufferLen(pResult, ReflectUtils.getFields(tpe))
    }

    tpeLen + dataLen + 4
  }

  override def deserialize(byteBuf: ByteBuf): Unit = {
    tpe = ReflectUtils.getType(SerDe.primitiveFromBuffer[String](byteBuf))

    tpe match {
      case t if GUtils.isPrimitive(tpe) =>
        pResult = SerDe.primitiveFromBuffer(tpe, byteBuf)
      case t if GUtils.isPrimitiveArray(tpe) =>
        pResult = SerDe.arrFromBuffer(tpe.typeArgs.head, byteBuf)
      case t if GUtils.isFastMap(tpe) =>
        pResult = SerDe.fastMapFromBuffer(tpe, byteBuf)
      case t if GUtils.isVector(tpe) =>
        pResult = SerDe.vectorFromBuffer(tpe, byteBuf)
      case t =>
        pResult = ReflectUtils.newInstance(t)
        SerDe.deserialize(pResult, ReflectUtils.getFields(tpe), byteBuf)
    }

    mergeFuncId = byteBuf.readInt()
  }
}