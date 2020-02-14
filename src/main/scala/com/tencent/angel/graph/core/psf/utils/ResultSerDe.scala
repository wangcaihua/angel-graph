package com.tencent.angel.graph.core.psf.utils

import com.tencent.angel.graph.VertexId
import com.tencent.angel.common.Serialize
import com.tencent.angel.ml.math2.vector.Vector
import com.tencent.angel.graph.utils.{GUtils, ReflectUtils, SerDe}
import io.netty.buffer.ByteBuf

import scala.reflect.runtime.universe._

object ResultSerDe {

  def serialize(nodeIds: Array[VertexId], tpe: Type, params: Any, buf: ByteBuf): Unit = {
    SerDe.serPrimitive(tpe.toString, buf)
    assert(GUtils.isIntKeyFastMap(tpe))
    SerDe.serFastMap(params, nodeIds, 0, nodeIds.length, buf)
  }

  def serialize(tpe: Type, params: Any, buf: ByteBuf): Unit = {
    SerDe.serPrimitive(tpe.toString, buf)

    if (GUtils.isPrimitive(tpe)) {
      SerDe.serPrimitive(params, buf)
    } else if (GUtils.isPrimitiveArray(tpe)) {
      SerDe.serArr(tpe.typeArgs.head, params, buf)
    } else if (GUtils.isFastMap(tpe)) {
      SerDe.serFastMap(params, buf)
    } else if (GUtils.isVector(tpe)) {
      SerDe.serVector(params.asInstanceOf[Vector], buf)
    } else if (tpe.weak_<:<(typeOf[Serialize])) {
      params.asInstanceOf[Serialize].serialize(buf)
    } else {
      try {
        SerDe.serPrimitive(params, buf)
      } catch {
        case e: Exception => throw e
      }
    }
  }

  def deserialize(buf: ByteBuf): (Type, Any) = {
    val tpe = ReflectUtils.getType(SerDe.primitiveFromBuffer[String](buf))

    val params = if (GUtils.isPrimitive(tpe)) {
      SerDe.primitiveFromBuffer(tpe, buf)
    } else if (GUtils.isPrimitiveArray(tpe)) {
      SerDe.arrFromBuffer(tpe, buf)
    } else if (GUtils.isFastMap(tpe)) {
      SerDe.fastMapFromBuffer(tpe, buf)
    } else if (GUtils.isVector(tpe)) {
      SerDe.vectorFromBuffer(tpe, buf)
    } else if (tpe <:< typeOf[Serialize]) {
      val tmp = ReflectUtils.newInstance(tpe)
      tmp.asInstanceOf[Serialize].deserialize(buf)
      tmp
    } else {
      try {
        val tmp = ReflectUtils.newInstance(tpe)
        SerDe.deserialize(tmp, buf)
        tmp
      } catch {
        case e: Exception => throw e
      }
    }

    (tpe, params)
  }

  def bufferLen(nodeIds: Array[VertexId], tpe: Type, params: Any): Int = {
    var len = SerDe.serPrimitiveBufSize(tpe.toString)
    len += SerDe.serFastMapBufSize(params, nodeIds, 0, nodeIds.length)

    len
  }

  def bufferLen(tpe: Type, params: Any): Int = {
    var len = SerDe.serPrimitiveBufSize(tpe.toString)

    if (GUtils.isPrimitive(tpe)) {
      len += SerDe.serPrimitiveBufSize(params)
    } else if (GUtils.isPrimitiveArray(tpe)) {
      len += SerDe.serArrBufSize(params)
    } else if (GUtils.isFastMap(tpe)) {
      len += SerDe.serFastMapBufSize(params)
    } else if (GUtils.isVector(tpe)) {
      len += SerDe.serVectorBufSize(params.asInstanceOf[Vector])
    } else {
      try {
        len += SerDe.bufferLen(params)
      } catch {
        case e: Exception => throw e
      }
    }

    len
  }
}