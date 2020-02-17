package com.tencent.angel.graph.core.psf.utils

import com.tencent.angel.common.Serialize
import com.tencent.angel.graph.core.psf.common.{NonSplitter, RangeSplitter, Splitter}
import com.tencent.angel.graph.utils.{GUtils, ReflectUtils, SerDe}
import io.netty.buffer.ByteBuf

import scala.reflect.runtime.universe._

object ParamSerDe {

  def serializeSplit(splitter: Splitter, tpe: Type, params: Any, buf: ByteBuf): Unit = {
    SerDe.serPrimitive(tpe.toString, buf)

    splitter match {
      case split: RangeSplitter =>
        if (GUtils.isPrimitiveArray(tpe)) {
          SerDe.serArr(split.arr, split.start, split.end, buf)
        } else if (GUtils.isFastMap(tpe)) {
          SerDe.serFastMap(params, split.arr, split.start, split.end, buf)
        } else {
          throw new Exception("type error!")
        }
      case _: NonSplitter =>
        if (GUtils.isPrimitive(tpe)) {
          SerDe.serPrimitive(params, buf)
        } else if (tpe <:< typeOf[Serialize]) {
          params.asInstanceOf[Serialize].serialize(buf)
        } else {
          try {
            SerDe.serialize(params, buf)
          } catch {
            case e: Exception => throw e
          }
        }
    }
  }

  def deserializeSplit(buf: ByteBuf): (Type, Any) = {
    val tpe = ReflectUtils.getType(SerDe.primitiveFromBuffer[String](buf))

    val params = if (GUtils.isPrimitive(tpe)) {
      SerDe.primitiveFromBuffer(tpe, buf)
    } else if (GUtils.isPrimitiveArray(tpe)) {
      SerDe.arrFromBuffer(tpe.typeArgs.head, buf)
    } else if (GUtils.isFastMap(tpe)) {
      SerDe.fastMapFromBuffer(tpe, buf)
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

  def bufferLenSplit(splitter: Splitter, tpe: Type, params: Any): Int = {
    var len = 0

    len += SerDe.serPrimitiveBufSize(tpe.toString)

    splitter match {
      case split: RangeSplitter =>
        if (GUtils.isPrimitiveArray(tpe)) {
          len += SerDe.serArrBufSize(split.arr, split.start, split.end)
        } else if (GUtils.isFastMap(tpe)) {
          len += SerDe.serFastMapBufSize(params, split.arr, split.start, split.end)
        } else {
          throw new Exception("type error!")
        }
      case _: NonSplitter =>
        if (GUtils.isPrimitive(tpe)) {
          len += SerDe.serPrimitiveBufSize(params)
        } else if (tpe <:< typeOf[Serialize]) {
          len += params.asInstanceOf[Serialize].bufferLen()
        } else {
          try {
            len += SerDe.bufferLen(params)
          } catch {
            case e: Exception => throw e
          }
        }
    }

    len
  }
}