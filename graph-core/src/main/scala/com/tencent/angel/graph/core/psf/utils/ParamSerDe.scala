package com.tencent.angel.graph.core.psf.utils

import com.tencent.angel.common.Serialize
import com.tencent.angel.graph.core.data.GData
import com.tencent.angel.graph.core.psf.common.{NonSplitter, RangeSplitter, Splitter}
import com.tencent.angel.graph.utils.{GUtils, ReflectUtils, SerDe}
import io.netty.buffer.ByteBuf

import scala.reflect.runtime.universe._

object ParamSerDe {

  def serializeSplit(splitter: Splitter, tt: TypeTag[_], params: Any, buf: ByteBuf): Unit = {
    SerDe.javaSerialize(tt, buf)

    val tpe = tt.tpe
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
        } else if (tpe <:< typeOf[Serializable]) {
          SerDe.javaSerialize(params, buf)
        } else if (tpe <:< typeOf[GData]) {
          params.asInstanceOf[GData].serialize(buf)
        } else {
          try {
            SerDe.serialize(params, buf)
          } catch {
            case e: Exception => throw e
          }
        }
    }
  }

  def deserializeSplit(buf: ByteBuf): (TypeTag[_], Any) = {
    val tt = SerDe.javaDeserialize[TypeTag[_]](buf)
    val tpe = tt.tpe

    val params = if (GUtils.isPrimitive(tpe)) {
      SerDe.primitiveFromBuffer(tpe, buf)
    } else if (GUtils.isPrimitiveArray(tpe)) {
      SerDe.arrFromBuffer(tpe.typeArgs.head, buf)
    } else if (GUtils.isFastMap(tpe)) {
      SerDe.fastMapFromBuffer(tpe, buf)
    } else if (tpe <:< typeOf[Serializable]) {
      SerDe.javaDeserialize[Any](buf)
    } else if (tpe <:< typeOf[GData]) {
      val tmp = ReflectUtils.newInstance(tpe)
      tmp.asInstanceOf[GData].deserialize(buf)
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

    (tt, params)
  }

  def bufferLenSplit(splitter: Splitter, tt: TypeTag[_], params: Any): Int = {
    var len = 0

    len += SerDe.javaSerBufferSize(tt)
    val tpe = tt.tpe

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
        } else if (tpe <:< typeOf[Serializable]) {
          len += SerDe.javaSerBufferSize(params)
        } else if (tpe <:< typeOf[GData]) {
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