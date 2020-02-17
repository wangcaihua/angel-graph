package com.tencent.angel.graph.core.psf.get

import java.util.concurrent.atomic.AtomicInteger

import com.tencent.angel.graph.utils.SerDe
import com.tencent.angel.ps.PSContext
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap

import scala.reflect.runtime.universe._

trait GetOp extends Serializable {
  def apply(psContext: PSContext, mId: Int, pId: Int, tpe: Type, partParam: Any): (Type, Any)
}


object GetOp {
  private val ids = new AtomicInteger(0)
  private val cache = new Int2ObjectOpenHashMap[Array[Byte]]()

  def apply(func: (PSContext, Int, Int, Type, Any) => (Type, Any)): Int = {
    val fId = ids.getAndIncrement()

    val op = new GetOp with Serializable {
      override def apply(psContext: PSContext, mId: Int, pId: Int, tpe: Type, partParam: Any): (Type, Any) = {
        func(psContext, mId, pId, tpe, partParam)
      }
    }

    cache.synchronized {
      cache.put(fId, SerDe.javaSer2Bytes(op))
    }

    fId
  }

  def get(fid: Int): Array[Byte] = {
    cache.get(fid)
  }

  def remove(fId: Int): Unit = {
    cache.synchronized {
      if (cache.containsKey(fId)) {
        cache.remove(fId)
      }
    }
  }
}