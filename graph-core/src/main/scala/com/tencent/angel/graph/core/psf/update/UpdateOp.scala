package com.tencent.angel.graph.core.psf.update

import java.util.concurrent.atomic.AtomicInteger

import com.tencent.angel.graph.utils.SerDe
import com.tencent.angel.ps.PSContext
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap

import scala.reflect.runtime.universe._

trait UpdateOp extends Serializable {
  def apply(psContext: PSContext, mId: Int, pId: Int, tpe: Type, partParam: Any): Unit
}

object UpdateOp {
  private val ids = new AtomicInteger(0)
  private val funcs = new Int2ObjectOpenHashMap[UpdateOp]()
  private val cache = new Int2ObjectOpenHashMap[Array[Byte]]()

  def apply(func: (PSContext, Int, Int, Type, Any) => Unit): Int = {
    val fId = ids.getAndIncrement()

    val op = new UpdateOp with Serializable {
      override def apply(psContext: PSContext, mId: Int, pId: Int, tpe: Type, partParam: Any): Unit = {
        func(psContext, mId, pId, tpe, partParam)
      }
    }

    cache.synchronized{
      cache.put(fId, SerDe.javaSer2Bytes(op))
      funcs.put(fId, op)
    }

    fId
  }

  def get(fid: Int): Array[Byte] = {
    cache.get(fid)
  }

  def getOp(fid: Int): UpdateOp = {
    funcs.get(fid)
  }

  def remove(fId: Int): Unit = {
    cache.synchronized {
      if (cache.containsKey(fId)) {
        cache.remove(fId)
      }

      if (funcs.containsKey(fId)) {
        funcs.remove(fId)
      }
    }
  }
}