package com.tencent.angel.graph.core.psf.update

import java.util.concurrent.atomic.AtomicInteger

import com.tencent.angel.graph.core.psf.common.PSFGUCtx
import com.tencent.angel.graph.utils.SerDe
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap


trait UpdateOp extends Serializable {
  def apply(puParam: PSFGUCtx): Unit
}

object UpdateOp {
  private val ids = new AtomicInteger(0)
  private val funcs = new Int2ObjectOpenHashMap[UpdateOp]()
  private val cache = new Int2ObjectOpenHashMap[Array[Byte]]()

  def apply(func: PSFGUCtx => Unit): UpdateOp = {
    new UpdateOp with Serializable {
      override def apply(puParam: PSFGUCtx): Unit = func(puParam)
    }
  }

  def add(updateOp: UpdateOp): Int = {
    val fId = ids.getAndIncrement()
    cache.synchronized {
      cache.put(fId, SerDe.javaSer2Bytes(updateOp))
      funcs.put(fId, updateOp)
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