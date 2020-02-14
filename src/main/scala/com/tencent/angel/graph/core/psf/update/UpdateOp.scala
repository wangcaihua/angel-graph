package com.tencent.angel.graph.core.psf.update

import java.util.concurrent.atomic.AtomicInteger

//import com.tencent.angel.graph.core.psf.utils.CCleaner
import com.tencent.angel.graph.utils.SerDe
import com.tencent.angel.ps.PSContext
import com.tencent.angel.ps.storage.vector.ServerRow
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap

import scala.reflect.runtime.universe._

trait UpdateOp {
  def apply(psContext: PSContext, mId: Int, pId: Int, row: ServerRow, tpe: Type, partParam: Any): Unit
}

object UpdateOp {
  private val ids = new AtomicInteger(0)
  private val cache = new Int2ObjectOpenHashMap[Array[Byte]]()

  def apply(func: (PSContext, Int, Int, ServerRow, Type, Any) => Unit): Int = {
    val fId = ids.getAndIncrement()

    // val cleanedFunc = CCleaner.clean(func)
    val op = new UpdateOp {
      override def apply(psContext: PSContext, mId: Int, pId: Int, row: ServerRow, tpe: Type, partParam: Any): Unit = {
        func(psContext, mId, pId, row, tpe, partParam)
      }
    }

    cache.synchronized{
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