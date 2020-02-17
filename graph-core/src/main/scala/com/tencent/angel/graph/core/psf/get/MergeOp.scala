package com.tencent.angel.graph.core.psf.get

import java.util.concurrent.atomic.AtomicInteger
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap
import scala.reflect.runtime.universe._

trait MergeOp {
  def apply(tpe: Type, last:Any, curr: Any): Any
}

object MergeOp {
  private val ids = new AtomicInteger(0)
  private val cache = new Int2ObjectOpenHashMap[MergeOp]()


  def apply(func: (Type, Any, Any) => Any): Int = {
    val fId = ids.getAndIncrement()

    val op = new MergeOp {
      override def apply(tpe: Type, last:Any, curr: Any): Any = {
        func(tpe, last, curr)
      }
    }

    cache.synchronized {
      cache.put(fId, op)
    }

    fId
  }

  def get(fid: Int): MergeOp = {
    cache.get(fid)
  }

  def remove(fid: Int): Unit = {
    cache.synchronized {
      if (cache.containsKey(fid)) {
        cache.remove(fid)
      }
    }
  }
}

