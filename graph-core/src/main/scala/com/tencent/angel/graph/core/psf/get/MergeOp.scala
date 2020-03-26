package com.tencent.angel.graph.core.psf.get

import java.util.concurrent.atomic.AtomicInteger

import com.tencent.angel.graph.core.psf.common.PSFMCtx
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap

import scala.reflect.runtime.universe._

trait MergeOp extends Serializable {
  def apply(gmParam: PSFMCtx): Any
}

object MergeOp {
  private val ids = new AtomicInteger(0)
  private val cache = new Int2ObjectOpenHashMap[MergeOp]()

  def apply[U: TypeTag](func: PSFMCtx => U): MergeOp = {
    new MergeOp {
      override def apply(gmParam: PSFMCtx): Any = func(gmParam)
    }
  }

  def add(mergeOp: MergeOp): Int = {
    val fId = ids.getAndIncrement()
    cache.synchronized {
      cache.put(fId, mergeOp)
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

