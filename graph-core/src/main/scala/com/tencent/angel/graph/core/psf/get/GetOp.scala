package com.tencent.angel.graph.core.psf.get

import java.util.concurrent.atomic.AtomicInteger

import com.tencent.angel.graph.core.psf.common.PSFGUCtx
import com.tencent.angel.graph.utils.{ReflectUtils, SerDe}
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap

import scala.reflect.runtime.universe._


trait GetOp extends Serializable {
  def apply(pgParam: PSFGUCtx): (Type, Any)
}


object GetOp {
  private val ids = new AtomicInteger(0)
  private val funcs = new Int2ObjectOpenHashMap[GetOp]()
  private val cache = new Int2ObjectOpenHashMap[Array[Byte]]()

  def apply[U: TypeTag](func: PSFGUCtx => U): GetOp = {

    new GetOp with Serializable {
      override def apply(pgParam: PSFGUCtx): (Type, Any) = {
        val result = func(pgParam)

        ReflectUtils.getType(result) -> result
      }
    }
  }

  def add(getOp: GetOp): Int = {
    val fId = ids.getAndIncrement()

    cache.synchronized {
      funcs.put(fId, getOp)
      cache.put(fId, SerDe.javaSer2Bytes(getOp))
    }

    fId
  }

  def get(fid: Int): Array[Byte] = {
    cache.get(fid)
  }

  def getOp(fid: Int): GetOp = {
    funcs.get(fid)
  }

  def remove(fId: Int): Unit = cache.synchronized {
    if (cache.containsKey(fId)) {
      funcs.remove(fId)
      cache.remove(fId)
    }
  }

}