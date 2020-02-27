package com.tencent.angel.graph.core.psf.common

import com.tencent.angel.graph.VertexId
import com.tencent.angel.graph.core.data.PSPartition
import com.tencent.angel.graph.utils.FastHashMap
import com.tencent.angel.ps.PSContext

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

case class PSFGUCtx(psContext: PSContext, matrixId: Int, partitionId: Int,
                    private val tpe: Type, private val param: Any) {
  lazy val key: String = s"$matrixId-$partitionId"

  def getParam[T: TypeTag]: T = {
    assert(tpe =:= typeOf[T])
    param.asInstanceOf[T]
  }

  def getMapParam[V: ClassTag]: FastHashMap[VertexId, V] = {
    FastHashMap.fromUnimi[VertexId, V](param)
  }

  def getArrayParam: Array[VertexId] = {
    assert(tpe =:= typeOf[Array[VertexId]])
    param.asInstanceOf[Array[VertexId]]
  }

  def put(vid: VertexId): this.type = {
    val builder = PSPartition.getOrCreateBuilder(key)
    builder.put(vid)

    this
  }

  def put(vids: Array[VertexId]): this.type = {
    val builder = PSPartition.getOrCreateBuilder(key)
    builder.put(vids)

    this
  }

  def getOrCreatePartition[VD: ClassTag]: PSPartition[VD] = {
    PSPartition.getOrCreate[VD](key)
  }

  def getPartition[VD: ClassTag]: PSPartition[VD] = {
    PSPartition.get[VD](key)
  }

}
