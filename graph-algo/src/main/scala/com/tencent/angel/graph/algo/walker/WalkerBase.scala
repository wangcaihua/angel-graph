package com.tencent.angel.graph.algo.walker


import com.tencent.angel.graph.VertexId
import com.tencent.angel.graph.core.psf.common.{PSFGUCtx, PSFMCtx, Singular}
import com.tencent.angel.graph.core.psf.update.UpdatePSF
import com.tencent.angel.graph.framework.EdgeDirection.EdgeDirection
import com.tencent.angel.graph.framework.{EdgeDirection, EdgePartition, Graph}
import com.tencent.angel.graph.utils.{FastArray, FastHashMap, Logging}
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.spark.models.PSMatrix
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import com.tencent.angel.graph.utils.psfConverters._

abstract class WalkerBase[N : ClassTag : TypeTag, ED: ClassTag](graph: Graph[N, ED]) extends Logging with Serializable {
  protected var pathLength: Int = 10
  protected var epoch: Int = 1
  protected var batchSize: Int = 3000
  protected var activeDirection: EdgeDirection = EdgeDirection.Both

  protected val psVertices: PSMatrix = graph.psVertices
  protected val edgesRDD: RDD[EdgePartition[N, ED]] = graph.edges
  protected val slotName: String = "walkPath"

  protected val numPSPartition: Int = PSAgentContext.get.getMatrixMetaManager
    .getPartitions(psVertices.id, 0).size()
  protected val partBatchSize: Int = (batchSize + numPSPartition - 1) / numPSPartition

  def setPathLength(pathLength: Int): this.type = {
    this.pathLength = pathLength
    this
  }

  def setEpoch(epoch: Int): this.type = {
    this.epoch = epoch
    this
  }

  def setBatchSize(batchSize: Int): this.type = {
    this.batchSize = batchSize
    this
  }

  def setActiveDirection(activeDirection: EdgeDirection): this.type = {
    this.activeDirection = activeDirection
    this
  }

  def samplePath(): Graph[N, ED]

  def save(path: String): Unit = {
    val pathRDD = graph.sparkContext.parallelize(0 until numPSPartition, numPSPartition).mapPartitions{ iter =>
      val psPartId = iter.next()

      val pullPathPSF = psVertices.createGet{ ctx: PSFGUCtx =>
        assert(ctx.getParam[Singular].partition == ctx.partitionId)
        val psPartition = ctx.getPartition[N]
        val slot = psPartition.getSlot[FastArray[VertexId]](slotName)

        slot.mapValues(_.trim().array).asFastHashMap
      } { ctx => PSFMCtx
        val last = ctx.getLast[FastHashMap[VertexId, Array[VertexId]]]
        val curr = ctx.getCurr[FastHashMap[VertexId, Array[VertexId]]]

        last.merge(curr)
      }

      pullPathPSF(Singular(psPartId)).iterator
    }

    pathRDD.saveAsTextFile(path)
  }
}

