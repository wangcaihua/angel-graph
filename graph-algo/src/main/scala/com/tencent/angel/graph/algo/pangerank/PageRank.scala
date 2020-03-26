package com.tencent.angel.graph.algo.pangerank

import java.nio.ByteBuffer

import com.tencent.angel.graph.core.psf.common.{PSFGUCtx, PSFMCtx}
import com.tencent.angel.graph.framework.EdgeDirection.EdgeDirection
import com.tencent.angel.graph.framework.{EdgeDirection, EdgeTriplet, Graph, Pregel}
import com.tencent.angel.graph.utils.psfConverters._
import com.tencent.angel.graph.utils.{FastHashMap, Logging}
import com.tencent.angel.graph.{VertexId, _}

object PageRank extends Logging with Serializable {
  def apply(graph: Graph[Float, Long], numIter: Int, batchSize: Int,
    resetProb: Float = 0.15f, activeDirection: EdgeDirection = EdgeDirection.Both): Graph[Float, Long] = {

    val degreeSlotName = "degreeBoth"
    graph.calDegree(degreeSlotName)

    val psMatrix = graph.psVertices
    graph.edges.foreachPartition { iter =>
      val edgePartition = iter.next()

      val pullDegree = psMatrix.createGet { ctx: PSFGUCtx =>
        val part = ctx.getPartition[Float]
        part.getSlot[Int](degreeSlotName).asFastHashMap
      } { ctx: PSFMCtx =>
        val last = ctx.getLast[FastHashMap[VertexId, Int]]
        val curr = ctx.getCurr[FastHashMap[VertexId, Int]]

        last.merge(curr)
      }

      val degrees = pullDegree(edgePartition.local2global)

      val buf = ByteBuffer.allocate(8)
      (0 until edgePartition.size).foreach { pos =>
        val srcId = edgePartition.srcIdFromPos(pos)
        val dstId = edgePartition.dstIdFromPos(pos)

        val srcWeight = 1.0f / degrees(srcId)
        val dstWeight = 1.0f / degrees(dstId)
        buf.putFloat(srcWeight).putFloat(dstWeight)
        buf.flip()
        val attr = buf.getLong()
        buf.flip()

        edgePartition.updateEdgeAttrsByPos(pos, attr)
      }
    }

    def vprog(vd: Float, m: Float): Float = resetProb * vd + (1 - resetProb) * m

    def active(vd: Float, m: Float): Boolean = Math.abs((1 - resetProb) * (m - vd)) >= 1e-6

    Pregel(graph, initialMsg = 0.0f, maxIterations = numIter,
      batchSize = batchSize, activeDirection = activeDirection)(
      vprog, sendMsg, mergeMsg, active)

    graph
  }

  private def sendMsg(ctx: EdgeTriplet[Float, Long]): Iterator[(VertexId, Float)] = {
    Iterator(
      ctx.dstId -> ctx.srcAttr * ctx.attr.srcWeight,
      ctx.srcId -> ctx.dstAttr * ctx.attr.dstWeight
    )
  }

  private def mergeMsg(m1: Float, m2: Float): Float = m1 + m2
}