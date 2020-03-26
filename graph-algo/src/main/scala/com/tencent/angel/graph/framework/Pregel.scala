package com.tencent.angel.graph.framework

import com.tencent.angel.graph.VertexId
import com.tencent.angel.graph.framework.EdgeActiveness.EdgeActiveness
import com.tencent.angel.graph.framework.EdgeDirection.EdgeDirection
import com.tencent.angel.graph.utils.Logging

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

object Pregel extends Logging with Serializable {
  def apply[VD: ClassTag : TypeTag, ED: ClassTag, M: ClassTag : TypeTag]
  (graph: Graph[VD, ED], initialMsg: M,
   maxIterations: Int = Int.MaxValue, batchSize: Int = -1,
   activeDirection: EdgeDirection = EdgeDirection.Either)
  (vprog: (VD, M) => VD,
   sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, M)],
   mergeMsg: (M, M) => M,
   active: (VD, M) => Boolean = (vd: VD, m: M) => true): Graph[VD, ED] = {

    require(maxIterations > 0, s"Maximum number of iterations must be greater than 0, but got $maxIterations")

    val activeness: EdgeActiveness = activeDirection match {
      case EdgeDirection.Both => EdgeActiveness.Both
      case EdgeDirection.Either => EdgeActiveness.Either
      case EdgeDirection.Out => EdgeActiveness.SrcOnly
      case EdgeDirection.In => EdgeActiveness.DstOnly
      case _ => EdgeActiveness.Neither
    }

    logInfo("initVertex")
    graph.initVertex(vprog, active, initialMsg)

    // compute the messages
    mapReduceTriplets(graph, sendMsg, mergeMsg, batchSize, activeness)
    graph.updateVertex(vprog, active)
    var activeMessages = graph.activeMessageCount()

    // Loop
    var i = 0
    while (activeMessages > 0 && i < maxIterations) {
      // Receive the messages and update the vertices.
      mapReduceTriplets(graph, sendMsg, mergeMsg, batchSize, activeness)
      graph.updateVertex(vprog, active)
      activeMessages = graph.activeMessageCount()

      i += 1
    }

    graph
  } // end of apply

  private def mapReduceTriplets[VD: ClassTag, ED: ClassTag,
  M: ClassTag : TypeTag](graph: Graph[VD, ED],
                         mapFunc: EdgeTriplet[VD, ED] => Iterator[(VertexId, M)],
                         reduceFunc: (M, M) => M,
                         batchSize: Int = -1,
                         activeness: EdgeActiveness): Unit = {
    def sendMsg(ctx: EdgeContext[VD, ED, M]) {
      mapFunc(ctx.toEdgeTriplet).foreach { kv =>
        val id = kv._1
        val msg = kv._2
        if (id == ctx.srcId) {
          ctx.sendToSrc(msg)
        } else {
          assert(id == ctx.dstId)
          ctx.sendToDst(msg)
        }
      }
    }

    graph.aggregateMessagesWithActiveSet(sendMsg, reduceFunc, TripletFields.All, batchSize, activeness)
  }
}
