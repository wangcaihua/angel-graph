package com.tencent.angel.graph.framework

import com.tencent.angel.graph.WgtTpe
import com.tencent.angel.graph.core.data._
import com.tencent.angel.graph.core.psf.common.PSFGUCtx
import com.tencent.angel.graph.framework.EdgeDirection.EdgeDirection
import com.tencent.angel.graph.utils._

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

import com.tencent.angel.graph._
import com.tencent.angel.graph.utils.psfConverters._


class GraphOps[VD: ClassTag : TypeTag, ED: ClassTag](graph: Graph[VD, ED]) {
  private val psVertices = graph.psVertices
  private val edges = graph.edges

  def adjacency[N <: Neighbor : ClassTag : TypeTag](direction: EdgeDirection): this.type = {
  edges.foreachPartition { iter =>
    val edgePartition = iter.next()

    // 1. create Adjacency in spark RDD partition
    var pos = 0
    val adjBuilder = new PartitionUnTypedNeighborBuilder[N](direction)
    typeOf[N] match {
      case nt if nt =:= typeOf[NeighN] =>
        while (pos < edgePartition.size) {
          adjBuilder.add(edgePartition.srcIdFromPos(pos), edgePartition.dstIdFromPos(pos))
          pos += 1
        }
      case nt if nt =:= typeOf[NeighNW] =>
        while (pos < edgePartition.size) {
          adjBuilder.add(edgePartition.srcIdFromPos(pos),
            edgePartition.dstIdFromPos(pos),
            edgePartition.attrs(pos).asInstanceOf[WgtTpe])
          pos += 1
        }
      case _ =>
        throw new Exception("Adjacency data error!")
    }

    // 2. create PSF to push Adjacency (from executor)
    val pushAdjacency = psVertices.createUpdate { ctx: PSFGUCtx =>
      val psPartition = ctx.getPartition[VD]
      val psAdjBuilder = psPartition.getOrCreateSlot[UnTypedNeighborBuilder]("adjacencyBuilder")
      ctx.getMapParam[N].foreach { case (vid, neigh) =>
        if (psAdjBuilder.containsKey(vid)) {
          psAdjBuilder(vid).add(neigh)
        } else {
          val builder = new UnTypedNeighborBuilder()
          builder.add(neigh)
          psAdjBuilder(vid) = builder
        }
      }
    }

    // 3. push adjacency (on executor)
    pushAdjacency(adjBuilder.build)

    // 4. remove PSF (on executor)
    pushAdjacency.clear()
  }

  // create adjacency on PS (from driver)
  val createAdjacency = psVertices.createUpdate { ctx: PSFGUCtx =>
    val psPartition = ctx.getPartition[VD]
    val psAdjBuilder = psPartition.getSlot[UnTypedNeighborBuilder]("adjacencyBuilder")
    if (psAdjBuilder != null) {
      val psAdjacency = psPartition.getOrCreateSlot[N]("adjacency")

      psAdjBuilder.foreach { case (vid, adj) =>
        psAdjacency(vid) = adj.clearAndBuild[N](vid)
      }

      psPartition.removeSlot("adjacencyBuilder")
    } else {
      psPartition.getOrCreateSlot[N]("adjacency")
    }
  }
  createAdjacency()
  createAdjacency.clear() // remove PSF (on driver)

  this
}

  def typedAdjacency[N <: Neighbor : ClassTag : TypeTag](direction: EdgeDirection): this.type = {
    edges.foreachPartition { iter =>
      val edgePartition = iter.next()

      // 1. create Adjacency in spark RDD partition
      var pos = 0
      val adjBuilder = new PartitionTypedNeighborBuilder[N](direction)
      typeOf[N] match {
        case nt if nt =:= typeOf[NeighTN] =>
          while (pos < edgePartition.size) {
            val srcId = edgePartition.srcIdFromPos(pos)
            val dstId = edgePartition.dstIdFromPos(pos)
            val attr = edgePartition.attrs(pos).asInstanceOf[Long]
            val srcType = attr.srcType
            val dstType = attr.dstType
            adjBuilder.add(srcId, srcType, dstId, dstType)
            pos += 1
          }
        case nt if nt =:= typeOf[NeighTNW] =>
          while (pos < edgePartition.size) {
            val srcId = edgePartition.srcIdFromPos(pos)
            val dstId = edgePartition.dstIdFromPos(pos)
            val attr = edgePartition.attrs(pos).asInstanceOf[Long]
            val srcType = attr.srcType
            val dstType = attr.dstType
            val weight = attr.weight
            adjBuilder.add(srcId, srcType, dstId, dstType, weight)
            pos += 1
          }
        case _ =>
          throw new Exception("Adjacency data error!")
      }

      // 2. create PSF to push Adjacency (from executor)
      val pushAdjacency = psVertices.createUpdate { ctx: PSFGUCtx =>
        val psPartition = ctx.getPartition[VD]
        val psAdjBuilder = psPartition.getOrCreateSlot[TypedNeighborBuilder]("adjacencyBuilder")
        ctx.getMapParam[N].foreach { case (vid, neigh) =>
          if (psAdjBuilder.containsKey(vid)) {
            psAdjBuilder(vid).add(neigh)
          } else {
            val builder = new TypedNeighborBuilder()
            builder.add(neigh)
            psAdjBuilder(vid) = builder
          }
        }
      }

      // 3. push adjacency (on executor)
      pushAdjacency(adjBuilder.build)

      // 4. remove PSF (on executor)
      pushAdjacency.clear()
    }

    // create adjacency on PS (from driver)
    val createAdjacency = psVertices.createUpdate { ctx: PSFGUCtx =>
      val psPartition = ctx.getPartition[VD]
      val psAdjBuilder = psPartition.getSlot[TypedNeighborBuilder]("adjacencyBuilder")
      if (psAdjBuilder != null) {
        val psAdjacency = psPartition.getOrCreateSlot[N]("adjacency")

        psAdjBuilder.foreach { case (vid, adj) =>
          psAdjacency(vid) = adj.clearAndBuild[N](vid)
        }

        psPartition.removeSlot("adjacencyBuilder")
      } else {
        psPartition.getOrCreateSlot[N]("adjacency")
      }
    }
    createAdjacency()
    createAdjacency.clear() // remove PSF (on driver)

    this
  }

}
