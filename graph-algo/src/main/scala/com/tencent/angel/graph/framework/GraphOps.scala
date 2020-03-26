package com.tencent.angel.graph.framework

import com.tencent.angel.graph.core.data._
import com.tencent.angel.graph.{WgtTpe, _}
import com.tencent.angel.graph.core.psf.common.PSFGUCtx
import com.tencent.angel.graph.framework.EdgeDirection.EdgeDirection
import com.tencent.angel.graph.utils._
import com.tencent.angel.graph.utils.psfConverters._

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag


class GraphOps[VD: ClassTag : TypeTag, ED: ClassTag](graph: Graph[VD, ED]) extends Serializable with Logging {

  private val psVertices = graph.psVertices
  private val edges = graph.edges

  def computeAndPushNeigh2Attr(direction: EdgeDirection, batchSize: Int = -1): Graph[VD, ED] = {
    edges.foreachPartition { iter =>
      val edgePartition = iter.next()

      // 1. create Adjacency in spark RDD partition
      var pos = 0
      val neighType = implicitly[ClassTag[VD]].runtimeClass
      val builder = neighType match {
        case nt if nt == classOf[NeighN] =>
          val adjBuilder = new PartitionUnTypedNeighborBuilder[NeighN](direction)
          while (pos < edgePartition.size) {
            adjBuilder.add(edgePartition.srcIdFromPos(pos), edgePartition.dstIdFromPos(pos))
            pos += 1
          }

          adjBuilder
        case nt if nt == classOf[NeighNW] =>
          val adjBuilder = new PartitionUnTypedNeighborBuilder[NeighNW](direction)
          while (pos < edgePartition.size) {
            adjBuilder.add(edgePartition.srcIdFromPos(pos),
              edgePartition.dstIdFromPos(pos),
              edgePartition.attrs(pos).asInstanceOf[WgtTpe])
            pos += 1
          }

          adjBuilder
        case nt if nt == classOf[NeighTN] =>
          val adjBuilder = new PartitionTypedNeighborBuilder[NeighTN](direction)
          while (pos < edgePartition.size) {
            val srcId = edgePartition.srcIdFromPos(pos)
            val dstId = edgePartition.dstIdFromPos(pos)
            val attr = new TypedEdgeAttribute(edgePartition.attrs(pos).asInstanceOf[Long])
            val srcType = attr.srcType
            val dstType = attr.dstType
            adjBuilder.add(srcId, srcType, dstId, dstType)
            pos += 1
          }

          adjBuilder
        case nt if nt == classOf[NeighTNW] =>
          val adjBuilder = new PartitionTypedNeighborBuilder[NeighTNW](direction)
          while (pos < edgePartition.size) {
            val srcId = edgePartition.srcIdFromPos(pos)
            val dstId = edgePartition.dstIdFromPos(pos)
            val attr = new TypedEdgeAttribute(edgePartition.attrs(pos).asInstanceOf[Long])
            val srcType = attr.srcType
            val dstType = attr.dstType
            val weight = attr.weight
            adjBuilder.add(srcId, srcType, dstId, dstType, weight)
            pos += 1
          }

          adjBuilder
        case _ =>
          throw new Exception("Adjacency data error!")
      }

      // 2. create PSF to push Adjacency (from executor)
      val pushAdjacency = psVertices.createUpdate { ctx: PSFGUCtx =>
        val neighs = ctx.getMapParam[VD]
        val psPartition = ctx.getPartition[VD]

        neighs.foreach {
          case (vid, neigh: NeighN) =>
            val attr = psPartition.getAttr(vid)
            val newAttr = if (attr != null) {
              attr.asInstanceOf[NeighN]
                .mergeSorted(neigh).asInstanceOf[VD]
            } else {
              neigh.asInstanceOf[VD]
            }
            psPartition.setAttr(vid, newAttr)
          case (vid, neigh: NeighNW) =>
            val attr = psPartition.getAttr(vid)
            val newAttr = if (attr != null) {
              attr.asInstanceOf[NeighNW]
                .mergeSorted(neigh).asInstanceOf[VD]
            } else {
              neigh.asInstanceOf[VD]
            }
            psPartition.setAttr(vid, newAttr)
          case (vid, neigh: NeighTN) =>
            val attr = psPartition.getAttr(vid)
            val newAttr = if (attr != null) {
              attr.asInstanceOf[NeighTN]
                .mergeSorted(neigh).asInstanceOf[VD]
            } else {
              neigh.asInstanceOf[VD]
            }
            psPartition.setAttr(vid, newAttr)
          case (vid, neigh: NeighTNW) =>
            val attr = psPartition.getAttr(vid)
            val newAttr = if (attr != null) {
              attr.asInstanceOf[NeighTNW]
                .mergeSorted(neigh).asInstanceOf[VD]
            } else {
              neigh.asInstanceOf[VD]
            }
            psPartition.setAttr(vid, newAttr)
        }
      }

      // 3. push adjacency (on executor)
      (builder, neighType) match {
        case (b: PartitionUnTypedNeighborBuilder[_], n) if n == classOf[NeighN]=>
          pushAdjacency(b.asInstanceOf[PartitionUnTypedNeighborBuilder[NeighN]].build, batchSize)
        case (b: PartitionUnTypedNeighborBuilder[_], n) if n == classOf[NeighNW]=>
          pushAdjacency(b.asInstanceOf[PartitionUnTypedNeighborBuilder[NeighNW]].build, batchSize)
        case (b: PartitionTypedNeighborBuilder[_], n) if n == classOf[NeighTN]=>
          pushAdjacency(b.asInstanceOf[PartitionTypedNeighborBuilder[NeighTN]].build, batchSize)
        case (b: PartitionTypedNeighborBuilder[_], n) if n == classOf[NeighTNW]=>
          pushAdjacency(b.asInstanceOf[PartitionTypedNeighborBuilder[NeighTNW]].build, batchSize)
      }

      // 4. remove PSF (on executor)
      pushAdjacency.clear()
    }

    graph
  }

  def computeAndPushNeigh2Slot[N <: Neighbor : ClassTag](direction: EdgeDirection, batchSize: Int = -1): Graph[VD, ED] = {
    edges.foreachPartition { iter =>
      val edgePartition = iter.next()

      // 1. create Adjacency in spark RDD partition
      var pos = 0
      val neighType = implicitly[ClassTag[N]].runtimeClass
      val builder = neighType match {
        case nt if nt == classOf[NeighN] =>
          val adjBuilder = new PartitionUnTypedNeighborBuilder[NeighN](direction)
          while (pos < edgePartition.size) {
            adjBuilder.add(edgePartition.srcIdFromPos(pos), edgePartition.dstIdFromPos(pos))
            pos += 1
          }

          adjBuilder
        case nt if nt == classOf[NeighNW] =>
          val adjBuilder = new PartitionUnTypedNeighborBuilder[NeighNW](direction)
          while (pos < edgePartition.size) {
            adjBuilder.add(edgePartition.srcIdFromPos(pos),
              edgePartition.dstIdFromPos(pos),
              edgePartition.attrs(pos).asInstanceOf[WgtTpe])
            pos += 1
          }

          adjBuilder
        case nt if nt == classOf[NeighTN] =>
          val adjBuilder = new PartitionTypedNeighborBuilder[NeighTN](direction)
          while (pos < edgePartition.size) {
            val srcId = edgePartition.srcIdFromPos(pos)
            val dstId = edgePartition.dstIdFromPos(pos)
            val attr = new TypedEdgeAttribute(edgePartition.attrs(pos).asInstanceOf[Long])
            val srcType = attr.srcType
            val dstType = attr.dstType
            adjBuilder.add(srcId, srcType, dstId, dstType)
            pos += 1
          }

          adjBuilder
        case nt if nt == classOf[NeighTNW] =>
          val adjBuilder = new PartitionTypedNeighborBuilder[NeighTNW](direction)
          while (pos < edgePartition.size) {
            val srcId = edgePartition.srcIdFromPos(pos)
            val dstId = edgePartition.dstIdFromPos(pos)
            val attr = new TypedEdgeAttribute(edgePartition.attrs(pos).asInstanceOf[Long])
            val srcType = attr.srcType
            val dstType = attr.dstType
            val weight = attr.weight
            adjBuilder.add(srcId, srcType, dstId, dstType, weight)
            pos += 1
          }

          adjBuilder
        case _ =>
          throw new Exception("Adjacency data error!")
      }

      // 2. create PSF to push Adjacency (from executor)
      val pushAdjacency = psVertices.createUpdate { ctx: PSFGUCtx =>
        val neighs = ctx.getMapParam[N]
        val psPartition = ctx.getPartition[VD]
        val slot = psPartition.getOrCreateSlot[N]("neighbor")

        neighs.foreach {
          case (vid, neigh: NeighN) =>
            if (slot.containsKey(vid)) {
              slot(vid) = slot(vid).asInstanceOf[NeighN]
                .mergeSorted(neigh).asInstanceOf[N]
            } else {
              slot(vid) = neigh.asInstanceOf[N]
            }
          case (vid, neigh: NeighNW) =>
            if (slot.containsKey(vid)) {
              slot(vid) = slot(vid).asInstanceOf[NeighNW]
                .mergeSorted(neigh).asInstanceOf[N]
            } else {
              slot(vid) = neigh.asInstanceOf[N]
            }
          case (vid, neigh: NeighTN) =>
            if (slot.containsKey(vid)) {
              slot(vid) = slot(vid).asInstanceOf[NeighTN]
                .mergeSorted(neigh).asInstanceOf[N]
            } else {
              slot(vid) = neigh.asInstanceOf[N]
            }
          case (vid, neigh: NeighTNW) =>
            if (slot.containsKey(vid)) {
              slot(vid) = slot(vid).asInstanceOf[NeighTNW]
                .mergeSorted(neigh).asInstanceOf[N]
            } else {
              slot(vid) = neigh.asInstanceOf[N]
            }
        }
      }

      // 3. push adjacency (on executor)
      (builder, neighType) match {
        case (b: PartitionUnTypedNeighborBuilder[_], n) if n == classOf[NeighN]=>
          pushAdjacency(b.asInstanceOf[PartitionUnTypedNeighborBuilder[NeighN]].build, batchSize)
        case (b: PartitionUnTypedNeighborBuilder[_], n) if n == classOf[NeighNW]=>
          pushAdjacency(b.asInstanceOf[PartitionUnTypedNeighborBuilder[NeighNW]].build, batchSize)
        case (b: PartitionTypedNeighborBuilder[_], n) if n == classOf[NeighTN]=>
          pushAdjacency(b.asInstanceOf[PartitionTypedNeighborBuilder[NeighTN]].build, batchSize)
        case (b: PartitionTypedNeighborBuilder[_], n) if n == classOf[NeighTNW]=>
          pushAdjacency(b.asInstanceOf[PartitionTypedNeighborBuilder[NeighTNW]].build, batchSize)
      }

      // 4. remove PSF (on executor)
      pushAdjacency.clear()
    }

    graph
  }

  def pushNeighInAttr(batchSize: Int = -1): Graph[VD, ED] = {
    edges.foreachPartition { iter =>
      val edgePartition = iter.next()

      val pushNeigh = psVertices.createUpdate { ctx: PSFGUCtx =>
        val neighs = ctx.getMapParam[VD]
        val psPartition = ctx.getPartition[VD]

        neighs.foreach {
          case (vid, neigh: NeighN) =>
            val attr = psPartition.getAttr(vid)
            val newAttr = if (attr != null) {
              attr.asInstanceOf[NeighN]
                .mergeSorted(neigh).asInstanceOf[VD]
            } else {
              neigh.asInstanceOf[VD]
            }
            psPartition.setAttr(vid, newAttr)
          case (vid, neigh: NeighNW) =>
            val attr = psPartition.getAttr(vid)
            val newAttr = if (attr != null) {
              attr.asInstanceOf[NeighNW]
                .mergeSorted(neigh).asInstanceOf[VD]
            } else {
              neigh.asInstanceOf[VD]
            }
            psPartition.setAttr(vid, newAttr)
          case (vid, neigh: NeighTN) =>
            val attr = psPartition.getAttr(vid)
            val newAttr = if (attr != null) {
              attr.asInstanceOf[NeighTN]
                .mergeSorted(neigh).asInstanceOf[VD]
            } else {
              neigh.asInstanceOf[VD]
            }
            psPartition.setAttr(vid, newAttr)
          case (vid, neigh: NeighTNW) =>
            val attr = psPartition.getAttr(vid)
            val newAttr = if (attr != null) {
              attr.asInstanceOf[NeighTNW]
                .mergeSorted(neigh).asInstanceOf[VD]
            } else {
              neigh.asInstanceOf[VD]
            }
            psPartition.setAttr(vid, newAttr)
        }
      }

      val neighs = new FastHashMap[VertexId, VD](Math.max(64, edgePartition.size / 2))
      edgePartition.vertexAttrs.zipWithIndex.foreach { case (neigh, idx) =>
        if (neigh != null) {
          val vid = edgePartition.local2global(idx)
          neighs(vid) = neigh
        }
      }


      pushNeigh(neighs)
      neighs.clear()
    }

    graph
  }

  def pushNeighInSlot[N <: UnTyped : ClassTag : TypeTag](batchSize: Int = -1): Graph[VD, ED] = {
    edges.foreachPartition { iter =>
      val edgePartition = iter.next()

      val pushNeigh = psVertices.createUpdate { ctx: PSFGUCtx =>
        val neighs = ctx.getMapParam[N]
        val psPartition = ctx.getPartition[VD]

        val slot = psPartition.getOrCreateSlot[N]("neighbor")
        neighs.foreach {
          case (vid, neigh: NeighN) =>
            if (slot.containsKey(vid)) {
              slot(vid) = slot(vid).asInstanceOf[NeighN]
                .mergeSorted(neigh).asInstanceOf[N]
            } else {
              slot(vid) = neigh.asInstanceOf[N]
            }
          case (vid, neigh: NeighNW) =>
            if (slot.containsKey(vid)) {
              slot(vid) = slot(vid).asInstanceOf[NeighNW]
                .mergeSorted(neigh).asInstanceOf[N]
            } else {
              slot(vid) = neigh.asInstanceOf[N]
            }
          case (vid, neigh: NeighTN) =>
            if (slot.containsKey(vid)) {
              slot(vid) = slot(vid).asInstanceOf[NeighTN]
                .mergeSorted(neigh).asInstanceOf[N]
            } else {
              slot(vid) = neigh.asInstanceOf[N]
            }
          case (vid, neigh: NeighTNW) =>
            if (slot.containsKey(vid)) {
              slot(vid) = slot(vid).asInstanceOf[NeighTNW]
                .mergeSorted(neigh).asInstanceOf[N]
            } else {
              slot(vid) = neigh.asInstanceOf[N]
            }
        }
      }

      pushNeigh(edgePartition.getSlot[N]("neighbor").asFastHashMap)
      pushNeigh.clear()
    }

    graph
  }

  def pullNeigh2Attr(batchSize: Int = -1): Graph[VD, ED] = {
    edges.foreachPartition { iter =>
      val edgePartition = iter.next()
      edgePartition.updateVertexAttrs(batchSize)
    }

    graph
  }

  def pullNeigh2Slot[N <: Neighbor : ClassTag : TypeTag](batchSize: Int = -1): Graph[VD, ED] = {
    edges.foreachPartition { iter =>
      val edgePartition = iter.next()
      edgePartition.updateSlot[N]("neighbor", batchSize)
    }

    graph
  }
}
