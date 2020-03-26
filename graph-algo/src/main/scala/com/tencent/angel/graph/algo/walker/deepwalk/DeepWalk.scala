package com.tencent.angel.graph.algo.walker.deepwalk

import com.tencent.angel.graph.VertexId
import com.tencent.angel.graph.algo.walker.{PathQueue, WalkerBase}
import com.tencent.angel.graph.core.data.UnTyped
import com.tencent.angel.graph.core.psf.common.{PSFGUCtx, PSFMCtx, Singular}
import com.tencent.angel.graph.core.psf.get.GetPSF
import com.tencent.angel.graph.core.psf.update.UpdatePSF
import com.tencent.angel.graph.framework.Graph
import com.tencent.angel.graph.utils.psfConverters._
import com.tencent.angel.graph.utils.{FastArray, FastHashMap, FastHashSet}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag


class DeepWalk[N <: UnTyped : ClassTag : TypeTag, ED: ClassTag](graph: Graph[N, ED])
  extends WalkerBase[N, ED](graph) {

  private val initWalkPathPSF: UpdatePSF = psVertices.createUpdate { ctx: PSFGUCtx =>
    val walkLength = ctx.getParam[Int]
    val psPartition = ctx.getPartition[N]

    val slot = psPartition.getOrCreateSlot[FastArray[VertexId]](slotName)
    val queue = new PathQueue(walkLength)
    val buf = new FastArray[FastArray[VertexId]](100)
    psPartition.local2global.foreach { vid =>
      val array = if (slot.containsKey(vid)) {
        slot(vid).clear
      } else {
        new FastArray[VertexId](walkLength)
      }

      array += vid
      val neigh = psPartition.getAttr(vid)
      array += neigh.sample()

      if (buf.size < buf.capacity) {
        buf += array
      } else {
        queue.pushBatch(buf.array)
        buf.clear
        buf += array
      }

      slot.put(vid, array)
    }

    queue.pushBatch(buf.trim().array)

    psPartition.putCache(ctx.key, queue)
  }

  override def samplePath(): Graph[N, ED] = {
    logInfo(s"begin to sample path")

    // create or init walk path on ps
    initWalkPathPSF(pathLength)

    // sampling
    edgesRDD.foreachPartition { iter =>
      val edgePartition = iter.next()
      edgePartition.setPSMatrix(psVertices)

      // 1. define PSFs
      val pullNeigh: GetPSF[FastHashMap[VertexId, N]] = psVertices.createGet { ctx: PSFGUCtx =>
        val params = ctx.getArrayParam
        val psPartition = ctx.getPartition[N]

        val backRes = new FastHashMap[VertexId, N](params.length)
        params.foreach { vid =>
          backRes.put(vid, psPartition.getAttr(vid))
        }

        backRes
      } { ctx: PSFMCtx =>
        val last = ctx.getLast[FastHashMap[VertexId, N]]
        val curr = ctx.getCurr[FastHashMap[VertexId, N]]

        last.merge(curr)
      }

      val pullTail: GetPSF[FastHashMap[VertexId, VertexId]] = psVertices.createGet { ctx: PSFGUCtx =>
        val partBS = ctx.getParam[Int]
        val psPartition = ctx.getPartition[N]

        val queue = psPartition.getCache[PathQueue](ctx.key)
        val backRes = new FastHashMap[VertexId, VertexId](partBS)
        queue.popBath(partBS).foreach{array =>
          backRes.put(array.head, array.last)
        }

        backRes
      } { ctx: PSFMCtx =>
        val last = ctx.getLast[FastHashMap[VertexId, VertexId]]
        val curr = ctx.getCurr[FastHashMap[VertexId, VertexId]]

        last.merge(curr)
      }

      val pushTail: UpdatePSF = psVertices.createUpdate { ctx: PSFGUCtx =>
        val params = ctx.getMapParam[VertexId]
        val psPartition = ctx.getPartition[N]
        val slot = psPartition.getSlot[FastArray[VertexId]](slotName)
        val queue = psPartition.getCache[PathQueue](ctx.key)

        val buf = new FastArray[FastArray[VertexId]](100)
        params.foreach { case (vid, tail) =>
          val path = slot(vid)
          path += tail

          if (path.size < queue.pathLength) {
            if (buf.size < buf.capacity) {
              buf += path
            } else {
              queue.pushBatch(buf.array)
              buf.clear
              buf += path
            }
          }
        }

        queue.pushBatch(buf.trim().array)
      }

      val isFinish: GetPSF[Boolean] = psVertices.createGet{ ctx: PSFGUCtx =>
        val psPartition = ctx.getPartition[N]
        val slot = psPartition.getSlot[FastArray[VertexId]](slotName)

        slot.iterator.forall{ case (_, fastArray) =>
          fastArray.size == fastArray.capacity
        }
      } {ctx: PSFMCtx =>
        val last = ctx.getLast[Boolean]
        val curr = ctx.getCurr[Boolean]

        last && curr
      }

      // 2. sample tail in executor
      var outFlag = false
      // pull a batch tails from PS: FastHashMap[VertexId, VertexId]
      var oldTails: FastHashMap[VertexId, VertexId] = pullTail(partBatchSize)
      while (!outFlag && oldTails.nonEmpty) {
        // 2.1 the first round, compute neighs need pull
        val needPullNeigh = new FastHashSet[VertexId](oldTails.size())
        val newTails = new FastHashMap[VertexId, VertexId](oldTails.size())
        oldTails.foreach { case (vid, dst) =>
          if (!edgePartition.contains(dst)) {
            needPullNeigh.add(dst)
          } else {
            val dstNeigh = edgePartition.vertexAttrFromId(dst)

            // sample neighbor
            newTails(vid) = dstNeigh.sample()
          }
        }

        // 2.2 pull neighs required in this batch
        val pulledNeighs: FastHashMap[VertexId, N] = pullNeigh(needPullNeigh.toArray)
        assert(pulledNeighs.size() == needPullNeigh.size())

        // 2.3 sample neighbor using reject sampling
        oldTails.foreach { case (vid, dst) =>
          if (!newTails.containsKey(vid)) {
            val dstNeigh = if (edgePartition.contains(dst)) {
              edgePartition.vertexAttrFromId(dst)
            } else {
              pulledNeighs(dst)
            }

            // sample neighbor
            newTails(vid) = dstNeigh.sample()
          }
        }

        // 2.4 sample neighbor using reject sampling
        pushTail(newTails)

        // 2.5 pull a batch tails from PS: FastHashMap[VertexId, Array[VertexId]]
        oldTails = pullTail(partBatchSize)

        // 2.6 check finish
        if (oldTails.isEmpty) {
          outFlag = isFinish()
        }
      }

      pullNeigh.clear()
      pullTail.clear()
      pushTail.clear()
      isFinish.clear()
    }

    logInfo(s"finished to sample path ")

    graph
  }
}
