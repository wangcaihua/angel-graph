package com.tencent.angel.graph.framework

import com.tencent.angel.graph.framework.EdgeActiveness.EdgeActiveness
import com.tencent.angel.graph.framework.EdgeDirection.EdgeDirection
import com.tencent.angel.graph.utils.{BitSet, FastHashMap}
import com.tencent.angel.graph.{VertexId, VertexSet}

import scala.reflect._
import scala.{specialized => spec}

class EdgePartition[VD: ClassTag,
@spec(Int, Long, Float, Double) ED: ClassTag](localSrcIds: Array[Int],
                                              localDstIds: Array[Int],
                                              data: Array[ED],
                                              index: FastHashMap[VertexId, (Int, Int)],
                                              global2local: FastHashMap[VertexId, Int],
                                              local2global: Array[VertexId],
                                              vertexAttrs: Array[VD],
                                              edgeDirection: EdgeDirection,
                                              activeSet: Option[VertexSet]) extends Serializable {

  val size: Int = localSrcIds.length

  def indexSize: Int = index.size

  @inline private def srcIdFromPos(pos: Int): VertexId = local2global(localSrcIds(pos))

  @inline private def dstIdFromPos(pos: Int): VertexId = local2global(localDstIds(pos))

  @inline private def vertexAttrFromId(VerId: VertexId): VD = vertexAttrs(global2local(VerId))

  @inline private def edgeFromPos(pos: Int): Edge[ED] = {
    Edge(srcIdFromPos(pos), dstIdFromPos(pos), data(pos))
  }

  @inline private def tripletFromPos(pos: Int): EdgeTriplet[VD, ED] = {
    val srcId = srcIdFromPos(pos)
    val dstId = dstIdFromPos(pos)
    EdgeTriplet(srcId, dstId, vertexAttrFromId(srcId), vertexAttrFromId(dstId), data(pos))
  }

  @inline private def edgeWithLocalIdsFromPos(pos: Int): EdgeWithLocalIds[ED] = {
    val localSrcId = localSrcIds(pos)
    val srcId = local2global(localSrcId)

    val localDstId = localDstIds(pos)
    val dstId = local2global(localDstId)

    EdgeWithLocalIds(srcId, dstId, localSrcId, localDstId, data(pos))
  }

  def attrs(pos: Int): ED = data(pos)

  def isActive(vid: VertexId): Boolean = activeSet match {
    case Some(as) => as.contains(vid)
    case None => true
  }

  def numActives: Option[Int] = activeSet match {
    case Some(as) => Some(as.size)
    case None => None
  }

  def withData[ED2: ClassTag](newData: Array[ED2]): EdgePartition[VD, ED2] = {
    new EdgePartition[VD, ED2](localSrcIds, localDstIds, newData, index,
      global2local, local2global, vertexAttrs, edgeDirection, activeSet)
  }

  def withActiveSet(iter: Iterator[VertexId]): EdgePartition[VD, ED] = {
    val activeSet = new VertexSet
    while (iter.hasNext) {
      activeSet.add(iter.next())
    }
    new EdgePartition[VD, ED](localSrcIds, localDstIds, data, index,
      global2local, local2global, vertexAttrs, edgeDirection, Some(activeSet))
  }

  def updateVertices(iter: Iterator[(VertexId, VD)]): EdgePartition[VD, ED] = {
    val newVertexAttrs = new Array[VD](vertexAttrs.length)
    System.arraycopy(vertexAttrs, 0, newVertexAttrs, 0, vertexAttrs.length)
    while (iter.hasNext) {
      val kv = iter.next()
      newVertexAttrs(global2local(kv._1)) = kv._2
    }
    new EdgePartition[VD, ED](localSrcIds, localDstIds, data, index,
      global2local, local2global, newVertexAttrs, edgeDirection, activeSet)
  }

  def withoutVertexAttributes[VD2: ClassTag](): EdgePartition[VD2, ED] = {
    val newVertexAttrs = new Array[VD2](vertexAttrs.length)
    new EdgePartition[VD2, ED](localSrcIds, localDstIds, data, index,
      global2local, local2global, newVertexAttrs, edgeDirection, activeSet)
  }

  def map[ED2: ClassTag](f: Edge[ED] => ED2): EdgePartition[VD, ED2] = {
    val newData = new Array[ED2](data.length)
    val size = data.length
    var pos = 0
    while (pos < size) {
      newData(pos) = f(edgeFromPos(pos))
      pos += 1
    }
    this.withData(newData)
  }

  def map[ED2: ClassTag](iter: Iterator[ED2]): EdgePartition[VD, ED2] = {
    // Faster than iter.toArray, because the expected size is known.
    val newData = new Array[ED2](data.length)
    var pos = 0
    while (iter.hasNext) {
      newData(pos) = iter.next()
      pos += 1
    }
    assert(newData.length == pos)
    this.withData(newData)
  }

  def reverse: EdgePartition[VD, ED] = {
    val builder = new ExistingEdgePartitionBuilder[VD, ED](
      global2local, local2global, vertexAttrs, activeSet, size)
    var pos = 0
    while (pos < size) {
      builder.add(edgeWithLocalIdsFromPos(pos))
      pos += 1
    }
    builder.build
  }

  def filter(epred: EdgeTriplet[VD, ED] => Boolean,
             vpred: (VertexId, VD) => Boolean): EdgePartition[VD, ED] = {
    val builder = new EdgePartitionBuilder[VD, ED]()
    var pos = 0
    while (pos < size) {
      // The user sees the EdgeTriplet, so we can't reuse it and must create one per edge.
      val et = tripletFromPos(pos)
      if (vpred(et.srcId, et.srcAttr) && vpred(et.dstId, et.dstAttr) && epred(et)) {
        builder.add(et.edge)
      }
      pos += 1
    }
    builder.build
  }

  def foreach(f: Edge[ED] => Unit) {
    iterator.foreach(f)
  }

  // for deduplicate
  def groupEdges(merge: (ED, ED) => ED): EdgePartition[VD, ED] = {
    val builder = new ExistingEdgePartitionBuilder[VD, ED](
      global2local, local2global, vertexAttrs, activeSet)
    var currSrcId: VertexId = null.asInstanceOf[VertexId]
    var currDstId: VertexId = null.asInstanceOf[VertexId]
    var currAttr: ED = null.asInstanceOf[ED]

    var currLocalSrcId = -1
    var currLocalDstId = -1
    // Iterate through the edges, accumulating runs of identical edges using the curr* variables and
    // releasing them to the builder when we see the beginning of the next run
    var i = 0
    while (i < size) {
      if (i > 0 && currSrcId == srcIdFromPos(i) && currDstId == srcIdFromPos(i)) {
        // This edge should be accumulated into the existing run
        currAttr = merge(currAttr, data(i))
      } else {
        // This edge starts a new run of edges
        if (i > 0) {
          // First release the existing run to the builder
          builder.add(currSrcId, currDstId, currLocalSrcId, currLocalDstId, currAttr)
        }
        // Then start accumulating for a new run
        currSrcId = srcIdFromPos(i)
        currDstId = srcIdFromPos(i)
        currLocalSrcId = localSrcIds(i)
        currLocalDstId = localDstIds(i)
        currAttr = data(i)
      }
      i += 1
    }
    // Finally, release the last accumulated run
    if (size > 0) {
      builder.add(currSrcId, currDstId, currLocalSrcId, currLocalDstId, currAttr)
    }
    builder.build
  }

  def innerJoin[ED2: ClassTag, ED3: ClassTag](other: EdgePartition[_, ED2])
                                             (f: (VertexId, VertexId, ED, ED2) => ED3): EdgePartition[VD, ED3] = {
    val builder = new ExistingEdgePartitionBuilder[VD, ED3](
      global2local, local2global, vertexAttrs, activeSet)
    var i = 0
    var j = 0
    // For i = index of each edge in `this`...
    while (i < size && j < other.size) {
      val srcId = this.srcIdFromPos(i)
      val dstId = this.dstIdFromPos(i)
      // ... forward j to the index of the corresponding edge in `other`, and...

      while (j < other.size && other.srcIdFromPos(j) < srcId) {
        j += 1
      }
      if (j < other.size && other.srcIdFromPos(j) == srcId) {
        while (j < other.size && other.srcIdFromPos(j) == srcId && other.srcIdFromPos(j) < dstId) {
          j += 1
        }
        if (j < other.size && other.srcIdFromPos(j) == srcId && other.dstIdFromPos(j) == dstId) {
          // ... run `f` on the matching edge
          builder.add(srcId, dstId, localSrcIds(i), localDstIds(i),
            f(srcId, dstId, this.data(i), other.attrs(j)))
        }
      }
      i += 1
    }
    builder.build
  }

  def iterator: Iterator[Edge[ED]] = new Iterator[Edge[ED]] {
    private[this] var pos = 0

    override def hasNext: Boolean = pos < EdgePartition.this.size

    override def next(): Edge[ED] = {
      pos += 1
      edgeFromPos(pos)
    }
  }

  def tripletIterator(includeSrc: Boolean = true, includeDst: Boolean = true): Iterator[EdgeTriplet[VD, ED]] = {
    new Iterator[EdgeTriplet[VD, ED]] {
      private[this] var pos = 0

      override def hasNext: Boolean = pos < EdgePartition.this.size

      override def next(): EdgeTriplet[VD, ED] = {
        val srcId = srcIdFromPos(pos)
        val dstId = dstIdFromPos(pos)
        val et = if (includeSrc && includeDst) {
          EdgeTriplet(srcId, dstId, vertexAttrFromId(srcId), vertexAttrFromId(dstId), data(pos))
        } else if (includeSrc && !includeDst) {
          EdgeTriplet(srcId, dstId, vertexAttrFromId(srcId), null.asInstanceOf[VD], data(pos))
        } else if (!includeSrc && includeDst) {
          EdgeTriplet(srcId, dstId, null.asInstanceOf[VD], vertexAttrFromId(dstId), data(pos))
        } else {
          EdgeTriplet(srcId, dstId, null.asInstanceOf[VD], null.asInstanceOf[VD], data(pos))
        }

        pos += 1

        et
      }
    }
  }

  def aggregateMessagesScan[A: ClassTag](sendMsg: EdgeContext[VD, ED, A] => Unit,
                                         mergeMsg: (A, A) => A,
                                         tripletFields: TripletFields,
                                         activeness: EdgeActiveness): Iterator[(VertexId, A)] = {
    /*
    val aggregates = new Array[A](vertexAttrs.length)
    val bitset = new BitSet(vertexAttrs.length)

    val ctx = new AggregatingEdgeContext[VD, ED, A](mergeMsg, aggregates, bitset)
    index.iterator.foreach { cluster =>
      val clusterSrcId = cluster._1
      val clusterPos = cluster._2
      val clusterLocalSrcId = localSrcIds(clusterPos)

      val scanCluster =
        if (activeness == EdgeActiveness.Neither) true
        else if (activeness == EdgeActiveness.SrcOnly) isActive(clusterSrcId)
        else if (activeness == EdgeActiveness.DstOnly) true
        else if (activeness == EdgeActiveness.Both) isActive(clusterSrcId)
        else if (activeness == EdgeActiveness.Either) true
        else throw new Exception("unreachable")

      if (scanCluster) {
        var pos = clusterPos
        val srcAttr =
          if (tripletFields.useSrc) vertexAttrs(clusterLocalSrcId) else null.asInstanceOf[VD]
        ctx.setSrcOnly(clusterSrcId, clusterLocalSrcId, srcAttr)
        while (pos < size && localSrcIds(pos) == clusterLocalSrcId) {
          val localDstId = localDstIds(pos)
          val dstId = local2global(localDstId)
          val edgeIsActive =
            if (activeness == EdgeActiveness.Neither) true
            else if (activeness == EdgeActiveness.SrcOnly) true
            else if (activeness == EdgeActiveness.DstOnly) isActive(dstId)
            else if (activeness == EdgeActiveness.Both) isActive(dstId)
            else if (activeness == EdgeActiveness.Either) isActive(clusterSrcId) || isActive(dstId)
            else throw new Exception("unreachable")
          if (edgeIsActive) {
            val dstAttr =
              if (tripletFields.useDst) vertexAttrs(localDstId) else null.asInstanceOf[VD]
            ctx.setRest(dstId, localDstId, dstAttr, data(pos))
            sendMsg(ctx)
          }
          pos += 1
        }
      }
    }

    bitset.iterator.map { localId => (local2global(localId), aggregates(localId)) }

     */

    null
  }
}

private class AggregatingEdgeContext[VD, ED, A](mergeMsg: (A, A) => A, aggregates: Array[A], bitset: BitSet)
  extends EdgeContext[VD, ED, A] {
  private[this] var _localSrcId: Int = _
  private[this] var _localDstId: Int = _

  def set(srcId: VertexId, dstId: VertexId, localSrcId: Int, localDstId: Int,
          srcAttr: VD, dstAttr: VD, attr: ED) {
    _srcId = srcId
    _dstId = dstId
    _localSrcId = localSrcId
    _localDstId = localDstId
    _srcAttr = srcAttr
    _dstAttr = dstAttr
    _attr = attr
  }

  def setSrcOnly(srcId: VertexId, localSrcId: Int, srcAttr: VD) {
    _srcId = srcId
    _localSrcId = localSrcId
    _srcAttr = srcAttr
  }

  def setRest(dstId: VertexId, localDstId: Int, dstAttr: VD, attr: ED) {
    _dstId = dstId
    _localDstId = localDstId
    _dstAttr = dstAttr
    _attr = attr
  }

  override def sendToSrc(msg: A) {
    send(_localSrcId, msg)
  }

  override def sendToDst(msg: A) {
    send(_localDstId, msg)
  }

  @inline private def send(localId: Int, msg: A) {
    if (bitset.get(localId)) {
      aggregates(localId) = mergeMsg(aggregates(localId), msg)
    } else {
      aggregates(localId) = msg
      bitset.set(localId)
    }
  }
}
