package com.tencent.angel.graph.framework

import java.util.concurrent.Future

import com.tencent.angel.graph.core.psf.common.{PSFGUCtx, PSFMCtx}
import com.tencent.angel.graph.core.psf.get.GetPSF
import com.tencent.angel.graph.core.psf.update.UpdatePSF
import com.tencent.angel.graph.framework.EdgeActiveness.EdgeActiveness
import com.tencent.angel.graph.utils.psfConverters._
import com.tencent.angel.graph.utils.{BitSet, FastArray, FastHashMap, Logging, RefHashMap}
import com.tencent.angel.graph.{VertexId, VertexSet}
import com.tencent.angel.ml.matrix.psf.update.base.VoidResult
import com.tencent.angel.spark.models.PSMatrix

import scala.reflect._
import scala.reflect.runtime.universe.TypeTag
import scala.{specialized => spec}

class EdgePartition[VD: ClassTag : TypeTag,
@spec(Int, Long, Float, Double) ED: ClassTag](val localSrcIds: Array[Int],
                                              val localDstIds: Array[Int],
                                              val data: Array[ED],
                                              val index: FastHashMap[VertexId, (Int, Int)],
                                              val global2local: FastHashMap[VertexId, Int],
                                              val local2global: Array[VertexId],
                                              val vertexAttrs: Array[VD],
                                              val localDegreeHist: Array[Int],
                                              val activeSet: Option[VertexSet]) extends Serializable with Logging {

  val size: Int = localSrcIds.length

  private val maxTruck: Int = 10000

  lazy val maxVertexId: VertexId = local2global.max

  lazy val minVertexId: VertexId = local2global.min

  private var psMatrix: PSMatrix = _

  private lazy val updateActiveSet: GetPSF[Array[VertexId]] = psMatrix.createGet { ctx: PSFGUCtx =>
    val param = ctx.getArrayParam
    val partition = ctx.getPartition[VD]

    param.filter(vid => partition.isActive(global2local(vid)))
  } { ctx: PSFMCtx =>
    val last = ctx.getLast[Array[VertexId]]
    val curr = ctx.getCurr[Array[VertexId]]

    if (last != null && last.nonEmpty) {
      if (curr != null && curr.nonEmpty) {
        val newArray = new Array[VertexId](last.length + curr.length)
        Array.copy(last, 0, newArray, 0, last.length)
        Array.copy(curr, 0, newArray, last.length, curr.length)
        newArray
      } else {
        last
      }
    } else {
      curr
    }
  }

  private lazy val pullNodeAttr: GetPSF[FastHashMap[VertexId, VD]] = psMatrix.createGet { ctx: PSFGUCtx =>
    val param = ctx.getArrayParam
    val partition = ctx.getPartition[VD]
    val map = new FastHashMap[VertexId, VD](param.length)
    param.foreach { vid => map(vid) = partition.getAttr(vid) }
    map
  } { ctx: PSFMCtx =>
    val last = ctx.getLast[FastHashMap[VertexId, VD]]
    val curr = ctx.getCurr[FastHashMap[VertexId, VD]]

    curr.foreach { case (vid, attr) => last(vid) = attr }
    last
  }

  def updateVertexAttrs(batchSize: Int = -1): this.type = {
    val vAttrs = activeSet match {
      case Some(as) =>
        if (batchSize > 0) {
          pullNodeAttr(as.toArray, batchSize)
        } else {
          pullNodeAttr(as.toArray, maxTruck)
        }
      case None =>
        if (batchSize > 0) {
          pullNodeAttr(local2global, batchSize)
        } else {
          pullNodeAttr(local2global, maxTruck)
        }
    }

    vAttrs.foreach { case (k, v) => vertexAttrs(global2local(k)) = v }

    this
  }

  def updateActiveSet(batchSize: Int = -1): this.type = {
    activeSet match {
      case Some(as) =>
        val active = if (batchSize > 0) {
          updateActiveSet(as.toArray, batchSize)
        } else {
          updateActiveSet(as.toArray, maxTruck)
        }

        as.clear()
        active.foreach(v => as.add(v))
      case None =>
    }

    this
  }

  def setPSMatrix(psMat: PSMatrix): this.type = {
    psMatrix = psMat
    this
  }

  def localVertices: Array[VertexId] = local2global

  def indexSize: Int = index.size()

  @inline def srcIdFromPos(pos: Int): VertexId = local2global(localSrcIds(pos))

  @inline def dstIdFromPos(pos: Int): VertexId = local2global(localDstIds(pos))

  @inline def vertexAttrFromId(VerId: VertexId): VD = vertexAttrs(global2local(VerId))

  @inline def edgeFromPos(pos: Int): Edge[ED] = {
    Edge(srcIdFromPos(pos), dstIdFromPos(pos), data(pos))
  }

  @inline def tripletFromPos(pos: Int): EdgeTriplet[VD, ED] = {
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

  def updateEdgeAttrsByPos(pos: Int, attr: ED) : this.type = {
    data(pos) = attr
    this
  }

  def isActive(vid: VertexId): Boolean = activeSet match {
    case Some(as) => as.contains(vid)
    case None => true
  }

  def numActives: Option[Int] = activeSet match {
    case Some(as) => Some(as.size())
    case None => None
  }

  def withData[ED2: ClassTag](newData: Array[ED2]): EdgePartition[VD, ED2] = {
    new EdgePartition[VD, ED2](localSrcIds, localDstIds, newData, index,
      global2local, local2global, vertexAttrs, localDegreeHist, activeSet)
  }

  def withActiveSet(iter: Iterator[VertexId]): EdgePartition[VD, ED] = {
    val activeSet = new VertexSet
    while (iter.hasNext) {
      activeSet.add(iter.next())
    }
    new EdgePartition[VD, ED](localSrcIds, localDstIds, data, index,
      global2local, local2global, vertexAttrs, localDegreeHist, Some(activeSet))
  }

  def updateVertices(iter: Iterator[(VertexId, VD)]): EdgePartition[VD, ED] = {
    val newVertexAttrs = new Array[VD](vertexAttrs.length)
    System.arraycopy(vertexAttrs, 0, newVertexAttrs, 0, vertexAttrs.length)
    while (iter.hasNext) {
      val kv = iter.next()
      newVertexAttrs(global2local(kv._1)) = kv._2
    }
    new EdgePartition[VD, ED](localSrcIds, localDstIds, data, index,
      global2local, local2global, newVertexAttrs, localDegreeHist, activeSet)
  }

  def withoutVertexAttributes[VD2: ClassTag : TypeTag](): EdgePartition[VD2, ED] = {
    val newVertexAttrs = new Array[VD2](vertexAttrs.length)
    new EdgePartition[VD2, ED](localSrcIds, localDstIds, data, index,
      global2local, local2global, newVertexAttrs, localDegreeHist, activeSet)
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
      global2local, local2global, vertexAttrs, localDegreeHist, activeSet, size)
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
      global2local, local2global, vertexAttrs, localDegreeHist, activeSet)
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
      global2local, local2global, vertexAttrs, localDegreeHist, activeSet)
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
      edgeFromPos(pos - 1)
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

  private[graph] def aggregateMessagesScan[M: ClassTag : TypeTag](sendMsg: EdgeContext[VD, ED, M] => Unit,
                                                                  mergeMsg: (M, M) => M,
                                                                  tripletFields: TripletFields,
                                                                  activeness: EdgeActiveness,
                                                                  batchSize: Int = -1): Unit = {
    assert(psMatrix != null)

    // 1. create PSFs
    val updateRemoteVertexMsg: UpdatePSF = psMatrix.createUpdate { ctx: PSFGUCtx =>
      val param = ctx.getMapParam[M]
      val partition = ctx.getPartition[VD]

      param.foreach { case (vid, msg) => partition.mergeMessage[M](vid, msg, mergeMsg) }
    }

    // 2. prepare to create AggregatingEdgeContext
    val aggregates = new Array[M](vertexAttrs.length)
    val bitSet = new BitSet(vertexAttrs.length)
    val ctx = new AggregatingEdgeContext[VD, ED, M](mergeMsg, aggregates, bitSet)

    // 3. if batchSize > 0; use async push message
    var (batchMask, localMask, indexCount, futures) = if (batchSize > 0) {
      (new BitSet(vertexAttrs.length), new BitSet(vertexAttrs.length),
        0, new FastArray[Future[VoidResult]]())
    } else {
      (null.asInstanceOf[BitSet], null.asInstanceOf[BitSet],
        0, null.asInstanceOf[FastArray[Future[VoidResult]]])
    }

    // 4. compute and generate message
    index.iterator.foreach { case (clusterId: VertexId, (clusterPos: Int, clusterLen: Int)) =>
      if (isActive(clusterId)) {
        // foreach active clusterId
        (clusterPos until clusterPos + clusterLen).foreach { pos =>
          val localSrcId = localSrcIds(pos)
          val srcId = local2global(localSrcId)
          val localDstId = localDstIds(pos)
          val dstId = local2global(localDstId)

          val edgeIsActive =
            if (activeness == EdgeActiveness.Neither) true
            else if (activeness == EdgeActiveness.SrcOnly) clusterId == srcId || isActive(srcId)
            else if (activeness == EdgeActiveness.DstOnly) clusterId == dstId || isActive(dstId)
            else if (activeness == EdgeActiveness.Both) isActive(srcId) && isActive(dstId)
            else if (activeness == EdgeActiveness.Either) isActive(srcId) || isActive(dstId)
            else throw new Exception("unreachable")

          if (edgeIsActive) {
            val srcAttr =
              if (tripletFields.useSrc) vertexAttrs(localSrcId) else null.asInstanceOf[VD]
            val dstAttr =
              if (tripletFields.useDst) vertexAttrs(localDstId) else null.asInstanceOf[VD]
            ctx.set(dstId, dstId, localSrcId, localDstId, srcAttr, dstAttr, data(pos))
            sendMsg(ctx)
          }
        }

        if (batchSize > 0) {
          indexCount += 1 // cluster count
          batchMask.set(global2local(clusterId)) // mark the clusterId that finished in this batch
          localMask.set(global2local(clusterId)) // mark the clusterId that finished in history

          if (indexCount % batchSize == 0) {
            val batchMsg = new FastHashMap[VertexId, M](batchSize)
            batchMask.iterator.foreach {
              case id if aggregates(id) != null =>
                batchMsg(local2global(id)) = aggregates(id)
              case _ =>
            }

            futures += updateRemoteVertexMsg.async(batchMsg)
            batchMask.clear()
          }
        }
      }
    }

    if (batchSize > 0) {
      indexCount = 0
      bitSet.iterator.foreach { localId =>
        if (!localMask.get(localId)) batchMask.set(localId)
      }
      var batchMsg = new FastHashMap[VertexId, M](batchSize)
      batchMask.iterator.foreach {
        case id if aggregates(id) != null =>
          batchMsg(local2global(id)) = aggregates(id)
          indexCount += 1
          if (indexCount % batchSize == 0) {
            futures += updateRemoteVertexMsg.async(batchMsg)
            batchMsg = new FastHashMap[VertexId, M](batchSize)
          }
        case _ =>
      }

      if (batchMsg.size() > 0) {
        futures += updateRemoteVertexMsg.async(batchMsg)
      }

      futures.foreach(_.get)
    } else {
      val batchMsg = new FastHashMap[VertexId, M](bitSet.capacity)
      bitSet.iterator.foreach { id => batchMsg(local2global(id)) = aggregates(id) }
      updateRemoteVertexMsg(batchMsg, maxTruck)
    }

    updateRemoteVertexMsg.clear()
  }
}

private class AggregatingEdgeContext[VD, ED, M](mergeMsg: (M, M) => M,
                                                aggregates: Array[M],
                                                bitSet: BitSet
                                               ) extends EdgeContext[VD, ED, M] with Serializable {
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

  override def sendToSrc(msg: M) {
    send(_localSrcId, msg)
  }

  override def sendToDst(msg: M) {
    send(_localDstId, msg)
  }

  @inline private def send(localId: Int, msg: M) {
    if (bitSet.get(localId)) {
      aggregates(localId) = mergeMsg(aggregates(localId), msg)
    } else {
      aggregates(localId) = msg
      bitSet.set(localId)
    }
  }
}
