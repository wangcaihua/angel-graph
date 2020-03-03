package com.tencent.angel.graph.framework

import com.tencent.angel.graph.framework.EdgeDirection.EdgeDirection
import com.tencent.angel.graph.utils.{FastArray, FastHashMap, Logging, SortDataFormat, Sorter}
import com.tencent.angel.graph.{VertexId, VertexSet}

import scala.reflect._
import scala.{specialized => spec}
import scala.reflect.runtime.universe.TypeTag

class EdgePartitionBuilder[VD: ClassTag: TypeTag, @spec(Int, Long, Float, Double) ED: ClassTag]
(size: Int = 64, edgeDirection: EdgeDirection = EdgeDirection.Out) extends Logging {
  private val edges = new FastArray[Edge[ED]](size)

  def add(srcId: VertexId, dstId: VertexId, attr: ED): this.type = {
    if (srcId != dstId) {
      edges += Edge(srcId, dstId, attr)
    }

    this
  }

  def add(srcId: VertexId, dstId: VertexId): this.type = {
    if (srcId != dstId) {
      edges += Edge(srcId, dstId, 1.asInstanceOf[ED])
    }

    this
  }

  def add(edge: Edge[ED]): this.type = {
    if (edge.dstId == edge.dstId) {
      edges += edge
    }

    this
  }

  def build: EdgePartition[VD, ED] = {
    val edgeArray = edges.trim().array

    val localSrcIds = new Array[Int](edgeArray.length)
    val localDstIds = new Array[Int](edgeArray.length)
    val data = new Array[ED](edgeArray.length)
    val index = new FastHashMap[VertexId, (Int, Int)]
    val global2local = new FastHashMap[VertexId, Int]
    val local2global = new FastArray[VertexId]
    var localDegreeHist: Array[Int] = Array.empty[Int]
    var vertexAttrs = Array.empty[VD]
    var vertexHist: FastHashMap[VertexId, Int] = null

    def mergeFunc(v1: (Int, Int), v2: (Int, Int)): (Int, Int) = {
      val x1 = if (v1._1 < v2._1) v1._1 else v1._1
      val x2 = v1._2 + v2._2

      x1 -> x2
    }

    def orderKey(edge: Edge[ED]): VertexId = {
      if (vertexHist(edge.srcId) < vertexHist(edge.dstId)) {
        edge.srcId
      } else if (vertexHist(edge.srcId) > vertexHist(edge.dstId)) {
        edge.dstId
      } else {
        if (edge.srcId < edge.dstId) {
          edge.srcId
        } else {
          edge.dstId
        }
      }
    }

    if (edgeDirection == EdgeDirection.Out) {
      new Sorter(Edge.sortDataFormat[ED])
        .sort(edgeArray, 0, edgeArray.length, Edge.srcOrdering[ED])

      edgeArray.zipWithIndex.foreach { case (edge, idx) =>
        index.putMerge(edge.srcId, (idx, 1), mergeFunc)
      }
    } else if (edgeDirection == EdgeDirection.In) {
      new Sorter(Edge.sortDataFormat[ED])
        .sort(edgeArray, 0, edgeArray.length, Edge.dstOrdering[ED])

      edgeArray.zipWithIndex.foreach { case (edge, idx) =>
        index.putMerge(edge.dstId, (idx, 1), mergeFunc)
      }
    } else if (edgeDirection == EdgeDirection.Both) {
      vertexHist = Edge.vertexHist(edgeArray)
      val pairOrdering = Edge.pairOrdering[ED](vertexHist)
      new Sorter(Edge.sortDataFormat[ED])
        .sort(edgeArray, 0, edgeArray.length, pairOrdering)

      edgeArray.zipWithIndex.foreach { case (edge, idx) =>
        val currKey = orderKey(edge)
        index.putMerge(currKey, (idx, 1), mergeFunc)
      }
      /*
      index.foreach{ case (id, (start, len)) =>
        (0 until len).foreach{ idx =>
          assert(id == orderKey(edgeArray(start + idx)))
        }
      }
       */
    } else {
      new Sorter(Edge.sortDataFormat[ED])
        .sort(edgeArray, 0, edgeArray.length, Edge.srcOrdering[ED])

      edgeArray.zipWithIndex.foreach { case (edge, idx) =>
        index.putMerge(edge.srcId, (idx, 1), mergeFunc)
      }
    }

    if (edgeArray.length > 0) {
      var currSrcId: VertexId = edgeArray(0).srcId
      var currLocalId = -1
      var pos = 0
      while (pos < edgeArray.length) {
        val edge = edgeArray(pos)
        val srcId = edge.srcId
        val dstId = edge.dstId

        localSrcIds(pos) = global2local.changeValue(srcId,
          {
            currLocalId += 1
            local2global += srcId
            currLocalId
          }, identity)

        localDstIds(pos) = global2local.changeValue(dstId,
          {
            currLocalId += 1
            local2global += dstId
            currLocalId
          }, identity)

        data(pos) = edge.attr

        if (srcId != currSrcId) {
          currSrcId = srcId
        }

        pos += 1
      }

      vertexAttrs = new Array[VD](currLocalId + 1)
    }

    if (edgeDirection == EdgeDirection.Both) {
      localDegreeHist = new Array[Int](local2global.size)
      global2local.foreach { case (vid, idx) =>
        localDegreeHist(idx) = vertexHist(vid)
      }
    }

    new EdgePartition[VD, ED](localSrcIds, localDstIds, data, index,
      global2local, local2global.trim().array, vertexAttrs, localDegreeHist, None)
  }
}

class ExistingEdgePartitionBuilder[VD: ClassTag: TypeTag, @spec(Long, Int, Float, Double) ED: ClassTag]
(global2local: FastHashMap[VertexId, Int],
 local2global: Array[VertexId],
 vertexAttrs: Array[VD],
 localDegreeHist: Array[Int],
 activeSet: Option[VertexSet],
 size: Int = 64,
 edgeDirection: EdgeDirection = EdgeDirection.Out) {
  private[this] val edges = new FastArray[EdgeWithLocalIds[ED]](size)

  /** Add a new edge to the partition. */
  def add(src: VertexId, dst: VertexId, localSrc: Int, localDst: Int, d: ED): this.type = {
    edges += EdgeWithLocalIds(src, dst, localSrc, localDst, d)
    this
  }

  def add(edge: EdgeWithLocalIds[ED]): this.type = {
    edges += edge
    this
  }

  def build: EdgePartition[VD, ED] = {
    val edgeArray = edges.trim().array

    val localSrcIds = new Array[Int](edgeArray.length)
    val localDstIds = new Array[Int](edgeArray.length)
    val data = new Array[ED](edgeArray.length)
    val index = new FastHashMap[VertexId, (Int, Int)]

    if (edgeDirection == EdgeDirection.Out) {
      new Sorter(EdgeWithLocalIds.sortDataFormat[ED])
        .sort(edgeArray, 0, edgeArray.length, EdgeWithLocalIds.srcOrdering[ED])

      var (key, start, len) = (edgeArray.head.srcId, 0, 0)
      edgeArray.zipWithIndex.foreach { case (edge, idx) =>
        if (edge.srcId == key) {
          len += 1
        } else {
          index.put(key, (start, len))
          key = edge.srcId
          start = idx
          len = 1
        }
      }
    } else if (edgeDirection == EdgeDirection.In) {
      new Sorter(EdgeWithLocalIds.sortDataFormat[ED])
        .sort(edgeArray, 0, edgeArray.length, EdgeWithLocalIds.dstOrdering[ED])

      var (key, start, len) = (edgeArray.head.dstId, 0, 0)
      edgeArray.zipWithIndex.foreach { case (edge, idx) =>
        if (edge.dstId == key) {
          len += 1
        } else {
          index.put(key, (start, len))
          key = edge.dstId
          start = idx
          len = 1
        }
      }
    } else if (edgeDirection == EdgeDirection.Both) {
      val vertexHist = EdgeWithLocalIds.vertexHist(edgeArray)
      val pairOrdering = EdgeWithLocalIds.pairOrdering[ED](vertexHist)
      new Sorter(EdgeWithLocalIds.sortDataFormat[ED])
        .sort(edgeArray, 0, edgeArray.length, pairOrdering)

      def orderKey(edge: EdgeWithLocalIds[ED]): VertexId = {
        if (vertexHist(edge.srcId) < vertexHist(edge.dstId)) {
          edge.srcId
        } else if (vertexHist(edge.srcId) > vertexHist(edge.dstId)) {
          edge.dstId
        } else {
          if (edge.srcId < edge.dstId) {
            edge.srcId
          } else {
            edge.dstId
          }
        }
      }

      var (key, start, len) = (orderKey(edgeArray.head), 0, 0)
      edgeArray.zipWithIndex.foreach { case (edge, idx) =>
        val currKey = orderKey(edge)
        if (currKey == key) {
          len += 1
        } else {
          index.put(key, (start, len))
          key = currKey
          start = idx
          len = 1
        }
      }
    } else {
      new Sorter(EdgeWithLocalIds.sortDataFormat[ED])
        .sort(edgeArray, 0, edgeArray.length, EdgeWithLocalIds.srcOrdering[ED])

      var (key, start, len) = (edgeArray.head.srcId, 0, 0)
      edgeArray.zipWithIndex.foreach { case (edge, idx) =>
        if (edge.srcId == key) {
          len += 1
        } else {
          index.put(key, (start, len))
          key = edge.srcId
          start = idx
          len = 1
        }
      }
    }

    if (edgeArray.length > 0) {
      var pos = 0
      while (pos < edgeArray.length) {
        val edge = edgeArray(pos)
        localSrcIds(pos) = edge.localSrcId
        localDstIds(pos) = edge.localDstId
        data(pos) = edge.attr
        pos += 1
      }
    }

    new EdgePartition[VD, ED](localSrcIds, localDstIds, data, index,
      global2local, local2global, vertexAttrs, localDegreeHist, activeSet)
  }
}

private[framework] case class EdgeWithLocalIds[@specialized ED](srcId: VertexId, dstId: VertexId, localSrcId: Int, localDstId: Int, attr: ED)

private[framework] object EdgeWithLocalIds {
  def srcOrdering[ED]: Ordering[EdgeWithLocalIds[ED]] = {
    new Ordering[EdgeWithLocalIds[ED]] {
      override def compare(a: EdgeWithLocalIds[ED], b: EdgeWithLocalIds[ED]): Int = {
        if (a.srcId == b.srcId) {
          if (a.dstId == b.dstId) 0
          else if (a.dstId < b.dstId) -1
          else 1
        } else if (a.srcId < b.srcId) -1
        else 1
      }
    }
  }

  def dstOrdering[ED]: Ordering[EdgeWithLocalIds[ED]] = {
    new Ordering[EdgeWithLocalIds[ED]] {
      override def compare(a: EdgeWithLocalIds[ED], b: EdgeWithLocalIds[ED]): Int = {
        if (a.dstId == b.dstId) {
          if (a.srcId == b.srcId) 0
          else if (a.srcId < b.srcId) -1
          else 1
        } else if (a.dstId < b.dstId) -1
        else 1
      }
    }
  }

  def vertexHist[ED](edges: Array[EdgeWithLocalIds[ED]]): FastHashMap[VertexId, Int] = {
    val map = new FastHashMap[VertexId, Int](Math.max(edges.length / 2, 64))

    edges.foreach { edge =>
      map.changeValue(edge.srcId, 1, v => v + 1)
      map.changeValue(edge.dstId, 1, v => v + 1)
    }

    map
  }

  def pairOrdering[ED](map: FastHashMap[VertexId, Int]): Ordering[EdgeWithLocalIds[ED]] = {
    new Ordering[EdgeWithLocalIds[ED]] {
      override def compare(a: EdgeWithLocalIds[ED], b: EdgeWithLocalIds[ED]): Int = {
        val (asc: Int, adc: Int) = (map(a.srcId), map(a.dstId))
        val (bsc: Int, bdc: Int) = (map(b.srcId), map(b.dstId))

        val (ahist, aver) = if (asc <= adc) (asc, a.srcId) else (adc, a.dstId)
        val (bhist, bver) = if (bsc <= bdc) (bsc, b.srcId) else (bdc, b.dstId)

        if (ahist == bhist) {
          if (aver == bver) 0
          else if (aver < bver) -1
          else 1
        } else if (ahist < bhist) -1
        else 1
      }
    }
  }

  def sortDataFormat[ED]: SortDataFormat[EdgeWithLocalIds[ED], Array[EdgeWithLocalIds[ED]]] = {
    new SortDataFormat[EdgeWithLocalIds[ED], Array[EdgeWithLocalIds[ED]]] {
      override def getKey(data: Array[EdgeWithLocalIds[ED]], pos: Int): EdgeWithLocalIds[ED] = {
        data(pos)
      }

      override def swap(data: Array[EdgeWithLocalIds[ED]], pos0: Int, pos1: Int): Unit = {
        val tmp = data(pos0)
        data(pos0) = data(pos1)
        data(pos1) = tmp
      }

      override def copyElement(src: Array[EdgeWithLocalIds[ED]], srcPos: Int,
                               dst: Array[EdgeWithLocalIds[ED]], dstPos: Int) {
        dst(dstPos) = src(srcPos)
      }

      override def copyRange(src: Array[EdgeWithLocalIds[ED]], srcPos: Int,
                             dst: Array[EdgeWithLocalIds[ED]], dstPos: Int, length: Int) {
        System.arraycopy(src, srcPos, dst, dstPos, length)
      }

      override def allocate(length: Int): Array[EdgeWithLocalIds[ED]] = {
        new Array[EdgeWithLocalIds[ED]](length)
      }
    }
  }
}