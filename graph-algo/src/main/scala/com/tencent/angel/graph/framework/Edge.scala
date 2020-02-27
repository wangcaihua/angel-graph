package com.tencent.angel.graph.framework

import java.nio.ByteBuffer

import com.tencent.angel.graph._
import com.tencent.angel.graph.utils.{FastHashMap, SortDataFormat}

import scala.{specialized => spec}

class Edge[@spec(Boolean, Char, Byte, Short, Int, Long, Float, Double) ED]
(val srcId: VertexId, val dstId: VertexId, val attr: ED) extends Serializable {
  override def toString: String = s"$srcId\t$dstId\t${attr.toString}"
}

object Edge {

  def apply[ED](srcId: VertexId, dstId: VertexId, attr: ED): Edge[ED] = new Edge(srcId, dstId, attr)

  def unapply[ED](edge: Edge[ED]): Option[(VertexId, VertexId, ED)] = {
    if (edge == null) {
      None
    } else {
      Some((edge.srcId, edge.srcId, edge.attr))
    }
  }

  def srcOrdering[ED]: Ordering[Edge[ED]] = {
    new Ordering[Edge[ED]] {
      override def compare(a: Edge[ED], b: Edge[ED]): Int = {
        if (a.srcId == b.srcId) {
          if (a.dstId == b.dstId) 0
          else if (a.dstId < b.dstId) -1
          else 1
        } else if (a.srcId < b.srcId) -1
        else 1
      }
    }
  }

  def dstOrdering[ED]: Ordering[Edge[ED]] = {
    new Ordering[Edge[ED]] {
      override def compare(a: Edge[ED], b: Edge[ED]): Int = {
        if (a.dstId == b.dstId) {
          if (a.srcId == b.srcId) 0
          else if (a.srcId < b.srcId) -1
          else 1
        } else if (a.dstId < b.dstId) -1
        else 1
      }
    }
  }

  def vertexHist[ED](edges: Array[Edge[ED]]): FastHashMap[VertexId, Int] = {
    val map = new FastHashMap[VertexId, Int](Math.max(edges.length / 2, 64))

    edges.foreach { edge =>
      map.changeValue(edge.srcId, 1, v => v + 1)
      map.changeValue(edge.dstId, 1, v => v + 1)
    }

    map
  }

  def pairOrdering[ED](map: FastHashMap[VertexId, Int]): Ordering[Edge[ED]] = {
    new Ordering[Edge[ED]] {
      override def compare(a: Edge[ED], b: Edge[ED]): Int = {
        val (asc: Int, adc: Int) = (map(a.srcId), map(a.dstId))
        val (bsc: Int, bdc: Int) = (map(b.srcId), map(b.dstId))

        val (firstA, secondA, thirdA) = if (asc < adc) (asc, a.srcId, a.dstId)
        else if (asc == adc && a.srcId < a.dstId) (asc, a.srcId, a.dstId)
        else if (asc == adc && a.srcId > a.dstId) (adc, a.dstId, a.srcId)
        else if (asc == adc && a.srcId == a.dstId) (adc, a.dstId, a.srcId)
        else (adc, a.dstId, a.srcId)

        val (firstB, secondB, thirdB) = if (bsc < bdc) (bsc, b.srcId, b.dstId)
        else if (bsc == bdc && b.srcId < b.dstId) (bsc, b.srcId, b.dstId)
        else if (bsc == bdc && b.srcId > b.dstId) (bdc, b.dstId, b.srcId)
        else if (bsc == bdc && b.srcId == b.dstId) (bdc, b.dstId, b.srcId)
        else (bdc, b.dstId, b.srcId)

        if (firstA == firstB) {
          if (secondA == secondB)
            if (thirdA == thirdB) 0
            else if (thirdA < thirdB) -1
            else 1
          else if (secondA < secondB) -1
          else 1
        } else if (firstA < firstB) -1
        else 1
      }
    }
  }

  def sortDataFormat[ED]: SortDataFormat[Edge[ED], Array[Edge[ED]]] = {
    new SortDataFormat[Edge[ED], Array[Edge[ED]]] {
      override def getKey(data: Array[Edge[ED]], pos: Int): Edge[ED] = {
        data(pos)
      }

      override def swap(data: Array[Edge[ED]], pos0: Int, pos1: Int): Unit = {
        val tmp = data(pos0)
        data(pos0) = data(pos1)
        data(pos1) = tmp
      }

      override def copyElement(src: Array[Edge[ED]], srcPos: Int,
                               dst: Array[Edge[ED]], dstPos: Int) {
        dst(dstPos) = src(srcPos)
      }

      override def copyRange(src: Array[Edge[ED]], srcPos: Int,
                             dst: Array[Edge[ED]], dstPos: Int, length: Int) {
        System.arraycopy(src, srcPos, dst, dstPos, length)
      }

      override def allocate(length: Int): Array[Edge[ED]] = {
        new Array[Edge[ED]](length)
      }
    }
  }
}

case class EdgeTriplet[VD, ED](srcId: VertexId, dstId: VertexId, srcAttr: VD, dstAttr: VD, attr: ED)
  extends Serializable {
  def edge: Edge[ED] = Edge(srcId, dstId, attr)

  def src: Node[VD] = Node(srcId, srcAttr)

  def dst: Node[VD] = Node(dstId, dstAttr)
}

class TripletFields(val useSrc: Boolean, val useDst: Boolean, val useEdge: Boolean)

object TripletFields {
  def apply(): TripletFields = {
    new TripletFields(true, true, true)
  }

  def apply(useSrc: Boolean, useDsr: Boolean, useEdge: Boolean): TripletFields = {
    new TripletFields(useSrc, useDsr, useEdge)
  }

  val None: TripletFields = new TripletFields(false, false, false)
  val EdgeOnly: TripletFields = new TripletFields(false, false, true)
  val Src: TripletFields = new TripletFields(true, false, false)
  val Dst: TripletFields = new TripletFields(false, true, false)
  val All: TripletFields = new TripletFields(true, true, true)
}

abstract class EdgeContext[VD, ED, A] {
  protected var _srcId: VertexId = _
  protected var _dstId: VertexId = _
  protected var _srcAttr: VD = _
  protected var _dstAttr: VD = _
  protected var _attr: ED = _

  def srcId: VertexId = _srcId

  def dstId: VertexId = _dstId

  def srcAttr: VD = _srcAttr

  def dstAttr: VD = _dstAttr

  def attr: ED = _attr


  def sendToSrc(msg: A): Unit

  def sendToDst(msg: A): Unit

  def toEdgeTriplet: EdgeTriplet[VD, ED] = {
    EdgeTriplet(_srcId, _dstId, _srcAttr, _dstAttr, _attr)
  }
}

object EdgeContext {
  def unapply[VD, ED, A](edge: EdgeContext[VD, ED, A]): Some[(VertexId, VertexId, VD, VD, ED)] =
    Some((edge.srcId, edge.dstId, edge.srcAttr, edge.dstAttr, edge.attr))
}

object EdgeActiveness extends Enumeration with Serializable {
  type EdgeActiveness = Value
  val Neither, SrcOnly, DstOnly, Both, Either = Value
}

object EdgeDirection extends Enumeration with Serializable {
  type EdgeDirection = Value
  val In, Out, Either, Both = Value
}
