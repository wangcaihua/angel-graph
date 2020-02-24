package com.tencent.angel.graph.framework

import com.tencent.angel.graph.VertexId


case class EdgeTriplet[VD, ED](srcId: VertexId, dstId:VertexId, srcAttr: VD, dstAttr: VD, attr: ED)
  extends Serializable {
  def edge: Edge[ED] = Edge(srcId, dstId, attr)
  def src: Node[VD] = Node(srcId, srcAttr)
  def dst: Node[VD] = Node(dstId, dstAttr)
}

class TripletFields(val useSrc:Boolean, val useDst: Boolean, val useEdge: Boolean)

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
