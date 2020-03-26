package com.tencent.angel.graph.core.data

import com.tencent.angel.graph.{VertexId, VertexType, WgtTpe}

trait Neighbor extends GData

trait UnTyped {
  def sample(): VertexId

  def sample(k: Int): Array[VertexId]

  def hasNeighbor(neigh: VertexId): Boolean
}

trait Typed {
  def sample(tpe: VertexType): VertexId

  def sample(tpe: VertexType, k: Int): Array[VertexId]

  def hasNeighbor(tpe: VertexType, neigh: VertexId): Boolean
}

trait UnTypedNeighborBuilder[N <: UnTyped] {
  def add(neigh: VertexId): this.type = ???

  def add(neigh: VertexId, weight: WgtTpe): this.type = ???

  def add(other: N): this.type

  def build: N
}


trait TypedNeighborBuilder[N <: Typed] {
  def setTpe(tpe: VertexType): this.type

  def add(vt: VertexType, neigh: VertexId): this.type = ???

  def add(vt: VertexType, neigh: VertexId, weight: WgtTpe): this.type = ???

  def add(other: N): this.type

  def build: N
}