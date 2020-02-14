package com.tencent.angel

package object graph {
  type VertexId = Long
  type WgtTpe = Float

  val ALL = 0
  val NEIGHBOR = 1
  val WEIGHT = 2
  val ATTRIBUTE = 3
  val NEIGHBOR_WEIGHT = 4
  val NEIGHBOR_ATTRIBUTE = 5
  val WEIGHT_ATTRIBUTE = 6
  val OTHER = 7
}