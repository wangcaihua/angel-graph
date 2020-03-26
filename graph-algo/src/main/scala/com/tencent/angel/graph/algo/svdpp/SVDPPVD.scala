package com.tencent.angel.graph.algo.svdpp

import com.tencent.angel.graph.core.data.GData

case class SVDPPVD(v1: Array[Double], v2:Array[Double], v3:Double, v4: Double) extends GData {
  def this() = this(null, null, 0.0, 0.0)
}
