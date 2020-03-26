package com.tencent.angel.graph.core.sampler

import java.util.Random

import com.tencent.angel.graph.VertexId

trait Sampler

abstract class SampleOne extends Sampler {
  def sample(): VertexId
}

abstract class SampleK extends Sampler {
  def sample(k: Int): Array[VertexId]
}

object Sampler {
  val rand = new Random()
}
