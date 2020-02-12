package com.tencent.angel.graph.core.sampler

import com.tencent.angel.graph.{VertexId, WgtTpe}

class BetWheel(neighs: Array[VertexId], weights: Array[WgtTpe]) extends SampleOne {
  assert(neighs != null && weights != null)
  assert(weights.length == neighs.length && weights.length > 0)

  private val wSum = weights.sum

  override def sample(): VertexId = {
    val threshold = wSum * Sampler.rand.nextFloat()
    var (partSum, idx) = (0.0.asInstanceOf[WgtTpe], 0)

    while (partSum <= threshold) {
      partSum += weights(idx)
      idx += 1
    }

    neighs(idx-1)
  }
}
