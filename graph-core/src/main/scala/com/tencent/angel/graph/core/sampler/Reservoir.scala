package com.tencent.angel.graph.core.sampler

import com.tencent.angel.graph.VertexId

class Reservoir(neighs: Array[VertexId]) extends SampleK {
  override def sample(k: Int): Array[VertexId] = {
    if (neighs.length <= k) {
      neighs
    } else {
      val result = new Array[VertexId](k)

      var idx = 0
      neighs.foreach{ neigh =>
        if (idx < k) {
          result(idx) = neigh
        } else {
          val rIdx = Sampler.rand.nextInt(idx + 1)
          if (rIdx < k) {
            result(rIdx) = neigh
          }
        }

        idx += 1
      }

      result
    }
  }
}