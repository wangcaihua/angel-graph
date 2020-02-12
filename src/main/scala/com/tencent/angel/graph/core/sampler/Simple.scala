package com.tencent.angel.graph.core.sampler

import com.tencent.angel.graph.VertexId

class Simple(neighs: Array[VertexId]) extends SampleOne {
  override def sample(): VertexId = {
    neighs(Sampler.rand.nextInt(neighs.length))
  }
}
