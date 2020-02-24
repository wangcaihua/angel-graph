package com.tencent.angel

import com.tencent.angel.graph.utils.FastHashSet

package object graph {
  type VertexId = Long
  type WgtTpe = Float
  type PartitionID = Int

  type VertexSet = FastHashSet[VertexId]
}