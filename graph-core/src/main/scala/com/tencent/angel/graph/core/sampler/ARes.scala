package com.tencent.angel.graph.core.sampler

import scala.collection.mutable
import com.tencent.angel.graph.{VertexId, WgtTpe}

class ARes(var neighs: Array[VertexId], var weights: Array[WgtTpe]) extends SampleK {
  private implicit val comparator = new Ordering[(VertexId, WgtTpe)]() {
    override def compare(x: (VertexId, WgtTpe), y: (VertexId, WgtTpe)): Int = {
      if (x._2 <= y._2) 1 else -1
    }
  }

  override def sample(k: Int): Array[VertexId] = {
    if (neighs.length <= k) {
      neighs
    } else {
      val minHeap = new mutable.PriorityQueue[(VertexId, WgtTpe)]

      var idx: Int = 0
      neighs.zip(weights).foreach { case (neigh, weight) =>
        val we = (Math.log(Sampler.rand.nextDouble()) / weight)
          .asInstanceOf[WgtTpe]
        if (idx < k) {
          minHeap += (neighs(idx) -> we)
        } else {
          val (_, minValue) = minHeap.head
          if (we > minValue) {
            minHeap.dequeue()
            minHeap += (neighs(idx) -> we)
          }
        }

        idx += 1
      }

      minHeap.iterator.map(_._1).toArray
    }
  }
}