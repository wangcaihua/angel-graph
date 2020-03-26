package com.tencent.angel.graph.core.data

import java.util

import com.tencent.angel.graph.VertexId
import com.tencent.angel.graph.core.sampler.{Reservoir, SampleK, SampleOne, Simple}
import com.tencent.angel.graph.utils.FastArray

case class NeighN(neighs: Array[VertexId]) extends Neighbor with UnTyped {
  def this() = this(null)

  @transient private lazy val sample1: SampleOne = new Simple(neighs)
  @transient private lazy val samplek: SampleK = new Reservoir(neighs)

  override def sample(): VertexId = sample1.sample()

  override def sample(k: Int): Array[VertexId] = samplek.sample(k)

  override def hasNeighbor(neigh: VertexId): Boolean = {
    util.Arrays.binarySearch(neighs, neigh) >= 0
  }

  def merge(other: NeighN): NeighN = {
    val sorted = (neighs ++ other.neighs).sorted

    var last = sorted.head - 1
    val deduplicate = sorted.filter { curr =>
      if (curr != last) {
        last = curr
        true
      } else {
        false
      }
    }

    new NeighN(deduplicate)
  }

  def mergeSorted(other: NeighN): NeighN = {
    val sorted = new Array[VertexId](neighs.length + other.neighs.length)

    var (left, right) = (0, 0)
    var idx = 0
    var last = if (neighs.head < other.neighs.head) neighs.head -1 else other.neighs.head -1
    while (left < neighs.length && right < other.neighs.length) {
      if (neighs(left) <= other.neighs(right)) {
        val candidate = neighs(left)
        if (last != candidate) {
          sorted(idx) = candidate
          last = candidate
          idx += 1
        }

        left += 1
      } else { // neighs(left) > other.neighs(right)
        val candidate =other.neighs(right)
        if (last != candidate) {
          sorted(idx) = candidate
          last = candidate
          idx += 1
        }

        right += 1
      }
    }

    while(left < neighs.length) {
      val candidate = neighs(left)
      if (last != candidate) {
        sorted(idx) = candidate
        last = candidate
        idx += 1
      }

      left += 1
    }

    while(right < other.neighs.length) {
      val candidate =other.neighs(right)
      if (last != candidate) {
        sorted(idx) = candidate
        last = candidate
        idx += 1
      }

      right += 1
    }

    if (idx == sorted.length) {
      new NeighN(sorted)
    } else if (idx < sorted.length) {
      val deduplicate = new Array[VertexId](idx)
      Array.copy(sorted, 0, deduplicate, 0, idx)
      new NeighN(deduplicate)
    } else {
      throw new Exception("Error!")
    }
  }
}

class NeighNBuilder extends UnTypedNeighborBuilder[NeighN] {
  private val neighs = new FastArray[VertexId]()

  override def add(neigh: VertexId): this.type = {
    neighs += neigh
    this
  }

  override def add(neigh: NeighN): this.type = {
    neighs.foreach(n => neighs += n)

    this
  }

  override def build: NeighN = {
    val sorted = neighs.trim().array.sorted

    var last = sorted.head - 1
    val deduplicate = sorted.filter { curr =>
      if (curr != last) {
        last = curr
        true
      } else {
        false
      }
    }

    new NeighN(deduplicate)
  }
}

object NeighN {
  def builder(): NeighNBuilder = {
    new NeighNBuilder
  }
}
