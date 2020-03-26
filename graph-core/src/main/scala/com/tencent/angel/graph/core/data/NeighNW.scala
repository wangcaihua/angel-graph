package com.tencent.angel.graph.core.data

import java.util

import com.tencent.angel.graph.core.sampler.{ARes, BetWheel, SampleK, SampleOne}
import com.tencent.angel.graph.utils.FastArray
import com.tencent.angel.graph.{VertexId, WgtTpe}

case class NeighNW(neighs: Array[VertexId], weights: Array[WgtTpe])
  extends Neighbor with UnTyped {
  def this() = this(null, null)

  @transient private lazy val sample1: SampleOne = new BetWheel(neighs, weights)
  @transient private lazy val samplek: SampleK = new ARes(neighs, weights)

  override def sample(): VertexId = sample1.sample()

  override def sample(k: Int): Array[VertexId] = samplek.sample(k)

  override def hasNeighbor(neigh: VertexId): Boolean = {
    util.Arrays.binarySearch(neighs, neigh) >= 0
  }

  def merge(other: NeighNW): NeighNW = {
    val sorted = (neighs ++ other.neighs).zip(weights ++ other.weights).sortBy(_._1)

    var last = sorted.head._1 - 1
    val (newNeighs, newWeights) = sorted.filter { case (curr, _) =>
      if (curr != last) {
        last = curr
        true
      } else {
        false
      }
    }.unzip

    NeighNW(newNeighs, newWeights)
  }

  def mergeSorted(other: NeighNW): NeighNW = {
    val sorted = new Array[VertexId](neighs.length + other.neighs.length)
    val mWeights = new Array[WgtTpe](neighs.length + other.neighs.length)

    var (left, right) = (0, 0)
    var idx = 0
    var last = if (neighs.head < other.neighs.head) neighs.head - 1 else other.neighs.head - 1
    while (left < neighs.length && right < other.neighs.length) {
      if (neighs(left) <= other.neighs(right)) {
        val candidate = neighs(left)
        if (last != candidate) {
          sorted(idx) = candidate
          mWeights(idx) = weights(left)
          last = candidate
          idx += 1
        }

        left += 1
      } else { // neighs(left) > other.neighs(right)
        val candidate = other.neighs(right)
        if (last != candidate) {
          sorted(idx) = candidate
          mWeights(idx) = other.weights(right)
          last = candidate
          idx += 1
        }

        right += 1
      }
    }

    while (left < neighs.length) {
      val candidate = neighs(left)
      if (last != candidate) {
        sorted(idx) = candidate
        mWeights(idx) = weights(left)
        last = candidate
        idx += 1
      }

      left += 1
    }

    while (right < other.neighs.length) {
      val candidate = other.neighs(right)
      if (last != candidate) {
        sorted(idx) = candidate
        mWeights(idx) = other.weights(right)
        last = candidate
        idx += 1
      }

      right += 1
    }

    if (idx == sorted.length) {
      new NeighNW(sorted, mWeights)
    } else if (idx < sorted.length) {
      val deduplicateNeighs = new Array[VertexId](idx)
      val deduplicateWeights = new Array[WgtTpe](idx)
      Array.copy(sorted, 0, deduplicateNeighs, 0, idx)
      Array.copy(mWeights, 0, deduplicateWeights, 0, idx)
      new NeighNW(deduplicateNeighs, deduplicateWeights)
    } else {
      throw new Exception("Error!")
    }
  }
}

class NeighNWBuilder extends UnTypedNeighborBuilder[NeighNW] {
  private val neighs = new FastArray[VertexId]()
  private val weights = new FastArray[WgtTpe]()

  override def add(neigh: VertexId, weight: WgtTpe): this.type = {
    neighs += neigh
    weights += weight
    this
  }

  override def add(neigh: NeighNW): this.type = {
    neigh.neighs.foreach(n => neighs += n)
    neigh.weights.foreach(w => weights += w)

    this
  }

  override def build: NeighNW = {
    assert(neighs.size == weights.size)
    val sorted = neighs.trim().array.zip(weights.trim().array).sortBy(_._1)

    var last = sorted.head._1 - 1
    val (newNeighs, newWeights) = sorted.filter { case (curr, _) =>
      if (curr != last) {
        last = curr
        true
      } else {
        false
      }
    }.unzip

    NeighNW(newNeighs, newWeights)
  }
}

object NeighNW {
  def builder(): NeighNWBuilder = {
    new NeighNWBuilder
  }
}