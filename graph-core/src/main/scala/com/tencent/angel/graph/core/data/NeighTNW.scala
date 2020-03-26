package com.tencent.angel.graph.core.data

import java.util

import com.tencent.angel.graph.core.sampler.{ARes, BetWheel, SampleK, SampleOne}
import com.tencent.angel.graph.utils.FastArray
import com.tencent.angel.graph.{VertexId, VertexType, WgtTpe, defaultVertexType}
import it.unimi.dsi.fastutil.shorts.Short2ObjectOpenHashMap

case class NeighTNW(tpe: VertexType, neighs: Short2ObjectOpenHashMap[Array[VertexId]],
                    weights: Short2ObjectOpenHashMap[Array[WgtTpe]])
  extends Neighbor with Typed {
  def this() = this(0.toShort, null, null)

  @transient private lazy val sample1: Short2ObjectOpenHashMap[SampleOne] =
    new Short2ObjectOpenHashMap[SampleOne](neighs.size())
  @transient private lazy val samplek: Short2ObjectOpenHashMap[SampleK] =
    new Short2ObjectOpenHashMap[SampleK](neighs.size())

  override def sample(tpe: VertexType): VertexId = {
    if (sample1.containsKey(tpe)) {
      sample1.get(tpe).sample()
    } else if (neighs.containsKey(tpe)) {
      val sampler = new BetWheel(neighs.get(tpe), weights.get(tpe))
      sample1.put(tpe, sampler)
      sampler.sample()
    } else {
      throw new Exception(s"the type $tpe is not exist!")
    }
  }

  override def sample(tpe: VertexType, k: Int): Array[VertexId] = {
    if (samplek.containsKey(tpe)) {
      samplek.get(tpe).sample(k)
    } else if (neighs.containsKey(tpe)) {
      val sampler = new ARes(neighs.get(tpe), weights.get(tpe))
      samplek.put(tpe, sampler)
      sampler.sample(k)
    } else {
      throw new Exception(s"the type $tpe is not exist!")
    }
  }

  override def hasNeighbor(tpe: VertexType, neigh: VertexId): Boolean = {
    util.Arrays.binarySearch(neighs.get(tpe), neigh) >= 0
  }

  def merge(other: NeighTNW): NeighTNW = {
    assert(tpe == other.tpe)

    if (other.neighs.size() > 0) {
      val iter = other.neighs.short2ObjectEntrySet().fastIterator()
      while (iter.hasNext) {
        val entry = iter.next()
        val key = entry.getShortKey
        val otherNeigh = entry.getValue
        val otherWeight = other.weights.get(key)

        if (neighs.containsKey(key)) {
          val sorted = (neighs.get(key) ++ otherNeigh).zip(weights.get(key) ++ otherWeight).sortBy(_._1)

          var last = sorted.head._1 - 1
          val (newNeighs, newWeights) = sorted.filter { case (curr, _) =>
            if (curr != last) {
              last = curr
              true
            } else {
              false
            }
          }.unzip

          neighs.put(key, newNeighs)
          weights.put(key, newWeights)
        } else {
          neighs.put(key, otherNeigh)
          weights.put(key, otherWeight)
        }
      }
    }

    this
  }

  def mergeSorted(other: NeighTNW): NeighTNW = {
    assert(tpe == other.tpe)

    if (other.neighs.size() > 0) {
      val iter = other.neighs.short2ObjectEntrySet().fastIterator()
      while (iter.hasNext) {
        val entry = iter.next()
        val key = entry.getShortKey
        val otherNeighs = entry.getValue
        val otherWeights = other.weights.get(key)

        if (neighs.containsKey(key)) {
          val thisNeighs = neighs.get(key)
          val thisWeights = weights.get(key)

          val sorted = new Array[VertexId](thisNeighs.length + otherNeighs.length)
          val mWeights = new Array[WgtTpe](thisNeighs.length + otherNeighs.length)

          var (left, right) = (0, 0)
          var idx = 0
          var last = if (thisNeighs.head < otherNeighs.head) thisNeighs.head -1 else otherNeighs.head -1
          while (left < thisNeighs.length && right < otherNeighs.length) {
            if (thisNeighs(left) <= otherNeighs(right)) {
              val candidate = thisNeighs(left)
              if (last != candidate) {
                sorted(idx) = candidate
                mWeights(idx) = thisWeights(left)
                last = candidate
                idx += 1
              }

              left += 1
            } else { // thisNeighs(left) > otherNeighs(right)
              val candidate = otherNeighs(right)
              if (last != candidate) {
                sorted(idx) = candidate
                mWeights(idx) = otherWeights(right)
                last = candidate
                idx += 1
              }

              right += 1
            }
          }

          while(left < thisNeighs.length) {
            val candidate = thisNeighs(left)
            if (last != candidate) {
              sorted(idx) = candidate
              mWeights(idx) = thisWeights(left)
              last = candidate
              idx += 1
            }

            left += 1
          }

          while(right < otherNeighs.length) {
            val candidate =otherNeighs(right)
            if (last != candidate) {
              sorted(idx) = candidate
              mWeights(idx) = otherWeights(right)
              last = candidate
              idx += 1
            }

            right += 1
          }

          if (idx == sorted.length) {
            neighs.put(key, sorted)
            weights.put(key, mWeights)
          } else if (idx < sorted.length) {
            val deduplicateNeighs = new Array[VertexId](idx)
            val deduplicateWeights = new Array[WgtTpe](idx)
            Array.copy(sorted, 0, deduplicateNeighs, 0, idx)
            Array.copy(mWeights, 0, deduplicateWeights, 0, idx)
            neighs.put(key, deduplicateNeighs)
            weights.put(key, deduplicateWeights)
          } else {
            throw new Exception("Error!")
          }
        } else {
          neighs.put(key, otherNeighs)
          weights.put(key, otherWeights)
        }
      }
    }

    this
  }
}


class NeighTNWBuilder extends TypedNeighborBuilder[NeighTNW] {
  private var tpe: VertexType = defaultVertexType
  private val neighs = new Short2ObjectOpenHashMap[FastArray[VertexId]]()
  private val weights = new Short2ObjectOpenHashMap[FastArray[WgtTpe]]()

  override def setTpe(tpe: VertexType): this.type = {
    this.tpe = tpe
    this
  }

  override def add(vt: VertexType, neigh: VertexId, weight: WgtTpe): this.type = {
    if (neighs.containsKey(vt)) {
      neighs.get(vt) += neigh
      weights.get(vt) += weight
    } else {
      val tempN = new FastArray[VertexId]()
      tempN += neigh
      neighs.put(vt, tempN)

      val tempW = new FastArray[WgtTpe]()
      tempW += weight
      weights.put(vt, tempW)
    }

    this
  }

  override def add(neigh: NeighTNW): this.type = {
    tpe = neigh.tpe
    val iter = neigh.neighs.short2ObjectEntrySet().fastIterator()
    while (iter.hasNext) {
      val entry = iter.next()
      val vt = entry.getShortKey
      val vtNeighs = entry.getValue
      val vtWeights = neigh.weights.get(vt)

      if (neighs.containsKey(vt)) {
        vtNeighs.foreach { n => neighs.get(vt) += n }
        vtWeights.foreach { w => weights.get(vt) += w }
      } else {
        val tempN = new FastArray[VertexId]()
        vtNeighs.foreach { n => tempN += n }
        neighs.put(vt, tempN)

        val tempW = new FastArray[WgtTpe]()
        vtWeights.foreach { w => tempW += w }
        weights.put(vt, tempW)
      }
    }

    this
  }

  override def build: NeighTNW = {
    assert(neighs.size == weights.size)
    val tempN = new Short2ObjectOpenHashMap[Array[VertexId]](neighs.size())
    val tempW = new Short2ObjectOpenHashMap[Array[WgtTpe]](neighs.size())

    val iter = neighs.short2ObjectEntrySet().fastIterator()
    while (iter.hasNext) {
      val entry = iter.next()
      val vt = entry.getShortKey
      val arr = entry.getValue

      val sorted = arr.trim().array.zip(weights.get(vt).trim().array).sortBy(_._1)
      var last = sorted.head._1 - 1
      val (newNeighs, newWeights) = sorted.filter { case (curr, _) =>
        if (curr != last) {
          last = curr
          true
        } else {
          false
        }
      }.unzip

      tempN.put(vt, newNeighs)
      tempW.put(vt, newWeights)
    }

    NeighTNW(tpe, tempN, tempW)
  }
}


object NeighTNW {
  def builder(): NeighTNWBuilder = {
    new NeighTNWBuilder
  }
}