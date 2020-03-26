package com.tencent.angel.graph.core.data

import java.util

import com.tencent.angel.graph.core.sampler.{Reservoir, SampleK, SampleOne, Simple}
import com.tencent.angel.graph.utils.FastArray
import com.tencent.angel.graph.{VertexId, VertexType, defaultVertexType}
import it.unimi.dsi.fastutil.shorts.Short2ObjectOpenHashMap

case class NeighTN(tpe: VertexType, neighs: Short2ObjectOpenHashMap[Array[VertexId]])
  extends Neighbor with Typed {
  def this() = this(0.toShort, null)

  @transient private lazy val sample1: Short2ObjectOpenHashMap[SampleOne] =
    new Short2ObjectOpenHashMap[SampleOne](neighs.size())
  @transient private lazy val samplek: Short2ObjectOpenHashMap[SampleK] =
    new Short2ObjectOpenHashMap[SampleK](neighs.size())

  override def sample(tpe: VertexType): VertexId = {
    if (sample1.containsKey(tpe)) {
      sample1.get(tpe).sample()
    } else if (neighs.containsKey(tpe)) {
      val sampler = new Simple(neighs.get(tpe))
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
      val sampler = new Reservoir(neighs.get(tpe))
      samplek.put(tpe, sampler)
      sampler.sample(k)
    } else {
      throw new Exception(s"the type $tpe is not exist!")
    }
  }

  override def hasNeighbor(tpe: VertexType, neigh: VertexId): Boolean = {
    util.Arrays.binarySearch(neighs.get(tpe), neigh) >= 0
  }

  def merge(other: NeighTN): NeighTN = {
    assert(tpe == other.tpe)

    if (other.neighs.size() > 0) {
      val iter = other.neighs.short2ObjectEntrySet().fastIterator()
      while (iter.hasNext) {
        val entry = iter.next()
        val key = entry.getShortKey
        val value = entry.getValue

        if (neighs.containsKey(key)) {
          val sorted = (neighs.get(key) ++ value).sorted

          var last = sorted.head - 1
          val deduplicate = sorted.filter { curr =>
            if (curr != last) {
              last = curr
              true
            } else {
              false
            }
          }

          neighs.put(key, deduplicate)
        } else {
          neighs.put(key, value)
        }
      }
    }

    this
  }

  def mergeSorted(other: NeighTN): NeighTN = {
    assert(tpe == other.tpe)

    if (other.neighs.size() > 0) {
      val iter = other.neighs.short2ObjectEntrySet().fastIterator()
      while (iter.hasNext) {
        val entry = iter.next()
        val key = entry.getShortKey
        val otherNeighs = entry.getValue

        if (neighs.containsKey(key)) {
          val thisNeighs = neighs.get(key)
          val sorted = new Array[VertexId](thisNeighs.length + otherNeighs.length)

          var (left, right) = (0, 0)
          var idx = 0
          var last = if (thisNeighs.head < otherNeighs.head) thisNeighs.head -1 else otherNeighs.head -1
          while (left < thisNeighs.length && right < otherNeighs.length) {
            if (thisNeighs(left) <= otherNeighs(right)) {
              val candidate = thisNeighs(left)
              if (last != candidate) {
                sorted(idx) = candidate
                last = candidate
                idx += 1
              }

              left += 1
            } else { // thisNeighs(left) > otherNeighs(right)
              val candidate =otherNeighs(right)
              if (last != candidate) {
                sorted(idx) = candidate
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
              last = candidate
              idx += 1
            }

            left += 1
          }

          while(right < otherNeighs.length) {
            val candidate =otherNeighs(right)
            if (last != candidate) {
              sorted(idx) = candidate
              last = candidate
              idx += 1
            }

            right += 1
          }

          if (idx == sorted.length) {
            neighs.put(key, sorted)
          } else if (idx < sorted.length) {
            val deduplicate = new Array[VertexId](idx)
            Array.copy(sorted, 0, deduplicate, 0, idx)
            neighs.put(key, deduplicate)
          } else {
            throw new Exception("Error!")
          }
        } else {
          neighs.put(key, otherNeighs)
        }
      }
    }

    this
  }
}


class NeighTNBuilder extends TypedNeighborBuilder[NeighTN] {
  private var tpe: VertexType = defaultVertexType
  private val neighs = new Short2ObjectOpenHashMap[FastArray[VertexId]]()

  override def setTpe(tpe: VertexType): this.type = {
    this.tpe = tpe
    this
  }

  override def add(vt: VertexType, neigh: VertexId): this.type = {
    if (neighs.containsKey(vt)) {
      neighs.get(vt) += neigh
    } else {
      val temp = new FastArray[VertexId]()
      temp += neigh
      neighs.put(vt, temp)
    }
    this
  }

  override def add(neigh: NeighTN): this.type = {
    tpe = neigh.tpe
    val iter = neigh.neighs.short2ObjectEntrySet().fastIterator()
    while (iter.hasNext) {
      val entry = iter.next()
      val vt = entry.getShortKey
      val vtNeighs = entry.getValue

      if (neighs.containsKey(vt)) {
        vtNeighs.foreach { n => neighs.get(vt) += n }
      } else {
        val tempN = new FastArray[VertexId]()
        vtNeighs.foreach { n => tempN += n }
        neighs.put(vt, tempN)
      }
    }

    this
  }

  override def build: NeighTN = {
    val tempN = new Short2ObjectOpenHashMap[Array[VertexId]](neighs.size())

    val iter = neighs.short2ObjectEntrySet().fastIterator()
    while (iter.hasNext) {
      val entry = iter.next()
      val vt = entry.getShortKey
      val sorted = entry.getValue.trim().array.sorted

      var last = sorted.head - 1
      val deduplicate = sorted.filter { curr =>
        if (curr != last) {
          last = curr
          true
        } else {
          false
        }
      }

      tempN.put(vt, deduplicate)
    }

    NeighTN(tpe, tempN)
  }
}


object NeighTN {
  def builder(): NeighTNBuilder = {
    new NeighTNBuilder
  }
}