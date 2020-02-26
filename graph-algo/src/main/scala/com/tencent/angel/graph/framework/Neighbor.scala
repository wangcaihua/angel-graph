package com.tencent.angel.graph.framework


import com.tencent.angel.graph.core.data.GData
import com.tencent.angel.graph.core.sampler._
import com.tencent.angel.graph.framework.EdgeDirection.EdgeDirection
import com.tencent.angel.graph.utils.{FastArray, FastHashMap}
import com.tencent.angel.graph.{VertexId, WgtTpe}
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap

import scala.reflect.ClassTag

trait Neighbor extends GData

trait UnTyped {
  def sample(): VertexId

  def sample(k: Int): Array[VertexId]

  def hasNeighbor(neigh: VertexId): Boolean = ???
}

trait Typed {
  def sample(tpe: Int): VertexId

  def sample(tpe: Int, k: Int): Array[VertexId]

  def hasNeighbor(tpe: Int, neigh: VertexId): Boolean = ???
}

// 1. untyped node
case class NeighN(neighs: Array[VertexId]) extends Neighbor with UnTyped {
  def this() = this(null)

  @transient private lazy val sample1: SampleOne = new Simple(neighs)
  @transient private lazy val samplek: SampleK = new Reservoir(neighs)

  override def sample(): VertexId = sample1.sample()

  override def sample(k: Int): Array[VertexId] = samplek.sample(k)
}

case class NeighNW(neighs: Array[VertexId], weights: Array[WgtTpe])
  extends Neighbor with UnTyped {
  def this() = this(null, null)

  @transient private lazy val sample1: SampleOne = new BetWheel(neighs, weights)
  @transient private lazy val samplek: SampleK = new ARes(neighs, weights)

  override def sample(): VertexId = sample1.sample()

  override def sample(k: Int): Array[VertexId] = samplek.sample(k)
}

class UnTypedNeighborBuilder {
  private val neighs = new FastArray[VertexId]()
  private val weights = new FastArray[WgtTpe]()

  def add(neigh: VertexId): this.type = {
    neighs += neigh
    this
  }

  def add(neigh: VertexId, weight: WgtTpe): this.type = {
    neighs += neigh
    weights += weight
    this
  }

  def add(neigh: NeighN): this.type = {
    neigh.neighs.foreach(n => neighs += n)
    this
  }

  def add(neigh: NeighNW): this.type = {
    neigh.neighs.foreach(n => neighs += n)
    neigh.weights.foreach(w => weights += w)
    this
  }

  def build[N: ClassTag]: N = {
    implicitly[ClassTag[N]].runtimeClass match {
      case nt if nt == classOf[NeighNW] =>
        assert(neighs.size == weights.size)
        NeighNW(neighs.trim().array, weights.trim().array)
          .asInstanceOf[N]
      case _ =>
        NeighN(neighs.trim().array)
          .asInstanceOf[N]
    }
  }

  def clearAndBuild[N: ClassTag](key: VertexId): N = {
    // remove duplicate && remove key
    implicitly[ClassTag[N]].runtimeClass match {
      case nt if nt == classOf[NeighNW] =>
        assert(neighs.size == weights.size)
        val sorted = neighs.trim().array.zip(weights.trim().array).sortBy{ case (n, _) => n }
        val tempN = new FastArray[VertexId]
        val tempW = new FastArray[WgtTpe]

        var last = sorted.head._1
        tempN += last
        tempW += sorted.head._2

        sorted.foreach{ case (vid, w) =>
          if (vid != key && vid != last) {
            tempN += vid
            tempW += w
            last = vid
          }
        }

        NeighNW(tempN.trim().array, tempW.trim().array)
          .asInstanceOf[N]
      case _ =>
        val sorted = neighs.trim().array.sorted
        val tempN = new FastArray[VertexId]

        var last = sorted.head
        tempN += last
        sorted.foreach{ vid =>
          if (vid != key && vid != last) {
            tempN += vid
            last = vid
          }
        }
        NeighN(tempN.trim().array).asInstanceOf[N]
    }
  }
}

class PartitionUnTypedNeighborBuilder[N: ClassTag]
(direction: EdgeDirection, private val neighTable: FastHashMap[VertexId, UnTypedNeighborBuilder]) {
  def this(direction: EdgeDirection) = {
    this(direction, new FastHashMap[VertexId, UnTypedNeighborBuilder]())
  }

  def add(src: VertexId, dst: VertexId): this.type = {
    direction match {
      case EdgeDirection.Both =>
        neighTable(src).add(dst)
        neighTable(dst).add(src)
      case _ => neighTable(src).add(dst)
    }
    this
  }

  def add(src: VertexId, neigh: NeighN): this.type = {
    neighTable(src).add(neigh)
    this
  }

  def add(src: VertexId, dst: VertexId, weight: WgtTpe): this.type = {
    direction match {
      case EdgeDirection.Both =>
        neighTable(src).add(dst)
        neighTable(dst).add(src)
      case _ => neighTable(src).add(dst)
    }
    this
  }

  def add(src: VertexId, neigh: NeighNW): this.type = {
    neighTable(src).add(neigh)
    this
  }

  def build[T]: T = {
    neighTable.mapValues[N](value => value.build[N]).toUnimi[T]
  }
}

// 2. typed node
case class NodeTN(tpe: Int, neighs: Int2ObjectOpenHashMap[Array[VertexId]])
  extends Neighbor with Typed {
  def this() = this(-1, null)

  @transient private lazy val sample1: Int2ObjectOpenHashMap[SampleOne] =
    new Int2ObjectOpenHashMap[SampleOne](neighs.size())
  @transient private lazy val samplek: Int2ObjectOpenHashMap[SampleK] =
    new Int2ObjectOpenHashMap[SampleK](neighs.size())

  override def sample(tpe: Int): VertexId = {
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

  override def sample(tpe: Int, k: Int): Array[VertexId] = {
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
}


case class NodeTNW(tpe: Int, neighs: Int2ObjectOpenHashMap[Array[VertexId]],
                   weights: Int2ObjectOpenHashMap[Array[WgtTpe]])
  extends Neighbor with Typed {
  def this() = this(-1, null, null)

  @transient private lazy val sample1: Int2ObjectOpenHashMap[SampleOne] =
    new Int2ObjectOpenHashMap[SampleOne](neighs.size())
  @transient private lazy val samplek: Int2ObjectOpenHashMap[SampleK] =
    new Int2ObjectOpenHashMap[SampleK](neighs.size())

  override def sample(tpe: Int): VertexId = {
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

  override def sample(tpe: Int, k: Int): Array[VertexId] = {
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
}
