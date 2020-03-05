package com.tencent.angel.graph.core.data

import com.tencent.angel.graph.core.sampler._
import com.tencent.angel.graph.{VertexId, VertexType, WgtTpe}
import it.unimi.dsi.fastutil.shorts.Short2ObjectOpenHashMap

trait Neighbor extends GData

trait UnTyped {
  def sample(): VertexId

  def sample(k: Int): Array[VertexId]

  def hasNeighbor(neigh: VertexId): Boolean = ???
}

trait Typed {
  def sample(tpe: VertexType): VertexId

  def sample(tpe: VertexType, k: Int): Array[VertexId]

  def hasNeighbor(tpe: VertexType, neigh: VertexId): Boolean = ???
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

// 2. typed node
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
}

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
}

