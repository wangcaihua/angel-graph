package com.tencent.angel.graph.core.data

import java.io.{DataInputStream, DataOutputStream}

import com.tencent.angel.graph.core.sampler._
import com.tencent.angel.graph.{VertexId, WgtTpe}
import com.tencent.angel.ps.storage.vector.element.IElement
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap


trait ANode extends IElement with GData {

  override def serialize(dataOutputStream: DataOutputStream): Unit = ???

  override def deserialize(dataInputStream: DataInputStream): Unit = ???

  override def dataLen(): Int = ???

  override def deepClone(): AnyRef = ???
}

trait UnTypedNode {
  def sample(): VertexId

  def sample(k: Int): Array[VertexId]

  def hasNeighbor(neigh: VertexId): Boolean = ???
}

trait TypedNode {
  def sample(tpe: Int): VertexId

  def sample(tpe: Int, k: Int): Array[VertexId]

  def hasNeighbor(tpe: Int, neigh: VertexId): Boolean = ???
}

// 1. untyped node
case class NodeN(neighs: Array[VertexId]) extends ANode with UnTypedNode {
  def this() = this(null)

  @transient private lazy val sample1: SampleOne = new Simple(neighs)
  @transient private lazy val samplek: SampleK = new Reservoir(neighs)

  override def sample(): VertexId = sample1.sample()

  override def sample(k: Int): Array[VertexId] = samplek.sample(k)
}


case class NodeNW(neighs: Array[VertexId], weights: Array[WgtTpe])
  extends ANode with UnTypedNode {
  def this() = this(null, null)

  @transient private lazy val sample1: SampleOne = new BetWheel(neighs, weights)
  @transient private lazy val samplek: SampleK = new ARes(neighs, weights)

  override def sample(): VertexId = sample1.sample()

  override def sample(k: Int): Array[VertexId] = samplek.sample(k)

}


// 2. typed node
case class NodeTN(tpe: Int, neighs: Int2ObjectOpenHashMap[Array[VertexId]])
  extends ANode with TypedNode {
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
  extends ANode with TypedNode {
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


object ANode {
  def apply(neighs: Array[VertexId]): ANode = NodeN(neighs)

  def apply(neighs: Array[VertexId], weights: Array[WgtTpe]): ANode = NodeNW(neighs, weights)

  def apply(tpe: Int, neighs: Int2ObjectOpenHashMap[Array[VertexId]]): ANode = NodeTN(tpe, neighs)

  def apply(tpe: Int, neighs: Int2ObjectOpenHashMap[Array[VertexId]],
            weights: Int2ObjectOpenHashMap[Array[WgtTpe]]): ANode = NodeTNW(tpe, neighs, weights)


  def unapply(arg: NodeN): Option[Array[VertexId]] = {
    if (arg != null) {
      Some(arg.neighs)
    } else {
      None
    }
  }

  def unapply(arg: NodeNW): Option[(Array[VertexId], Array[WgtTpe])] = {
    if (arg != null) {
      Some((arg.neighs, arg.weights))
    } else {
      None
    }
  }

  def unapply(arg: NodeTN): Option[(Int, Int2ObjectOpenHashMap[Array[VertexId]])] = {
    if (arg != null) {
      Some((arg.tpe, arg.neighs))
    } else {
      None
    }
  }

  def unapply(arg: NodeTNW): Option[(Int, Int2ObjectOpenHashMap[Array[VertexId]],
    Int2ObjectOpenHashMap[Array[WgtTpe]])] = {
    if (arg != null) {
      Some((arg.tpe, arg.neighs, arg.weights))
    } else {
      None
    }
  }
}
