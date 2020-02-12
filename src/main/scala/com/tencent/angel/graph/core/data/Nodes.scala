package com.tencent.angel.graph.core.data

import java.io.{DataInputStream, DataOutputStream}
import java.util.concurrent.atomic.AtomicBoolean

import com.tencent.angel.graph.core.sampler._
import com.tencent.angel.graph.{VertexId, WgtTpe}
import com.tencent.angel.graph.utils.{ReflectUtils, SerDe}
import com.tencent.angel.ml.math2.vector.Vector
import com.tencent.angel.ps.storage.vector.element.IElement
import io.netty.buffer.ByteBuf
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap

import scala.reflect.runtime.{universe => ru}

sealed trait ANode extends IElement {

  override def serialize(byteBuf: ByteBuf): Unit = {
    SerDe.serialize(this, ANode.getFields(this), byteBuf)
  }

  override def deserialize(byteBuf: ByteBuf): Unit = {
    SerDe.deserialize(this, ANode.getFields(this), byteBuf)
  }

  override def bufferLen(): Int = {
    SerDe.bufferLen(this, ANode.getFields(this))
  }

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
class NodeN(val neighs: Array[VertexId]) extends ANode with UnTypedNode {
  def this() = this(null)

  @transient private lazy val sample1: SampleOne = new Simple(neighs)
  @transient private lazy val samplek: SampleK = new Reservoir(neighs)

  override def sample(): VertexId = sample1.sample()

  override def sample(k: Int): Array[VertexId] = samplek.sample(k)
}

class NodeNA(val neighs: Array[VertexId], val attrs: Vector) extends ANode with UnTypedNode {
  def this() = this(null, null)

  @transient private lazy val sample1: SampleOne = new Simple(neighs)
  @transient private lazy val samplek: SampleK = new Reservoir(neighs)

  override def sample(): VertexId = sample1.sample()

  override def sample(k: Int): Array[VertexId] = samplek.sample(k)
}

class NodeNW(val neighs: Array[VertexId], val weights: Array[WgtTpe])
  extends ANode with UnTypedNode {
  def this() = this(null, null)

  @transient private lazy val sample1: SampleOne = new BetWheel(neighs, weights)
  @transient private lazy val samplek: SampleK = new ARes(neighs, weights)

  override def sample(): VertexId = sample1.sample()

  override def sample(k: Int): Array[VertexId] = samplek.sample(k)

}

class NodeNWA(val neighs: Array[VertexId], val weights: Array[WgtTpe], val attrs: Vector)
  extends ANode with UnTypedNode {
  def this() = this(null, null, null)

  @transient private lazy val sample1: SampleOne = new BetWheel(neighs, weights)
  @transient private lazy val samplek: SampleK = new ARes(neighs, weights)

  override def sample(): VertexId = sample1.sample()

  override def sample(k: Int): Array[VertexId] = samplek.sample(k)
}

// 2. typed node
class NodeTN(val tpe: Int, val neighs: Int2ObjectOpenHashMap[Array[VertexId]])
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

class NodeTNA(val tpe: Int, val neighs: Int2ObjectOpenHashMap[Array[VertexId]], val attrs: Vector)
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

class NodeTNW(val tpe: Int, val neighs: Int2ObjectOpenHashMap[Array[VertexId]],
              val weights: Int2ObjectOpenHashMap[Array[WgtTpe]])
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

class NodeTNWA(val tpe: Int, val neighs: Int2ObjectOpenHashMap[Array[VertexId]],
               val weights: Int2ObjectOpenHashMap[Array[WgtTpe]], val attrs: Vector)
  extends ANode with TypedNode {
  def this() = this(-1, null, null, null)

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
  private val fields = new scala.collection.mutable.HashMap[String, List[ru.TermSymbol]]()
  private val types = new scala.collection.mutable.HashMap[String, ru.Type]()
  private val isInited = new AtomicBoolean(false)

  private def init(): Unit = {
    val tpe = ru.typeOf[ANode]
    val clazz = tpe.typeSymbol.asClass
    clazz.knownDirectSubclasses.foreach {
      case clz: ru.ClassSymbol =>
        val cType = clz.toType
        types.put(clz.fullName, cType)
        fields.put(clz.fullName, ReflectUtils.getFields(cType))
      case _ =>
    }
  }

  def getType(node: ANode): ru.Type = {
    while (!isInited.get) {
      init()
      isInited.set(true)
    }
    val name = node.getClass.getCanonicalName
    types.getOrElse(name, null.asInstanceOf[ru.Type])
  }

  def getFields(node: ANode): List[ru.TermSymbol] = {
    while (!isInited.get) {
      init()
      isInited.set(true)
    }

    val name = node.getClass.getCanonicalName
    fields.getOrElse(name, null.asInstanceOf[List[ru.TermSymbol]])
  }

  def apply(neighs: Array[VertexId]): ANode = new NodeN(neighs)

  def apply(neighs: Array[VertexId], weights: Array[WgtTpe]): ANode = new NodeNW(neighs, weights)

  def apply(neighs: Array[VertexId], attrs: Vector): ANode = new NodeNA(neighs, attrs)

  def apply(neighs: Array[VertexId], weights: Array[WgtTpe], attrs: Vector): ANode = new NodeNWA(neighs, weights, attrs)

  def apply(tpe: Int, neighs: Int2ObjectOpenHashMap[Array[VertexId]]): ANode = new NodeTN(tpe, neighs)

  def apply(tpe: Int, neighs: Int2ObjectOpenHashMap[Array[VertexId]],
            weights: Int2ObjectOpenHashMap[Array[WgtTpe]]): ANode = new NodeTNW(tpe, neighs, weights)

  def apply(tpe: Int, neighs: Int2ObjectOpenHashMap[Array[VertexId]],
            attrs: Vector): ANode = new NodeTNA(tpe, neighs, attrs)

  def apply(tpe: Int, neighs: Int2ObjectOpenHashMap[Array[VertexId]],
            weights: Int2ObjectOpenHashMap[Array[WgtTpe]],
            attrs: Vector): ANode = new NodeTNWA(tpe, neighs, weights, attrs)

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

  def unapply(arg: NodeNA): Option[(Array[VertexId], Vector)] = {
    if (arg != null) {
      Some((arg.neighs, arg.attrs))
    } else {
      None
    }
  }

  def unapply(arg: NodeNWA): Option[(Array[VertexId], Array[WgtTpe], Vector)] = {
    if (arg != null) {
      Some((arg.neighs, arg.weights, arg.attrs))
    } else {
      None
    }
  }

  def unapply(arg: NodeTN): Option[Int2ObjectOpenHashMap[Array[VertexId]]] = {
    if (arg != null) {
      Some(arg.neighs)
    } else {
      None
    }
  }

  def unapply(arg: NodeTNW): Option[(Int2ObjectOpenHashMap[Array[VertexId]], Int2ObjectOpenHashMap[Array[WgtTpe]])] = {
    if (arg != null) {
      Some((arg.neighs, arg.weights))
    } else {
      None
    }
  }

  def unapply(arg: NodeTNA): Option[(Int2ObjectOpenHashMap[Array[VertexId]], Vector)] = {
    if (arg != null) {
      Some((arg.neighs, arg.attrs))
    } else {
      None
    }
  }

  def unapply(arg: NodeTNWA): Option[(Int2ObjectOpenHashMap[Array[VertexId]], Int2ObjectOpenHashMap[Array[WgtTpe]], Vector)] = {
    if (arg != null) {
      Some((arg.neighs, arg.weights, arg.attrs))
    } else {
      None
    }
  }
}
