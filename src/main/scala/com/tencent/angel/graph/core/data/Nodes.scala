package com.tencent.angel.graph.core.data

import java.io.{DataInputStream, DataOutputStream}
import java.util.concurrent.atomic.AtomicBoolean

import com.tencent.angel.graph.utils.{ReflectUtils, SerDe}
import com.tencent.angel.ml.math2.vector.Vector
import com.tencent.angel.ps.storage.vector.element.IElement
import io.netty.buffer.ByteBuf

import scala.collection.mutable.ListBuffer
import scala.reflect.runtime.{universe => ru}

sealed trait ANode extends IElement {
  def getNeighbors(args: Int*): Array[Int]

  def sampleOne(args: Int*): Int

  def sampleK(args: Int*): Array[Int]

  def hasNeighbor(args: Int*): Boolean

  override def serialize(byteBuf: ByteBuf): Unit = {
    SerDe.serialize(this, ANode.getFields(this), byteBuf)
  }

  override def deserialize(byteBuf: ByteBuf): Unit = {
    SerDe.serialize(this, ANode.getFields(this), byteBuf)
  }

  override def bufferLen(): Int = {
    SerDe.bufferLen(this, ANode.getFields(this))
  }

  override def serialize(dataOutputStream: DataOutputStream): Unit = ???

  override def deserialize(dataInputStream: DataInputStream): Unit = ???

  override def dataLen(): Int = ???

  override def deepClone(): AnyRef = ???
}

class NodeN(var neighs: Array[Int]) extends ANode {
  def this() = this(null)

  override def getNeighbors(args: Int*): Array[Int] = ???

  override def sampleOne(args: Int*): Int = ???

  override def sampleK(args: Int*): Array[Int] = ???

  override def hasNeighbor(args: Int*): Boolean = ???
}

class NodeNW(var neighs: Array[Int], var weights: Array[Float]) extends ANode {
  def this() = this(null, null)

  override def getNeighbors(args: Int*): Array[Int] = ???

  override def sampleOne(args: Int*): Int = ???

  override def sampleK(args: Int*): Array[Int] = ???

  override def hasNeighbor(args: Int*): Boolean = ???
}

class NodeNA(var neighs: Array[Int], var attrs: Vector) extends ANode {
  def this() = this(null, null)

  override def getNeighbors(args: Int*): Array[Int] = ???

  override def sampleOne(args: Int*): Int = ???

  override def sampleK(args: Int*): Array[Int] = ???

  override def hasNeighbor(args: Int*): Boolean = ???
}

class NodeNAW(var neighs: Array[Int], var weights: Array[Float], var attrs: Vector)
  extends ANode {
  def this() = this(null, null, null)

  override def getNeighbors(args: Int*): Array[Int] = ???

  override def sampleOne(args: Int*): Int = ???

  override def sampleK(args: Int*): Array[Int] = ???

  override def hasNeighbor(args: Int*): Boolean = ???
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

  def apply(neighs: Array[Int]): ANode = new NodeN(neighs)

  def apply(neighs: Array[Int], weights: Array[Float]): ANode = new NodeNW(neighs, weights)

  def apply(neighs: Array[Int], attrs: Vector): ANode = new NodeNA(neighs, attrs)

  def apply(neighs: Array[Int], weights: Array[Float], attrs: Vector): ANode = new NodeNAW(neighs, weights, attrs)

  def unapply(arg: NodeN): Option[Array[Int]] = {
    if (arg != null) {
      Some(arg.neighs)
    } else {
      None
    }
  }

  def unapply(arg: NodeNW): Option[(Array[Int], Array[Float])] = {
    if (arg != null) {
      Some((arg.neighs, arg.weights))
    } else {
      None
    }
  }

  def unapply(arg: NodeNA): Option[(Array[Int], Vector)] = {
    if (arg != null) {
      Some((arg.neighs, arg.attrs))
    } else {
      None
    }
  }

  def unapply(arg: NodeNAW): Option[(Array[Int], Array[Float], Vector)] = {
    if (arg != null) {
      Some((arg.neighs, arg.weights, arg.attrs))
    } else {
      None
    }
  }
}
