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
    val inst = ReflectUtils.instMirror(this)

    ANode.getFields(this).foreach { field =>
      field.typeSignature match {
        case tpe if ReflectUtils.isPrimitive(tpe) =>
          SerDe.serPrimitive(ReflectUtils.field(inst, field).get, byteBuf)
        case tpe if tpe.weak_<:<(ru.typeOf[ANode]) =>
          ReflectUtils.field(inst, field).get.asInstanceOf[ANode].serialize(byteBuf)
        case tpe if ReflectUtils.isPrimitiveArray(tpe) =>
          SerDe.serArr(tpe.typeArgs.head, ReflectUtils.field(inst, field).get, byteBuf)
        case tpe if ReflectUtils.isFastMap(tpe) =>
          SerDe.serFastMap(ReflectUtils.field(inst, field).get, byteBuf)
        case tpe if ReflectUtils.isVector(tpe) =>
          SerDe.serVector(ReflectUtils.field(inst, field).get.asInstanceOf[Vector], byteBuf)
        case tpe =>
          throw new Exception(s"cannot serialize field ${tpe.toString}")
      }
    }
  }

  override def deserialize(byteBuf: ByteBuf): Unit = {
    val inst = ReflectUtils.instMirror(this)

    ANode.getFields(this).foreach { field =>
      field.typeSignature match {
        case tpe if ReflectUtils.isPrimitive(tpe) =>
          ReflectUtils.field(inst, field)
            .set(field, SerDe.primitiveFromBuffer(tpe, byteBuf))
        case tpe if tpe.weak_<:<(ru.typeOf[ANode]) =>
          val node = ReflectUtils.newInstance(tpe).asInstanceOf[ANode]
          node.deserialize(byteBuf)
          ReflectUtils.field(inst, field)
            .set(field, node)
        case tpe if ReflectUtils.isPrimitiveArray(tpe) =>
          ReflectUtils.field(inst, field)
            .set(field, SerDe.arrFromBuffer(tpe.typeArgs.head, byteBuf))
        case tpe if ReflectUtils.isFastMap(tpe) =>
          ReflectUtils.field(inst, field)
            .set(field, SerDe.fastMapFromBuffer(tpe, byteBuf))
        case tpe if ReflectUtils.isVector(tpe) =>
          ReflectUtils.field(inst, field)
            .set(field, SerDe.vectorFromBuffer(tpe, byteBuf))
        case tpe =>
          throw new Exception(s"cannot serialize field ${tpe.toString}")
      }
    }
  }

  override def bufferLen(): Int = {
    val inst = ReflectUtils.instMirror(this)
    var len = 0

    ANode.getFields(this).foreach { field =>
      field.typeSignature match {
        case tpe if ReflectUtils.isPrimitive(tpe) =>
          len += SerDe.serPrimitiveBufSize(ReflectUtils.field(inst, field).get)
        case tpe if tpe.weak_<:<(ru.typeOf[ANode]) =>
          len += ReflectUtils.field(inst, field).get.asInstanceOf[ANode].bufferLen()
        case tpe if ReflectUtils.isPrimitiveArray(tpe) =>
          len += SerDe.serArrBufSize(tpe.typeArgs.head, ReflectUtils.field(inst, field).get)
        case tpe if ReflectUtils.isFastMap(tpe) =>
          len += SerDe.serFastMapBufSize(ReflectUtils.field(inst, field).get)
        case tpe if ReflectUtils.isVector(tpe) =>
          len += SerDe.serVectorBufSize(ReflectUtils.field(inst, field).get.asInstanceOf[Vector])
        case tpe =>
          throw new Exception(s"cannot serialize field ${tpe.toString}")
      }
    }

    len
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
        val buf = new ListBuffer[ru.TermSymbol]()
        cType.members.foreach {
          case field: ru.TermSymbol if !field.isMethod && (field.isVal || field.isVal) =>
            val annotations = field.annotations
            if (annotations.isEmpty) {
              buf.append(field)
            } else {
              val trans = annotations.forall { ann => !ann.toString.equalsIgnoreCase("transient") }
              if (trans) {
                buf.append(field)
              }
            }
          case _ =>
        }

        types.put(clz.fullName, cType)
        fields.put(clz.fullName, buf.toList)
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
