package com.tencent.angel.graph.core.data

import com.tencent.angel.common.Serialize
import com.tencent.angel.graph.utils.{ReflectUtils, SerDe}
import io.netty.buffer.ByteBuf

import scala.reflect.runtime.universe._


trait GData extends Serialize {
  def serialize(byteBuf: ByteBuf): Unit = {
    SerDe.serialize(this, GData.getFields(this), byteBuf)
  }

  def deserialize(byteBuf: ByteBuf): Unit = {
    SerDe.deserialize(this, GData.getFields(this), byteBuf)
  }

  def bufferLen(): Int = {
    SerDe.bufferLen(this, GData.getFields(this))
  }
}

object GData {
  private val fields = new scala.collection.mutable.HashMap[String, List[TermSymbol]]()
  private val types = new scala.collection.mutable.HashMap[String, Type]()

  def getType(node: GData): Type = types.synchronized {
    val name = node.getClass.getCanonicalName

    if (types.contains(name)) {
      types(name)
    } else {
      val tpe = ReflectUtils.typeFromObject(node)
      types.put(name, tpe)

      tpe
    }
  }

  def getFields(node: GData): List[TermSymbol] = fields.synchronized {
    val name = node.getClass.getCanonicalName

    if (fields.contains(name)) {
      fields(name)
    } else {
      val field = ReflectUtils.getFields(ReflectUtils.typeFromObject(node))
      fields.put(name, field)

      field
    }
  }
}
