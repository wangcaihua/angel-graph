package com.tencent.angel.graph.core.data

import java.io.{DataInputStream, DataOutputStream}
import java.util.concurrent.atomic.AtomicBoolean

import com.tencent.angel.graph.utils.{ReflectUtils, SerDe}
import com.tencent.angel.ml.math2.vector.Vector
import com.tencent.angel.ps.storage.vector.element.IElement
import io.netty.buffer.ByteBuf

import scala.collection.mutable.ListBuffer
import scala.reflect.runtime.{universe => ru}

sealed trait AEdge extends IElement {

}

object AEdge {

}