package com.tencent.angel.graph

import com.tencent.angel.graph.core.data.GData
import com.tencent.angel.graph.utils.FastHashMap
import io.netty.buffer.{ByteBuf, Unpooled}
import org.scalatest.funsuite.AnyFunSuite

import scala.reflect.runtime.universe._



class ReflectTest extends AnyFunSuite {
  val directBuf: ByteBuf = Unpooled.buffer(2048)

  def getTypeTag[T: TypeTag](obj: T): TypeTag[T] = {
    implicitly[TypeTag[T]]
  }

  test("getType") {
    println()
  }

}
