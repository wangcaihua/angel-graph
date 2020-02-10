package com.tencent.angel.graph

import com.tencent.angel.graph.core.data.ANode
import io.netty.buffer.{ByteBuf, Unpooled}
import org.scalatest.funsuite.AnyFunSuite

class ANodeTest extends AnyFunSuite{
  private val directBuf: ByteBuf = Unpooled.buffer(2048)

  test("field") {
    val node = ANode(Array(1, 2, 3, 4))
    val myField = ANode.getFields(node)
    println("OK")
  }
}