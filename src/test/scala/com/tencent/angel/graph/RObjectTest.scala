package com.tencent.angel.graph

import com.tencent.angel.common.Serialize
import com.tencent.angel.graph.data.UnTypedNode
import com.tencent.angel.graph.utils.RObject
import io.netty.buffer.{ByteBuf, Unpooled}
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap
import org.scalatest.funsuite.AnyFunSuite

import scala.reflect.runtime.universe._

class RObjectTest extends AnyFunSuite{
  private val directBuf: ByteBuf = Unpooled.buffer(2048)

  test("robj") {
    val data = new RObject(typeOf[UnTypedNode])
    data.create()


  }
}
