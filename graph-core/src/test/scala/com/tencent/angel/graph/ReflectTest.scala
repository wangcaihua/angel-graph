package com.tencent.angel.graph


import com.tencent.angel.graph.core.data.{NeighN, NeighNW, NeighTN, NeighTNW}
import com.tencent.angel.graph.utils.{FastHashMap, ReflectUtils}
import com.tencent.angel.ml.math2.vector._
import io.netty.buffer.{ByteBuf, Unpooled}
import it.unimi.dsi.fastutil.ints._
import it.unimi.dsi.fastutil.longs._
import org.scalatest.funsuite.AnyFunSuite

import scala.reflect.runtime.universe._
import scala.util.Random


class ReflectTest extends AnyFunSuite {
  val directBuf: ByteBuf = Unpooled.buffer(2048)
  val rand = new Random()

  def getTypeTag[T: TypeTag](obj: T): TypeTag[T] = {
    implicitly[TypeTag[T]]
  }

  test("vector") {

    println(ReflectUtils.typeFromString(typeOf[VertexId].toString))
    println(ReflectUtils.typeFromString(typeOf[Int].toString))
    println(ReflectUtils.typeFromString(typeOf[IntDoubleVector].toString))
    println(ReflectUtils.typeFromString(typeOf[Int2LongOpenHashMap].toString))
    println(ReflectUtils.typeFromString(typeOf[NeighN].toString))
    println(ReflectUtils.typeFromString(typeOf[Array[VertexId]].toString))
    println(ReflectUtils.typeFromString(typeOf[Array[Int]].toString))
    println(ReflectUtils.typeFromString(typeOf[Array[IntDoubleVector]].toString))
    println(ReflectUtils.typeFromString(typeOf[Array[Int2LongOpenHashMap]].toString))
    println(ReflectUtils.typeFromString(typeOf[Array[NeighN]].toString))

    println(ReflectUtils.typeFromString(typeOf[FastHashMap[VertexId, Int]].toString))
    println(ReflectUtils.typeFromString(typeOf[FastHashMap[VertexId, Int]].toString))



  }
}
