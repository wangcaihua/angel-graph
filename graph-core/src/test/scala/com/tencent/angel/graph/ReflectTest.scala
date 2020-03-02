package com.tencent.angel.graph


import com.tencent.angel.ml.math2.VFactory
import io.netty.buffer.{ByteBuf, Unpooled}
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
    println("hello world!")
  }
}
