package com.tencent.angel.graph

import org.scalatest.funsuite.AnyFunSuite

class ReflectTest extends AnyFunSuite {
  test("getType") {
    val builder = new EdgeAttributeBuilder()

    builder.putSrcType(1)
    builder.putDstType(32134)
    builder.putWeight(3.6647554f)

    val code = builder.build

    println(code)

    val attr = new EdgeAttribute(code)

    println(attr.srcType)
    println(attr.dstType)
    println(attr.weight)

    builder.putSrcType(12)
    builder.putDstType(134)
    builder.putWeight(9.6647554f)

    val code2 = builder.build
    val attr2 = new EdgeAttribute(code2)

    println(attr2.srcType)
    println(attr2.dstType)
    println(attr2.weight)
  }

}
