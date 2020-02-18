package com.tencent.angel.graph

import com.tencent.angel.graph.core.data._
import com.tencent.angel.graph.utils.ReflectUtils
import it.unimi.dsi.fastutil.ints._
import it.unimi.dsi.fastutil.longs._
import org.scalatest.funsuite.AnyFunSuite

import scala.reflect.runtime.universe._

class ReflectTest extends AnyFunSuite {
  test("getType") {
    println(s"${typeOf[Array[Int]].toString}")
    println(s"${typeOf[Array[String]].toString}")
    println(s"${typeOf[Int2LongOpenHashMap].toString}")
    println(s"${typeOf[Int2ObjectOpenHashMap[Int]].toString}")
    println(s"${typeOf[Int2ObjectOpenHashMap[Array[Int]]].toString}")
    println(s"${typeOf[Long2ObjectOpenHashMap[Array[String]]].toString}")
    println(s"${typeOf[Int2ObjectOpenHashMap[Array[ANode]]].toString}")

    /*
    val outerPrefix = "it.unimi.dsi.fastutil"
    val innerPrefix = "com.tencent.angel.graph.core.data"
    val PAT1 = s"$outerPrefix.\\w{3,4}s.(\\w+)\\[Array\\[$innerPrefix.(\\w+)\\]\\]".r
    val PAT2 = s"$outerPrefix.\\w{3,4}s.(\\w+)\\[Array\\[(\\w+)\\]\\]".r

    println()
    println()
    println()
    s"${typeOf[Int2ObjectOpenHashMap[Array[Int]]].toString}" match {
      case PAT1(map: String, ele: String) => println(s"$map[Array[$ele]]")
      case PAT2(map: String, ele: String) => println(s"$map[Array[$ele]]")
      case str => println(str)
    }
     */

    val tpeStr = s"${typeOf[Int2ObjectOpenHashMap[Array[NodeN]]].toString}"
    println()
    println(tpeStr)
    val tpe = ReflectUtils.typeFromString(tpeStr)

    println(tpe.toString)
  }

  test("reflect") {
    val abc1 = Array(1, 2, 3, 4)
    val abc2 = Array(ANode(Array(1L, 2L, 3L, 4L)))
    val abc3 = Array(ANode(Array(1L, 2L, 3L, 4L), Array(1.2f, 2, 3f, 4.3f, 4.6f)))
    val cde = new Long2ObjectOpenHashMap[Array[Int]](10)
    val dde = new Long2ObjectOpenHashMap[Array[NodeN]](10)
    val fgh = new Long2ObjectOpenHashMap[ANode](10)
    val ijk = new Long2ObjectOpenHashMap[NodeN](10)

    println(ReflectUtils.typeFromString(ReflectUtils.getType(abc1).toString))
    println(ReflectUtils.typeFromString(ReflectUtils.getType(abc2).toString))
    println(ReflectUtils.typeFromString(ReflectUtils.getType(abc3).toString))
    println(ReflectUtils.typeFromString(ReflectUtils.getType(cde).toString))
    println(ReflectUtils.typeFromString(ReflectUtils.getType(dde).toString))
    println(ReflectUtils.typeFromString(ReflectUtils.getType(fgh).toString))
    println(ReflectUtils.typeFromString(ReflectUtils.getType(ijk).toString))



    //    println(abc.getClass.getCanonicalName)
    //    val tpe1 = ReflectUtils.getType(abc1)
    //    val tpe2 = ReflectUtils.getReflectType(abc1)
    //    println(tpe1.toString)
    //    println(tpe2.toString)
  }
}
