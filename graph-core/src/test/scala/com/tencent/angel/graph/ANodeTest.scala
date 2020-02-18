package com.tencent.angel.graph

import com.tencent.angel.graph.core.data.{ANode, GData, TypedNode, UnTypedNode}
import com.tencent.angel.graph.utils.ReflectUtils
import io.netty.buffer.{ByteBuf, Unpooled}
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random

class ANodeTest extends AnyFunSuite with BeforeAndAfterAll {
  private val directBuf: ByteBuf = Unpooled.buffer(2048)

  private var nodes: Array[ANode] = _

  override def beforeAll(): Unit = {
    val rand = new Random()

    val n1 = Array.tabulate[VertexId](5)(_ => rand.nextLong())
    val w1 = Array.tabulate[WgtTpe](5)(_ => rand.nextFloat())
    val n2 = Array.tabulate[VertexId](25)(_ => rand.nextLong())
    val w2 = Array.tabulate[WgtTpe](25)(_ => rand.nextFloat())
    val ns = new Int2ObjectOpenHashMap[Array[VertexId]](2)
    ns.put(1, n1)
    ns.put(2, n2)
    val ws = new Int2ObjectOpenHashMap[Array[WgtTpe]](2)
    ws.put(1, w1)
    ws.put(2, w2)

    val n = ANode(n1)
    val nw = ANode(n1, w1)
    val tn = ANode(1, ns)
    val tnw = ANode(2, ns, ws)

    nodes = Array(n, nw, tn, tnw)
  }

  test("serde") {
    nodes.foreach(node => node.serialize(directBuf))

    val deNodes = nodes.map{ node =>
      val newNode = ReflectUtils.newInstance(GData.getType(node)).asInstanceOf[ANode]
      newNode.deserialize(directBuf)
      newNode
    }


    println("OK")
  }

  test("sample"){
    nodes.foreach{
      case node: TypedNode =>
        println(node.sample(1))
        println(node.sample(1, 2).mkString("[", ",", "]"))
      case node: UnTypedNode =>
        println(node.sample())
        println(node.sample(3).mkString("[", ",", "]"))
    }
  }


}