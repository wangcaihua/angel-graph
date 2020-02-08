package com.tencent.angel.graph

import com.tencent.angel.graph.data.UnTypedNode
import com.tencent.angel.graph.utils.SerDe
import io.netty.buffer.{ByteBuf, Unpooled}
import it.unimi.dsi.fastutil.ints._
import it.unimi.dsi.fastutil.longs._
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random


class SerDeTest extends AnyFunSuite {
  val directBuf: ByteBuf = Unpooled.buffer(2048)

  test("array") {
    val abo = Array[Boolean](true, false)
    val aby = "abc".getBytes
    val ac = "bcd".toCharArray
    val as = Array[Short](1, 2, 34)
    val ai = Array[Int](5, 3, 35)
    val al = Array[Long](8L, 2L, 74L)
    val af = Array[Float](1.0f, 2.2f, 3.4f)
    val ad = Array[Double](1.2, 2.5, 3.4)
    val astr = Array[String]("abx", "drf")
    val aser = Array[UnTypedNode](new UnTypedNode, new UnTypedNode)

    SerDe.serArr(abo, directBuf)
    SerDe.serArr(aby, directBuf)
    SerDe.serArr(ac, directBuf)
    SerDe.serArr(as, directBuf)
    SerDe.serArr(ai, directBuf)
    SerDe.serArr(al, directBuf)
    SerDe.serArr(af, directBuf)
    SerDe.serArr(ad, directBuf)
    SerDe.serArr(astr, directBuf)
    SerDe.serArr(aser, directBuf)

    val abo_ = SerDe.arrFromBuffer[Boolean](directBuf)
    val aby_ = SerDe.arrFromBuffer[Byte](directBuf)
    val ac_ = SerDe.arrFromBuffer[Char](directBuf)
    val as_ = SerDe.arrFromBuffer[Short](directBuf)
    val ai_ = SerDe.arrFromBuffer[Int](directBuf)
    val al_ = SerDe.arrFromBuffer[Long](directBuf)
    val af_ = SerDe.arrFromBuffer[Float](directBuf)
    val ad_ = SerDe.arrFromBuffer[Double](directBuf)
    val astr_ = SerDe.arrFromBuffer[String](directBuf)
    val aser_ = SerDe.arrFromBuffer[UnTypedNode](directBuf)

    println("OK")
  }

  test("map") {
    // 1. create data
    val i2i = new Int2IntOpenHashMap(10)
    val i2l = new Int2LongOpenHashMap(10)
    val i2f = new Int2FloatOpenHashMap(10)
    val i2d = new Int2DoubleOpenHashMap(10)
    val i2o = new Int2ObjectOpenHashMap[Array[Long]](10)
    val i2g = new Int2ObjectOpenHashMap[UnTypedNode](10)

    val rand = new Random()
    (0 until rand.nextInt(10)).foreach { _ =>
      val key = rand.nextInt()
      i2i.put(key, rand.nextInt())
      i2l.put(key, rand.nextLong())
      i2f.put(key, rand.nextFloat())
      i2d.put(key, rand.nextDouble())
      i2o.put(key, Array.tabulate[Long](5)(_ => rand.nextLong()))
      i2g.put(key, new UnTypedNode())
    }

    val l2i = new Long2IntOpenHashMap(10)
    val l2l = new Long2LongOpenHashMap(10)
    val l2f = new Long2FloatOpenHashMap(10)
    val l2d = new Long2DoubleOpenHashMap(10)
    val l2o = new Long2ObjectOpenHashMap[Array[Double]](10)
    val l2g = new Long2ObjectOpenHashMap[UnTypedNode](10)

    (0 until rand.nextInt(10)).foreach { _ =>
      val key = rand.nextLong()
      l2i.put(key, rand.nextInt())
      l2l.put(key, rand.nextLong())
      l2f.put(key, rand.nextFloat())
      l2d.put(key, rand.nextDouble())
      l2o.put(key, Array.tabulate[Double](5)(_ => rand.nextLong()))
      l2g.put(key, new UnTypedNode())
    }

    // 2. deserialize
    println("serFastMap")
    SerDe.serFastMap(i2i, directBuf)
    SerDe.serFastMap(i2l, directBuf)
    SerDe.serFastMap(i2f, directBuf)
    SerDe.serFastMap(i2d, directBuf)
    SerDe.serFastMap(i2o, directBuf)
    SerDe.serFastMap(i2g, directBuf)
    SerDe.serFastMap(l2i, directBuf)
    SerDe.serFastMap(l2l, directBuf)
    SerDe.serFastMap(l2f, directBuf)
    SerDe.serFastMap(l2d, directBuf)
    SerDe.serFastMap(l2o, directBuf)
    SerDe.serFastMap(l2g, directBuf)

    // 3. serialize
    println("fastMapFromBuffer")
    val i2i_ = SerDe.fastMapFromBuffer[Int2IntOpenHashMap](directBuf)
    val i2l_ = SerDe.fastMapFromBuffer[Int2LongOpenHashMap](directBuf)
    val i2f_ = SerDe.fastMapFromBuffer[Int2FloatOpenHashMap](directBuf)
    val i2d_ = SerDe.fastMapFromBuffer[Int2DoubleOpenHashMap](directBuf)
    val i2o_ = SerDe.fastMapFromBuffer[Int2ObjectOpenHashMap[Array[Long]]](directBuf)
    val i2g_ = SerDe.fastMapFromBuffer[Int2ObjectOpenHashMap[UnTypedNode]](directBuf)
    val l2i_ = SerDe.fastMapFromBuffer[Long2IntOpenHashMap](directBuf)
    val l2l_ = SerDe.fastMapFromBuffer[Long2LongOpenHashMap](directBuf)
    val l2f_ = SerDe.fastMapFromBuffer[Long2FloatOpenHashMap](directBuf)
    val l2d_ = SerDe.fastMapFromBuffer[Long2DoubleOpenHashMap](directBuf)
    val l2o_ = SerDe.fastMapFromBuffer[Long2ObjectOpenHashMap[Array[Double]]](directBuf)
    val l2g_ = SerDe.fastMapFromBuffer[Long2ObjectOpenHashMap[UnTypedNode]](directBuf)

    println("OK!")
  }

}
