package com.tencent.angel.graph


import com.tencent.angel.graph.core.data.GData
import com.tencent.angel.graph.utils.ReflectUtils.getMethod
import com.tencent.angel.graph.utils.{FastHashMap, ReflectUtils, SerDe}
import com.tencent.angel.ml.math2.VFactory
import io.netty.buffer.{ByteBuf, Unpooled}
import it.unimi.dsi.fastutil.ints._
import it.unimi.dsi.fastutil.longs._
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random
import scala.reflect.runtime.universe._

case class ABC(i: Int, d: Array[Int], fast: FastHashMap[Int, Array[Double]]) extends GData {
  def this() = this(0, null, null)
}

class ABCD[T](val i: Int, val f: T) extends GData {
  def this() = this(0, null.asInstanceOf[T])
}

class DEF(val d: Double) extends Serializable

class SerDeTest extends AnyFunSuite {
  val directBuf: ByteBuf = Unpooled.buffer(2048)
  val rand = new Random()

  test("primitive") {
    val bo = true
    val by = "b".getBytes.head
    val c = "c".toCharArray.head
    val s = 123.toShort
    val i = 123
    val l = 123L
    val f = 12.3f
    val d = 12.3
    val str = "hello world!"

    SerDe.serPrimitive(bo, directBuf)
    SerDe.serPrimitive(by, directBuf)
    SerDe.serPrimitive(c, directBuf)
    SerDe.serPrimitive(s, directBuf)
    SerDe.serPrimitive(i, directBuf)
    SerDe.serPrimitive(l, directBuf)
    SerDe.serPrimitive(f, directBuf)
    SerDe.serPrimitive(d, directBuf)
    SerDe.serPrimitive(str, directBuf)

    println(SerDe.primitiveFromBuffer[Boolean](directBuf))
    println(SerDe.primitiveFromBuffer[Byte](directBuf))
    println(SerDe.primitiveFromBuffer[Char](directBuf))
    println(SerDe.primitiveFromBuffer[Short](directBuf))
    println(SerDe.primitiveFromBuffer[Int](directBuf))
    println(SerDe.primitiveFromBuffer[Long](directBuf))
    println(SerDe.primitiveFromBuffer[Float](directBuf))
    println(SerDe.primitiveFromBuffer[Double](directBuf))
    println(SerDe.primitiveFromBuffer[String](directBuf))

    println(SerDe.serPrimitiveBufSize(bo))
    println(SerDe.serPrimitiveBufSize(by))
    println(SerDe.serPrimitiveBufSize(c))
    println(SerDe.serPrimitiveBufSize(s))
    println(SerDe.serPrimitiveBufSize(i))
    println(SerDe.serPrimitiveBufSize(l))
    println(SerDe.serPrimitiveBufSize(f))
    println(SerDe.serPrimitiveBufSize(d))
    println(SerDe.serPrimitiveBufSize(str))

    println("OK")
  }

  test("primitive array") {
    val abo = Array[Boolean](true, false)
    val aby = "abc".getBytes
    val ac = "bcd".toCharArray
    val as = Array[Short](1, 2, 34)
    val ai = Array[Int](5, 3, 35)
    val al = Array[Long](8L, 2L, 74L)
    val af = Array[Float](1.0f, 2.2f, 3.4f)
    val ad = Array[Double](1.2, 2.5, 3.4)
    val astr = Array[String]("abx", "drf")

    SerDe.serArr(abo, directBuf)
    SerDe.serArr(aby, directBuf)
    SerDe.serArr(ac, directBuf)
    SerDe.serArr(as, directBuf)
    SerDe.serArr(ai, directBuf)
    SerDe.serArr(al, directBuf)
    SerDe.serArr(af, directBuf)
    SerDe.serArr(ad, directBuf)
    SerDe.serArr(astr, directBuf)

    println(SerDe.arrFromBuffer[Boolean](directBuf).mkString("{", ",", "}"))
    println(SerDe.arrFromBuffer[Byte](directBuf).mkString("{", ",", "}"))
    println(SerDe.arrFromBuffer[Char](directBuf).mkString("{", ",", "}"))
    println(SerDe.arrFromBuffer[Short](directBuf).mkString("{", ",", "}"))
    println(SerDe.arrFromBuffer[Int](directBuf).mkString("{", ",", "}"))
    println(SerDe.arrFromBuffer[Long](directBuf).mkString("{", ",", "}"))
    println(SerDe.arrFromBuffer[Float](directBuf).mkString("{", ",", "}"))
    println(SerDe.arrFromBuffer[Double](directBuf).mkString("{", ",", "}"))
    println(SerDe.arrFromBuffer[String](directBuf).mkString("{", ",", "}"))

    println("OK")
  }

  test("map") {
    // 1. create data
    val i2i = new Int2IntOpenHashMap(10)
    val i2l = new Int2LongOpenHashMap(10)
    val i2f = new Int2FloatOpenHashMap(10)
    val i2d = new Int2DoubleOpenHashMap(10)
    val i2o = new Int2ObjectOpenHashMap[Array[Long]](10)

    val rand = new Random()
    (0 until rand.nextInt(10)).foreach { _ =>
      val key = rand.nextInt()
      i2i.put(key, rand.nextInt())
      i2l.put(key, rand.nextLong())
      i2f.put(key, rand.nextFloat())
      i2d.put(key, rand.nextDouble())
      i2o.put(key, Array.tabulate[Long](5)(_ => rand.nextLong()))
    }

    val l2i = new Long2IntOpenHashMap(10)
    val l2l = new Long2LongOpenHashMap(10)
    val l2f = new Long2FloatOpenHashMap(10)
    val l2d = new Long2DoubleOpenHashMap(10)
    val l2o = new Long2ObjectOpenHashMap[Array[Double]](10)

    (0 until rand.nextInt(10)).foreach { _ =>
      val key = rand.nextLong()
      l2i.put(key, rand.nextInt())
      l2l.put(key, rand.nextLong())
      l2f.put(key, rand.nextFloat())
      l2d.put(key, rand.nextDouble())
      l2o.put(key, Array.tabulate[Double](5)(_ => rand.nextLong()))
    }

    // 2. deserialize
    println("serFastMap")
    SerDe.serFastMap(i2i, directBuf)
    SerDe.serFastMap(i2l, directBuf)
    SerDe.serFastMap(i2f, directBuf)
    SerDe.serFastMap(i2d, directBuf)
    SerDe.serFastMap(i2o, directBuf)
    SerDe.serFastMap(l2i, directBuf)
    SerDe.serFastMap(l2l, directBuf)
    SerDe.serFastMap(l2f, directBuf)
    SerDe.serFastMap(l2d, directBuf)
    SerDe.serFastMap(l2o, directBuf)

    // 3. serialize
    println("fastMapFromBuffer")
    val i2i_ = SerDe.fastMapFromBuffer[Int2IntOpenHashMap](directBuf)
    val i2l_ = SerDe.fastMapFromBuffer[Int2LongOpenHashMap](directBuf)
    val i2f_ = SerDe.fastMapFromBuffer[Int2FloatOpenHashMap](directBuf)
    val i2d_ = SerDe.fastMapFromBuffer[Int2DoubleOpenHashMap](directBuf)
    val i2o_ = SerDe.fastMapFromBuffer[Int2ObjectOpenHashMap[Array[Long]]](directBuf)
    val l2i_ = SerDe.fastMapFromBuffer[Long2IntOpenHashMap](directBuf)
    val l2l_ = SerDe.fastMapFromBuffer[Long2LongOpenHashMap](directBuf)
    val l2f_ = SerDe.fastMapFromBuffer[Long2FloatOpenHashMap](directBuf)
    val l2d_ = SerDe.fastMapFromBuffer[Long2DoubleOpenHashMap](directBuf)
    val l2o_ = SerDe.fastMapFromBuffer[Long2ObjectOpenHashMap[Array[Double]]](directBuf)

    println("OK!")
  }

  test("vector") {
    val idu = VFactory.intDummyVector(10, Array(1, 3, 5, 7))

    val dsi = VFactory.denseIntVector(Array(1, 3, 5, 7, 9, 10))
    val sti = VFactory.sortedIntVector(10, Array(1, 3, 5, 7, 9), Array(2, 4, 6, 8, 10))
    val spi = VFactory.sparseIntVector(100, Array(1, 30, 50, 71, 89, 99), Array(2, 40, 65, 89, 190, 666))

    val dsl = VFactory.denseLongVector(Array(1L, 3L, 5L, 7L, 9L, 10L))
    val stl = VFactory.sortedLongVector(10, Array(1, 3, 5, 7, 9), Array(2L, 4L, 6L, 8L, 10L))
    val spl = VFactory.sparseLongVector(100, Array(1, 30, 50, 71, 89, 99), Array(2L, 40L, 65L, 89L, 190L, 666L))

    val dsf = VFactory.denseFloatVector(Array(1.5f, 3.6f, 5.8f, 7.9f, 9.2f, 10.6f))
    val stf = VFactory.sortedFloatVector(10, Array(1, 3, 5, 7, 9), Array(2.5f, 4.8f, 6.4f, 8.8f, 10.9f))
    val spf = VFactory.sparseFloatVector(100, Array(1, 30, 50, 71, 89, 99), Array(2.6f, 40.8f, 65.4f, 89.0f, 190.3f, 666.8f))

    val dsd = VFactory.denseDoubleVector(Array(1.5, 3.9, 5.7, 7.4, 9.7, 10.9))
    val std = VFactory.sortedDoubleVector(10, Array(1, 3, 5, 7, 9), Array(2.4, 4.1, 6.7, 8.6, 10.3))
    val spd = VFactory.sparseDoubleVector(100, Array(1, 30, 50, 71, 89, 99), Array(2.3, 40.5, 65.4, 89.2, 190.4, 666.6))

    val lidu = VFactory.longDummyVector(10, Array(1L, 3L, 5L, 7L))

    val lsti = VFactory.sortedLongKeyIntVector(10L, Array(1L, 3L, 5L, 7L, 9L), Array(2, 4, 6, 8, 10))
    val lspi = VFactory.sparseLongKeyIntVector(100L, Array(1L, 30L, 50L, 71L, 89L, 99L), Array(2, 40, 65, 89, 190, 666))

    val lstl = VFactory.sortedLongKeyLongVector(10L, Array(1L, 3L, 5L, 7L, 9L), Array(2L, 4L, 6L, 8L, 10L))
    val lspl = VFactory.sparseLongKeyLongVector(100L, Array(1L, 30L, 50L, 71L, 89L, 99L), Array(2L, 40L, 65L, 89L, 190L, 666L))

    val lstf = VFactory.sortedLongKeyFloatVector(10L, Array(1L, 3L, 5L, 7L, 9L), Array(2.5f, 4.8f, 6.4f, 8.8f, 10.9f))
    val lspf = VFactory.sparseLongKeyFloatVector(100L, Array(1L, 30L, 50L, 71L, 89L, 99L), Array(2.6f, 40.8f, 65.4f, 89.0f, 190.3f, 666.8f))

    val lstd = VFactory.sortedLongKeyDoubleVector(10L, Array(1L, 3L, 5L, 7L, 9L), Array(2.4, 4.1, 6.7, 8.6, 10.3))
    val lspd = VFactory.sparseLongKeyDoubleVector(100L, Array(1L, 30L, 50L, 71L, 89L, 99L), Array(2.3, 40.5, 65.4, 89.2, 190.4, 666.6))

    val allVectors = Array(idu, dsi, sti, spi, dsl, stl, spl, dsf, stf, spf, dsd, std, spd,
      lidu,  lsti, lspi, lstl, lspl, lstf, lspf, lstd, lspd)

    allVectors.foreach{ vec => SerDe.serVector(vec, directBuf)}

    allVectors.foreach{ vec =>
      val tpe = ReflectUtils.getType(vec)
      val desered = SerDe.vectorFromBuffer(tpe, directBuf)
    }

    print("OK")
  }

  test("FastHashMap1"){
    val i2d = new FastHashMap[Int, Double](10)
    (0 until 100).foreach{ _ =>
      i2d(rand.nextInt()) = rand.nextDouble()
    }

    i2d.serialize(directBuf)

    val i2d2 = new FastHashMap[Int, Double](10)
    i2d2.deserialize(directBuf)
    i2d.foreach{ case (k, v) => assert((v - i2d2(k)) < 1e-10)}
    println("OK")
  }

  test("FastHashMap2") {
    val i2d = new FastHashMap[Int, Array[Double]](10)
    (0 until 100).foreach { _ =>
      i2d(rand.nextInt()) = Array.tabulate[Double](rand.nextInt(20)+1){ _ => rand.nextDouble()}
    }

    i2d.serialize(directBuf)

    val i2d2 = new FastHashMap[Int, Array[Double]](10)
    i2d2.deserialize(directBuf)
    i2d.foreach { case (k, v) => v.zip(i2d2(k)).foreach{ case (x, y) => assert( (x - y)< 1e-10) } }
    println("OK")
  }

  test("FastHashMap3") {
    val i2d = new FastHashMap[Int, ABCD[Float]](10)
    val tpe = typeOf[FastHashMap[Int, ABCD[Float]]]

    val i2d2 = ReflectUtils.newFastHashMap(tpe)

    println("OK")
  }

  test("FastHashMap4") {
    val map = new FastHashMap[Int, Array[Double]](10)
    map(3) = Array(8.0, 8.8)
    map(4) = Array(43.0, 7.5)
    map(7) = Array(4.0, 5.35)
    map(12) = Array(3.0, 4.67)
    map(17) = Array(3.0, 8.42)
    val abc = ABC(5, Array(2, 3, 4, 5), map)

    SerDe.serialize(abc, directBuf)
    val abc2 = new ABC()
    SerDe.deserialize(abc2, directBuf)

    println("OK")
  }

}
