package com.tencent.angel.graph

import com.tencent.angel.graph.core.data.GData
import com.tencent.angel.graph.utils.ReflectUtils.getMethod
import com.tencent.angel.graph.utils.{FastHashMap, ReflectUtils, SerDe}
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector._
import com.tencent.angel.ml.math2.storage._
import io.netty.buffer.{ByteBuf, Unpooled}
import it.unimi.dsi.fastutil.ints._
import it.unimi.dsi.fastutil.longs._
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.reflect.runtime.universe._

case class ABC(i: Int, d: Array[Int], fast: FastHashMap[Int, Array[Double]]) extends GData {
  def this() = this(0, null, null)
}

class ABCD[T](val i: Int, val f: T) extends GData {
  def this() = this(0, null.asInstanceOf[T])
}

class DEF(val d: Double) extends Serializable {
  def this() = this(0.8)
  val ta = 4.0
}

class GHI(val i: Int, val f: Float) extends Serializable {
  def this() = this(3, 0.8f)
  val tib = 4.0
  val ti = 4
}

class SerDeTest extends AnyFunSuite {
  val directBuf: ByteBuf = Unpooled.buffer(4096)
  val rand = new Random()

  test("01 primitive") {
    val bo = true
    val by = "b".getBytes.head
    val c = "c".toCharArray.head
    val s = 123.toShort
    val i = 123
    val l = 123L
    val f = 12.3f
    val d = 12.3
    val str = "hello world!"

    var bufSize = 0
    SerDe.serPrimitive(bo, directBuf)
    bufSize += SerDe.serPrimitiveBufSize(bo)
    assert(directBuf.writerIndex() - directBuf.readerIndex() == bufSize)

    SerDe.serPrimitive(by, directBuf)
    bufSize += SerDe.serPrimitiveBufSize(by)
    assert(directBuf.writerIndex() - directBuf.readerIndex() == bufSize)

    SerDe.serPrimitive(c, directBuf)
    bufSize += SerDe.serPrimitiveBufSize(c)
    assert(directBuf.writerIndex() - directBuf.readerIndex() == bufSize)

    SerDe.serPrimitive(s, directBuf)
    bufSize += SerDe.serPrimitiveBufSize(s)
    assert(directBuf.writerIndex() - directBuf.readerIndex() == bufSize)

    SerDe.serPrimitive(i, directBuf)
    bufSize += SerDe.serPrimitiveBufSize(i)
    assert(directBuf.writerIndex() - directBuf.readerIndex() == bufSize)

    SerDe.serPrimitive(l, directBuf)
    bufSize += SerDe.serPrimitiveBufSize(l)
    assert(directBuf.writerIndex() - directBuf.readerIndex() == bufSize)

    SerDe.serPrimitive(f, directBuf)
    bufSize += SerDe.serPrimitiveBufSize(f)
    assert(directBuf.writerIndex() - directBuf.readerIndex() == bufSize)

    SerDe.serPrimitive(d, directBuf)
    bufSize += SerDe.serPrimitiveBufSize(d)
    assert(directBuf.writerIndex() - directBuf.readerIndex() == bufSize)

    SerDe.serPrimitive(str, directBuf)
    bufSize += SerDe.serPrimitiveBufSize(str)
    assert(directBuf.writerIndex() - directBuf.readerIndex() == bufSize)

    assert(SerDe.primitiveFromBuffer[Boolean](directBuf) == bo)
    assert(SerDe.primitiveFromBuffer[Byte](directBuf) == by)
    assert(SerDe.primitiveFromBuffer[Char](directBuf) == c)
    assert(SerDe.primitiveFromBuffer[Short](directBuf) == s)
    assert(SerDe.primitiveFromBuffer[Int](directBuf) == i)
    assert(SerDe.primitiveFromBuffer[Long](directBuf) == l)
    assert(SerDe.primitiveFromBuffer[Float](directBuf) == f)
    assert(SerDe.primitiveFromBuffer[Double](directBuf) == d)
    assert(SerDe.primitiveFromBuffer[String](directBuf) == str)

    //    println(SerDe.serPrimitiveBufSize(bo))
    //    println(SerDe.serPrimitiveBufSize(by))
    //    println(SerDe.serPrimitiveBufSize(c))
    //    println(SerDe.serPrimitiveBufSize(s))
    //    println(SerDe.serPrimitiveBufSize(i))
    //    println(SerDe.serPrimitiveBufSize(l))
    //    println(SerDe.serPrimitiveBufSize(f))
    //    println(SerDe.serPrimitiveBufSize(d))
    //    println(SerDe.serPrimitiveBufSize(str))

    println("test 01 OK")
  }

  test("02 primitive array") {
    val aemp = Array[Boolean]()
    val abo = Array[Boolean](true, false)
    val aby = "abc".getBytes
    val ac = "bcd".toCharArray
    val as = Array[Short](1, 2, 34)
    val ai = Array[Int](5, 3, 35)
    val al = Array[Long](8L, 2L, 74L)
    val af = Array[Float](1.0f, 2.2f, 3.4f)
    val ad = Array[Double](1.2, 2.5, 3.4)
    val astr = Array[String]("abx", "drf")

    var bufSize = 0
    SerDe.serArr(aemp, directBuf)
    bufSize += SerDe.serArrBufSize(aemp)
    assert(directBuf.writerIndex() - directBuf.readerIndex() == bufSize)

    SerDe.serArr(abo, directBuf)
    bufSize += SerDe.serArrBufSize(abo)
    assert(directBuf.writerIndex() - directBuf.readerIndex() == bufSize)

    SerDe.serArr(aby, directBuf)
    bufSize += SerDe.serArrBufSize(aby)
    assert(directBuf.writerIndex() - directBuf.readerIndex() == bufSize)

    SerDe.serArr(ac, directBuf)
    bufSize += SerDe.serArrBufSize(ac)
    assert(directBuf.writerIndex() - directBuf.readerIndex() == bufSize)

    SerDe.serArr(as, directBuf)
    bufSize += SerDe.serArrBufSize(as)
    assert(directBuf.writerIndex() - directBuf.readerIndex() == bufSize)

    SerDe.serArr(ai, directBuf)
    bufSize += SerDe.serArrBufSize(ai)
    assert(directBuf.writerIndex() - directBuf.readerIndex() == bufSize)

    SerDe.serArr(al, directBuf)
    bufSize += SerDe.serArrBufSize(al)
    assert(directBuf.writerIndex() - directBuf.readerIndex() == bufSize)

    SerDe.serArr(af, directBuf)
    bufSize += SerDe.serArrBufSize(af)
    assert(directBuf.writerIndex() - directBuf.readerIndex() == bufSize)

    SerDe.serArr(ad, directBuf)
    bufSize += SerDe.serArrBufSize(ad)
    assert(directBuf.writerIndex() - directBuf.readerIndex() == bufSize)

    SerDe.serArr(astr, directBuf)
    bufSize += SerDe.serArrBufSize(astr)
    assert(directBuf.writerIndex() - directBuf.readerIndex() == bufSize)

    //    assert(SerDe.serArrBufSize(aemp) == 4)
    //    println(SerDe.serArrBufSize(abo))
    //    println(SerDe.serArrBufSize(aby))
    //    println(SerDe.serArrBufSize(ac))
    //    println(SerDe.serArrBufSize(as))
    //    println(SerDe.serArrBufSize(ai))
    //    println(SerDe.serArrBufSize(al))
    //    println(SerDe.serArrBufSize(af))
    //    println(SerDe.serArrBufSize(ad))
    //    println(SerDe.serArrBufSize(astr))

    val aemp_ = SerDe.arrFromBuffer[Boolean](directBuf)
    for (idx <- aemp.indices) {
      assert(aemp_(idx) == aemp(idx))
    }
    val abo_ = SerDe.arrFromBuffer[Boolean](directBuf)
    for (idx <- abo.indices) {
      assert(abo_(idx) == abo(idx))
    }
    val aby_ = SerDe.arrFromBuffer[Byte](directBuf)
    for (idx <- aby.indices) {
      assert(aby_(idx) == aby(idx))
    }
    val ac_ = SerDe.arrFromBuffer[Char](directBuf)
    for (idx <- ac.indices) {
      assert(ac_(idx) == ac(idx))
    }
    val as_ = SerDe.arrFromBuffer[Short](directBuf)
    for (idx <- as.indices) {
      assert(as_(idx) == as(idx))
    }
    val ai_ = SerDe.arrFromBuffer[Int](directBuf)
    for (idx <- ai.indices) {
      assert(ai_(idx) == ai(idx))
    }
    val al_ = SerDe.arrFromBuffer[Long](directBuf)
    for (idx <- al.indices) {
      assert(al_(idx) == al(idx))
    }
    val af_ = SerDe.arrFromBuffer[Float](directBuf)
    for (idx <- af.indices) {
      assert(af_(idx) == af(idx))
    }
    val ad_ = SerDe.arrFromBuffer[Double](directBuf)
    for (idx <- ad.indices) {
      assert(ad_(idx) == ad(idx))
    }
    val astr_ = SerDe.arrFromBuffer[String](directBuf)
    for (idx <- astr.indices) {
      assert(astr_(idx) == astr(idx))
    }

    println("test 02 OK")
  }

  test("03 array with start and end") {
    val abo = Array[Boolean](true, false, false, true)
    val aby = "abcde".getBytes
    val ac = "bcdde".toCharArray
    val as = Array[Short](1, 2, 34, 45)
    val ai = Array[Int](5, 3, 35, 56)
    val al = Array[Long](8L, 2L, 74L, 98L)
    val af = Array[Float](1.0f, 2.2f, 3.4f, 6.9f)
    val ad = Array[Double](1.2, 2.5, 3.4, 6.9)
    val astr = Array[String]("abx", "drf", "sdf", "afa")

    var bufSize = 0
    SerDe.serArr(abo, 1, 3, directBuf)
    bufSize += SerDe.serArrBufSize(abo, 1, 3)
    assert(directBuf.writerIndex() - directBuf.readerIndex() == bufSize)

    SerDe.serArr(aby, 1, 3, directBuf)
    bufSize += SerDe.serArrBufSize(aby, 1, 3)
    assert(directBuf.writerIndex() - directBuf.readerIndex() == bufSize)

    SerDe.serArr(ac, 1, 3, directBuf)
    bufSize += SerDe.serArrBufSize(ac, 1, 3)
    assert(directBuf.writerIndex() - directBuf.readerIndex() == bufSize)

    SerDe.serArr(as, 1, 3, directBuf)
    bufSize += SerDe.serArrBufSize(as, 1, 3)
    assert(directBuf.writerIndex() - directBuf.readerIndex() == bufSize)

    SerDe.serArr(ai, 1, 3, directBuf)
    bufSize += SerDe.serArrBufSize(ai, 1, 3)
    assert(directBuf.writerIndex() - directBuf.readerIndex() == bufSize)

    SerDe.serArr(al, 1, 3, directBuf)
    bufSize += SerDe.serArrBufSize(al, 1, 3)
    assert(directBuf.writerIndex() - directBuf.readerIndex() == bufSize)

    SerDe.serArr(af, 1, 3, directBuf)
    bufSize += SerDe.serArrBufSize(af, 1, 3)
    assert(directBuf.writerIndex() - directBuf.readerIndex() == bufSize)

    SerDe.serArr(ad, 1, 3, directBuf)
    bufSize += SerDe.serArrBufSize(ad, 1, 3)
    assert(directBuf.writerIndex() - directBuf.readerIndex() == bufSize)

    SerDe.serArr(astr, 1, 3, directBuf)
    bufSize += SerDe.serArrBufSize(astr, 1, 3)
    assert(directBuf.writerIndex() - directBuf.readerIndex() == bufSize)

    //
    //    println(SerDe.serArrBufSize(abo, 1, 3))
    //    println(SerDe.serArrBufSize(aby, 1, 3))
    //    println(SerDe.serArrBufSize(ac, 1, 3))
    //    println(SerDe.serArrBufSize(as, 1, 3))
    //    println(SerDe.serArrBufSize(ai, 1, 3))
    //    println(SerDe.serArrBufSize(al, 1, 3))
    //    println(SerDe.serArrBufSize(af, 1, 3))
    //    println(SerDe.serArrBufSize(ad, 1, 3))
    //    println(SerDe.serArrBufSize(astr, 1, 3))

    val abo_ = SerDe.arrFromBuffer[Boolean](directBuf)
    for (idx <- abo_.indices) {
      assert(abo_(idx) == abo(idx + 1))
    }
    val aby_ = SerDe.arrFromBuffer[Byte](directBuf)
    for (idx <- aby_.indices) {
      assert(aby_(idx) == aby(idx + 1))
    }
    val ac_ = SerDe.arrFromBuffer[Char](directBuf)
    for (idx <- ac_.indices) {
      assert(ac_(idx) == ac(idx + 1))
    }
    val as_ = SerDe.arrFromBuffer[Short](directBuf)
    for (idx <- as_.indices) {
      assert(as_(idx) == as(idx + 1))
    }
    val ai_ = SerDe.arrFromBuffer[Int](directBuf)
    for (idx <- ai_.indices) {
      assert(ai_(idx) == ai(idx + 1))
    }
    val al_ = SerDe.arrFromBuffer[Long](directBuf)
    for (idx <- al_.indices) {
      assert(al_(idx) == al(idx + 1))
    }
    val af_ = SerDe.arrFromBuffer[Float](directBuf)
    for (idx <- af_.indices) {
      assert(af_(idx) == af(idx + 1))
    }
    val ad_ = SerDe.arrFromBuffer[Double](directBuf)
    for (idx <- ad_.indices) {
      assert(ad_(idx) == ad(idx + 1))
    }
    val astr_ = SerDe.arrFromBuffer[String](directBuf)
    for (idx <- astr_.indices) {
      assert(astr_(idx) == astr(idx + 1))
    }
    println("test 03 OK")
  }

  // map with no start end pos
  test("04 map") {
    // 1. create data
    val fastp = new FastHashMap[Int, Int](10)
    val i2bo = new Int2BooleanOpenHashMap(10)
    val i2by = new Int2ByteOpenHashMap(10)
    val i2c = new Int2CharOpenHashMap(10)
    val i2s = new Int2ShortOpenHashMap(10)
    val i2i = new Int2IntOpenHashMap(10)
    val i2l = new Int2LongOpenHashMap(10)
    val i2f = new Int2FloatOpenHashMap(10)
    val i2d = new Int2DoubleOpenHashMap(10)
    val i2o = new Int2ObjectOpenHashMap[Array[Long]](10)

    val rand = new Random()
    (0 until rand.nextInt(10)).foreach { _ =>
      val key = rand.nextInt()
      fastp.put(key, rand.nextInt())
      i2bo.put(key, rand.nextBoolean())
      i2by.put(key, rand.nextPrintableChar().toByte)
      i2c.put(key, rand.nextPrintableChar())
      i2s.put(key, rand.nextInt().toShort)
      i2i.put(key, rand.nextInt())
      i2l.put(key, rand.nextLong())
      i2f.put(key, rand.nextFloat())
      i2d.put(key, rand.nextDouble())
      i2o.put(key, Array.tabulate[Long](5)(_ => rand.nextLong()))

    }

    val l2bo = new Long2BooleanOpenHashMap(10)
    val l2by = new Long2ByteOpenHashMap(10)
    val l2c = new Long2CharOpenHashMap(10)
    val l2s = new Long2ShortOpenHashMap(10)
    val l2i = new Long2IntOpenHashMap(10)
    val l2l = new Long2LongOpenHashMap(10)
    val l2f = new Long2FloatOpenHashMap(10)
    val l2d = new Long2DoubleOpenHashMap(10)
    val l2o = new Long2ObjectOpenHashMap[Array[Double]](10)

    (0 until rand.nextInt(10)).foreach { _ =>
      val key = rand.nextLong()
      l2bo.put(key, rand.nextBoolean())
      l2by.put(key, rand.nextInt().toByte)
      l2c.put(key, rand.nextPrintableChar())
      l2s.put(key, rand.nextInt().toShort)
      l2i.put(key, rand.nextInt())
      l2l.put(key, rand.nextLong())
      l2f.put(key, rand.nextFloat())
      l2d.put(key, rand.nextDouble())
      l2o.put(key, Array.tabulate[Double](5)(_ => rand.nextDouble()))
    }

    // 2. deserialize
    //    println("serFastMap")
    var bufSize = 0

    SerDe.serFastMap(fastp, directBuf)
    bufSize += SerDe.serFastMapBufSize(fastp)
    assert(directBuf.writerIndex() - directBuf.readerIndex() == bufSize)

    SerDe.serFastMap(i2bo, directBuf)
    bufSize += SerDe.serFastMapBufSize(i2bo)
    assert(directBuf.writerIndex() - directBuf.readerIndex() == bufSize)

    SerDe.serFastMap(i2by, directBuf)
    bufSize += SerDe.serFastMapBufSize(i2by)
    assert(directBuf.writerIndex() - directBuf.readerIndex() == bufSize)

    SerDe.serFastMap(i2c, directBuf)
    bufSize += SerDe.serFastMapBufSize(i2c)
    assert(directBuf.writerIndex() - directBuf.readerIndex() == bufSize)

    SerDe.serFastMap(i2s, directBuf)
    bufSize += SerDe.serFastMapBufSize(i2s)
    assert(directBuf.writerIndex() - directBuf.readerIndex() == bufSize)

    SerDe.serFastMap(i2i, directBuf)
    bufSize += SerDe.serFastMapBufSize(i2i)
    assert(directBuf.writerIndex() - directBuf.readerIndex() == bufSize)

    SerDe.serFastMap(i2l, directBuf)
    bufSize += SerDe.serFastMapBufSize(i2l)
    assert(directBuf.writerIndex() - directBuf.readerIndex() == bufSize)

    SerDe.serFastMap(i2f, directBuf)
    bufSize += SerDe.serFastMapBufSize(i2f)
    assert(directBuf.writerIndex() - directBuf.readerIndex() == bufSize)

    SerDe.serFastMap(i2d, directBuf)
    bufSize += SerDe.serFastMapBufSize(i2d)
    assert(directBuf.writerIndex() - directBuf.readerIndex() == bufSize)

    SerDe.serFastMap(i2o, directBuf)
    bufSize += SerDe.serFastMapBufSize(i2o)
    assert(directBuf.writerIndex() - directBuf.readerIndex() == bufSize)

    SerDe.serFastMap(l2bo, directBuf)
    bufSize += SerDe.serFastMapBufSize(l2bo)
    assert(directBuf.writerIndex() - directBuf.readerIndex() == bufSize)

    SerDe.serFastMap(l2by, directBuf)
    bufSize += SerDe.serFastMapBufSize(l2by)
    assert(directBuf.writerIndex() - directBuf.readerIndex() == bufSize)

    SerDe.serFastMap(l2c, directBuf)
    bufSize += SerDe.serFastMapBufSize(l2c)
    assert(directBuf.writerIndex() - directBuf.readerIndex() == bufSize)

    SerDe.serFastMap(l2s, directBuf)
    bufSize += SerDe.serFastMapBufSize(l2s)
    assert(directBuf.writerIndex() - directBuf.readerIndex() == bufSize)

    SerDe.serFastMap(l2i, directBuf)
    bufSize += SerDe.serFastMapBufSize(l2i)
    assert(directBuf.writerIndex() - directBuf.readerIndex() == bufSize)

    SerDe.serFastMap(l2l, directBuf)
    bufSize += SerDe.serFastMapBufSize(l2l)
    assert(directBuf.writerIndex() - directBuf.readerIndex() == bufSize)

    SerDe.serFastMap(l2f, directBuf)
    bufSize += SerDe.serFastMapBufSize(l2f)
    assert(directBuf.writerIndex() - directBuf.readerIndex() == bufSize)

    SerDe.serFastMap(l2d, directBuf)
    bufSize += SerDe.serFastMapBufSize(l2d)
    assert(directBuf.writerIndex() - directBuf.readerIndex() == bufSize)

    SerDe.serFastMap(l2o, directBuf)
    bufSize += SerDe.serFastMapBufSize(l2o)
    assert(directBuf.writerIndex() - directBuf.readerIndex() == bufSize)


    // 3. serialize
    //    println("fastMapFromBuffer")
    val fastp_ = SerDe.fastMapFromBuffer[FastHashMap[Int, Int]](directBuf)
    val i2bo_ = SerDe.fastMapFromBuffer[Int2BooleanOpenHashMap](directBuf)
    val i2by_  = SerDe.fastMapFromBuffer[Int2ByteOpenHashMap](directBuf)
    val i2c_ = SerDe.fastMapFromBuffer[Int2CharOpenHashMap](directBuf)
    val i2s_ = SerDe.fastMapFromBuffer[Int2ShortOpenHashMap](directBuf)
    val i2i_ = SerDe.fastMapFromBuffer[Int2IntOpenHashMap](directBuf)
    val i2l_ = SerDe.fastMapFromBuffer[Int2LongOpenHashMap](directBuf)
    val i2f_ = SerDe.fastMapFromBuffer[Int2FloatOpenHashMap](directBuf)
    val i2d_ = SerDe.fastMapFromBuffer[Int2DoubleOpenHashMap](directBuf)
    val i2o_ = SerDe.fastMapFromBuffer[Int2ObjectOpenHashMap[Array[Long]]](directBuf)

    val l2bo_ = SerDe.fastMapFromBuffer[Long2BooleanOpenHashMap](directBuf)
    val l2by_ = SerDe.fastMapFromBuffer[Long2ByteOpenHashMap](directBuf)
    val l2c_ = SerDe.fastMapFromBuffer[Long2CharOpenHashMap](directBuf)
    val l2s_ = SerDe.fastMapFromBuffer[Long2ShortOpenHashMap](directBuf)
    val l2i_ = SerDe.fastMapFromBuffer[Long2IntOpenHashMap](directBuf)
    val l2l_ = SerDe.fastMapFromBuffer[Long2LongOpenHashMap](directBuf)
    val l2f_ = SerDe.fastMapFromBuffer[Long2FloatOpenHashMap](directBuf)
    val l2d_ = SerDe.fastMapFromBuffer[Long2DoubleOpenHashMap](directBuf)
    val l2o_ = SerDe.fastMapFromBuffer[Long2ObjectOpenHashMap[Array[Double]]](directBuf)

    for (key <- fastp.keyArray) {
      assert(fastp.get(key) == fastp_.get(key))
    }
    for (key <- i2bo.keySet().toIntArray) {
      assert(i2bo.get(key) == i2bo_.get(key))
    }
    for (key <- i2by.keySet().toIntArray) {
      assert(i2by.get(key) == i2by_.get(key))
    }
    for (key <- i2c.keySet().toIntArray) {
      assert(i2c.get(key) == i2c_.get(key))
    }
    for (key <- i2s.keySet().toIntArray) {
      assert(i2s.get(key) == i2s_.get(key))
    }
    for (key <- i2i.keySet().toIntArray) {
      assert(i2i.get(key) == i2i_.get(key))
    }
    for (key <- i2l.keySet().toIntArray) {
      assert(i2l.get(key) == i2l_.get(key))
    }
    for (key <- i2f.keySet().toIntArray) {
      assert(i2f.get(key) == i2f_.get(key))
    }
    for (key <- i2d.keySet().toIntArray) {
      assert(i2d.get(key) == i2d_.get(key))
    }
    //    for (key <- i2o.keySet()) {
    //      assert(i2o.get(key) == i2o_.get(key))
    //    }
    for (key <- l2bo.keySet().toLongArray) {
      assert(l2bo.get(key) == l2bo_.get(key))
    }
    for (key <- l2by.keySet().toLongArray) {
      assert(l2by.get(key) == l2by_.get(key))
    }
    for (key <- l2c.keySet().toLongArray) {
      assert(l2c.get(key) == l2c_.get(key))
    }
    for (key <- l2s.keySet().toLongArray) {
      assert(l2s.get(key) == l2s_.get(key))
    }
    for (key <- l2i.keySet().toLongArray) {
      assert(l2i.get(key) == l2i_.get(key))
    }
    for (key <- l2l.keySet().toLongArray) {
      assert(l2l.get(key) == l2l_.get(key))
    }
    for (key <- l2f.keySet().toLongArray) {
      assert(l2f.get(key) == l2f_.get(key))
    }
    for (key <- l2d.keySet().toLongArray) {
      assert(l2d.get(key) == l2d_.get(key))
    }
    //    for (key <- i2o.keySet()) {
    //      assert(i2o.get(key) == i2o_.get(key))
    //    }

    //    println(SerDe.serFastMapBufSize(fastp))
    //    println(SerDe.serFastMapBufSize(i2bo))
    //    println(SerDe.serFastMapBufSize(i2by))
    //    println(SerDe.serFastMapBufSize(i2c))
    //    println(SerDe.serFastMapBufSize(i2s))
    //    println(SerDe.serFastMapBufSize(i2i))
    //    println(SerDe.serFastMapBufSize(i2l))
    //    println(SerDe.serFastMapBufSize(i2f))
    //    println(SerDe.serFastMapBufSize(i2d))
    //    println(SerDe.serFastMapBufSize(i2o))
    //    println(SerDe.serFastMapBufSize(l2bo))
    //    println(SerDe.serFastMapBufSize(l2by))
    //    println(SerDe.serFastMapBufSize(l2c))
    //    println(SerDe.serFastMapBufSize(l2s))
    //    println(SerDe.serFastMapBufSize(l2i))
    //    println(SerDe.serFastMapBufSize(l2l))
    //    println(SerDe.serFastMapBufSize(l2f))
    //    println(SerDe.serFastMapBufSize(l2d))
    //    println(SerDe.serFastMapBufSize(l2o))

    println("test 04 OK")
  }

  test("05 fastMap with object") {
    val i2oG = new Int2ObjectOpenHashMap[GData](10)
    val i2oS = new Int2ObjectOpenHashMap[Serializable](10)
    val i2oV = new Int2ObjectOpenHashMap[Vector](10)
    val i2oAbo = new Int2ObjectOpenHashMap[Array[Boolean]](10)
    val i2oAby = new Int2ObjectOpenHashMap[Array[Byte]](10)
    val i2oAc = new Int2ObjectOpenHashMap[Array[Char]](10)
    val i2oAs = new Int2ObjectOpenHashMap[Array[Short]](10)
    val i2oAi = new Int2ObjectOpenHashMap[Array[Int]](10)
    val i2oAl = new Int2ObjectOpenHashMap[Array[Long]](10)
    val i2oAf = new Int2ObjectOpenHashMap[Array[Float]](10)
    val i2oAd = new Int2ObjectOpenHashMap[Array[Double]](10)
    val i2oStr = new Int2ObjectOpenHashMap[String](10)

    (0 until rand.nextInt(10)).foreach { _ =>
      i2oG.put(rand.nextInt(), new ABC())
      i2oS.put(rand.nextInt(), new DEF())
      i2oS.put(rand.nextInt(), new GHI())
      i2oV.put(rand.nextInt(), new IntDummyVector(10, Array(1, 3, 5, 7)))
      i2oAbo.put(rand.nextInt(), Array.tabulate[Boolean](5)(_ => rand.nextBoolean()))
      i2oAby.put(rand.nextInt(), Array.tabulate[Byte](5)(_ => rand.nextInt().toByte))
      i2oAc.put(rand.nextInt(), Array.tabulate[Char](5)(_ => rand.nextPrintableChar()))
      i2oAs.put(rand.nextInt(), Array.tabulate[Short](5)(_ => rand.nextInt.toShort))
      i2oAi.put(rand.nextInt(), Array.tabulate[Int](5)(_ => rand.nextInt()))
      i2oAl.put(rand.nextInt(), Array.tabulate[Long](5)(_ => rand.nextLong()))
      i2oAf.put(rand.nextInt(), Array.tabulate[Float](5)(_ => rand.nextFloat()))
      val ads = Array.tabulate[Double](5)(_ => rand.nextDouble())
      i2oAd.put(rand.nextInt(), ads)
      i2oStr.put(rand.nextInt(), rand.nextString(7))
    }
    //    println(SerDe.serFastMapBufSize(i2oG))
    ////    println(SerDe.serFastMapBufSize(i2oS))
    //    println(SerDe.serFastMapBufSize(i2oAbo))
    //    println(SerDe.serFastMapBufSize(i2oAby))
    //    println(SerDe.serFastMapBufSize(i2oAc))
    //    println(SerDe.serFastMapBufSize(i2oAs))
    //    println(SerDe.serFastMapBufSize(i2oAi))
    //    println(SerDe.serFastMapBufSize(i2oAl))
    //    println(SerDe.serFastMapBufSize(i2oAf))
    //    println(SerDe.serFastMapBufSize(i2oAd))
    //    println(SerDe.serFastMapBufSize(i2oStr))

    var bufSize = 0
    SerDe.serFastMap(i2oG, directBuf)
    bufSize += SerDe.serFastMapBufSize(i2oG)
    assert(bufSize == (directBuf.writerIndex() - directBuf.readerIndex()))

    SerDe.serFastMap(i2oS, directBuf)
    //    bufSize += SerDe.serFastMapBufSize(i2oS)
    //    assert(bufSize == (directBuf.writerIndex() - directBuf.readerIndex()))
    bufSize = directBuf.writerIndex() - directBuf.readerIndex()

    SerDe.serFastMap(i2oV, directBuf)
    bufSize += SerDe.serFastMapBufSize(i2oV)
    assert(bufSize == (directBuf.writerIndex() - directBuf.readerIndex()))

    SerDe.serFastMap(i2oAbo, directBuf)
    bufSize += SerDe.serFastMapBufSize(i2oAbo)
    assert(bufSize == (directBuf.writerIndex() - directBuf.readerIndex()))

    SerDe.serFastMap(i2oAby, directBuf)
    bufSize += SerDe.serFastMapBufSize(i2oAby)
    assert(bufSize == (directBuf.writerIndex() - directBuf.readerIndex()))

    SerDe.serFastMap(i2oAc, directBuf)
    bufSize += SerDe.serFastMapBufSize(i2oAc)
    assert(bufSize == (directBuf.writerIndex() - directBuf.readerIndex()))

    SerDe.serFastMap(i2oAs, directBuf)
    bufSize += SerDe.serFastMapBufSize(i2oAs)
    assert(bufSize == (directBuf.writerIndex() - directBuf.readerIndex()))

    SerDe.serFastMap(i2oAi, directBuf)
    bufSize += SerDe.serFastMapBufSize(i2oAi)
    assert(bufSize == (directBuf.writerIndex() - directBuf.readerIndex()))

    SerDe.serFastMap(i2oAl, directBuf)
    bufSize += SerDe.serFastMapBufSize(i2oAl)
    assert(bufSize == (directBuf.writerIndex() - directBuf.readerIndex()))

    SerDe.serFastMap(i2oAf, directBuf)
    bufSize += SerDe.serFastMapBufSize(i2oAf)
    assert(bufSize == (directBuf.writerIndex() - directBuf.readerIndex()))

    SerDe.serFastMap(i2oAd, directBuf)
    bufSize += SerDe.serFastMapBufSize(i2oAd)
    assert(bufSize == (directBuf.writerIndex() - directBuf.readerIndex()))

    SerDe.serFastMap(i2oStr, directBuf)
    bufSize += SerDe.serFastMapBufSize(i2oStr)
    assert(bufSize == (directBuf.writerIndex() - directBuf.readerIndex()))


    val i2oG_ = SerDe.fastMapFromBuffer[Int2ObjectOpenHashMap[ABC]](directBuf)
    val i2oS_ = SerDe.fastMapFromBuffer[Int2ObjectOpenHashMap[Serializable]](directBuf)
    val i2oV_ = SerDe.fastMapFromBuffer[Int2ObjectOpenHashMap[IntDummyVector]](directBuf)
    val i2oAbo_ = SerDe.fastMapFromBuffer[Int2ObjectOpenHashMap[Array[Boolean]]](directBuf)
    val i2oAby_ = SerDe.fastMapFromBuffer[Int2ObjectOpenHashMap[Array[Byte]]](directBuf)
    val i2oAc_ = SerDe.fastMapFromBuffer[Int2ObjectOpenHashMap[Array[Char]]](directBuf)
    val i2oAs_ = SerDe.fastMapFromBuffer[Int2ObjectOpenHashMap[Array[Short]]](directBuf)
    val i2oAi_ = SerDe.fastMapFromBuffer[Int2ObjectOpenHashMap[Array[Int]]](directBuf)
    val i2oAl_ = SerDe.fastMapFromBuffer[Int2ObjectOpenHashMap[Array[Long]]](directBuf)
    val i2oAf_ = SerDe.fastMapFromBuffer[Int2ObjectOpenHashMap[Array[Float]]](directBuf)
    val i2oAd_ = SerDe.fastMapFromBuffer[Int2ObjectOpenHashMap[Array[Double]]](directBuf)
    val i2oStr_ = SerDe.fastMapFromBuffer[Int2ObjectOpenHashMap[String]](directBuf)


    for (key <- i2oAbo.keySet().toIntArray) {
      assert(i2oAbo.get(key) sameElements i2oAbo_.get(key))
    }
    for (key <- i2oAby.keySet().toIntArray) {
      assert(i2oAby.get(key) sameElements i2oAby_.get(key))
    }
    for (key <- i2oAc.keySet().toIntArray) {
      assert(i2oAc.get(key) sameElements i2oAc_.get(key))
    }
    for (key <- i2oAs.keySet().toIntArray) {
      assert(i2oAs.get(key) sameElements i2oAs_.get(key))
    }
    for (key <- i2oAi.keySet().toIntArray) {
      assert(i2oAi.get(key) sameElements i2oAi_.get(key))
    }
    for (key <- i2oAl.keySet().toIntArray) {
      assert(i2oAl.get(key) sameElements i2oAl_.get(key))
    }
    for (key <- i2oAf.keySet().toIntArray) {
      assert(i2oAf.get(key) sameElements i2oAf_.get(key))
    }
    for (key <- i2oAd.keySet().toIntArray) {
      assert(i2oAd.get(key) sameElements i2oAd_.get(key))
    }
    for (key <- i2oStr.keySet().toIntArray) {
      assert(i2oStr.get(key) sameElements i2oStr_.get(key))
    }

    val l2oG = new Long2ObjectOpenHashMap[GData](10)
    val l2oS = new Long2ObjectOpenHashMap[Serializable](10)
    val l2oV = new Long2ObjectOpenHashMap[Vector](10)
    val l2oAbo = new Long2ObjectOpenHashMap[Array[Boolean]](10)
    val l2oAby = new Long2ObjectOpenHashMap[Array[Byte]](10)
    val l2oAc = new Long2ObjectOpenHashMap[Array[Char]](10)
    val l2oAs = new Long2ObjectOpenHashMap[Array[Short]](10)
    val l2oAi = new Long2ObjectOpenHashMap[Array[Int]](10)
    val l2oAl = new Long2ObjectOpenHashMap[Array[Long]](10)
    val l2oAf = new Long2ObjectOpenHashMap[Array[Float]](10)
    val l2oAd = new Long2ObjectOpenHashMap[Array[Double]](10)
    val l2oStr = new Long2ObjectOpenHashMap[String](10)

    (0 until rand.nextInt(5)).foreach { _ =>
      l2oG.put(rand.nextLong(), new ABC())
      l2oS.put(rand.nextLong(), new DEF(3.0))
      l2oS.put(rand.nextLong(), new GHI(4, 0.4f))
      l2oV.put(rand.nextLong(), new IntDummyVector(10, Array(1, 3, 5, 7)))
      l2oAbo.put(rand.nextLong(), Array.tabulate[Boolean](5)(_ => rand.nextBoolean()))
      l2oAby.put(rand.nextLong(), Array.tabulate[Byte](5)(_ => rand.nextInt().toByte))
      l2oAc.put(rand.nextLong(), Array.tabulate[Char](5)(_ => rand.nextPrintableChar()))
      l2oAs.put(rand.nextLong(), Array.tabulate[Short](5)(_ => rand.nextInt.toShort))
      l2oAi.put(rand.nextLong(), Array.tabulate[Int](5)(_ => rand.nextInt()))
      l2oAl.put(rand.nextLong(), Array.tabulate[Long](5)(_ => rand.nextLong()))
      l2oAf.put(rand.nextLong(), Array.tabulate[Float](5)(_ => rand.nextFloat()))
      l2oAd.put(rand.nextLong(), Array.tabulate[Double](5)(_ => rand.nextDouble()))
      l2oStr.put(rand.nextLong(), rand.nextString(7))
    }
    //
    //    println(SerDe.serFastMapBufSize(l2oG))
    ////    println(SerDe.serFastMapBufSize(l2oS))
    //    println(SerDe.serFastMapBufSize(l2oAbo))
    //    println(SerDe.serFastMapBufSize(l2oAby))
    //    println(SerDe.serFastMapBufSize(l2oAc))
    //    println(SerDe.serFastMapBufSize(l2oAs))
    //    println(SerDe.serFastMapBufSize(l2oAi))
    //    println(SerDe.serFastMapBufSize(l2oAl))
    //    println(SerDe.serFastMapBufSize(l2oAf))
    //    println(SerDe.serFastMapBufSize(l2oAd))
    //    println(SerDe.serFastMapBufSize(l2oStr))

    bufSize = 0
    SerDe.serFastMap(l2oG, directBuf)
    bufSize += SerDe.serFastMapBufSize(l2oG)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(l2oS, directBuf)
    //    bufSize += SerDe.serFastMapBufSize(l2oS)
    //    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())
    bufSize = directBuf.writerIndex() - directBuf.readerIndex()

    SerDe.serFastMap(l2oV, directBuf)
    bufSize += SerDe.serFastMapBufSize(l2oV)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(l2oAbo, directBuf)
    bufSize += SerDe.serFastMapBufSize(l2oAbo)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(l2oAby, directBuf)
    bufSize += SerDe.serFastMapBufSize(l2oAby)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(l2oAc, directBuf)
    bufSize += SerDe.serFastMapBufSize(l2oAc)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(l2oAs, directBuf)
    bufSize += SerDe.serFastMapBufSize(l2oAs)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(l2oAi, directBuf)
    bufSize += SerDe.serFastMapBufSize(l2oAi)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(l2oAl, directBuf)
    bufSize += SerDe.serFastMapBufSize(l2oAl)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(l2oAf, directBuf)
    bufSize += SerDe.serFastMapBufSize(l2oAf)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(l2oAd, directBuf)
    bufSize += SerDe.serFastMapBufSize(l2oAd)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(l2oStr, directBuf)
    bufSize += SerDe.serFastMapBufSize(l2oStr)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())


    val l2oG_ = SerDe.fastMapFromBuffer[Long2ObjectOpenHashMap[ABC]](directBuf)
    val l2oS_ = SerDe.fastMapFromBuffer[Long2ObjectOpenHashMap[Serializable]](directBuf)
    val l2oV_ = SerDe.fastMapFromBuffer[Long2ObjectOpenHashMap[IntDummyVector]](directBuf)
    val l2oAbo_ = SerDe.fastMapFromBuffer[Long2ObjectOpenHashMap[Array[Boolean]]](directBuf)
    val l2oAby_ = SerDe.fastMapFromBuffer[Long2ObjectOpenHashMap[Array[Byte]]](directBuf)
    val l2oAc_ = SerDe.fastMapFromBuffer[Long2ObjectOpenHashMap[Array[Char]]](directBuf)
    val l2oAs_ = SerDe.fastMapFromBuffer[Long2ObjectOpenHashMap[Array[Short]]](directBuf)
    val l2oAi_ = SerDe.fastMapFromBuffer[Long2ObjectOpenHashMap[Array[Int]]](directBuf)
    val l2oAl_ = SerDe.fastMapFromBuffer[Long2ObjectOpenHashMap[Array[Long]]](directBuf)
    val l2oAf_ = SerDe.fastMapFromBuffer[Long2ObjectOpenHashMap[Array[Float]]](directBuf)
    val l2oAd_ = SerDe.fastMapFromBuffer[Long2ObjectOpenHashMap[Array[Double]]](directBuf)
    val l2oStr_ = SerDe.fastMapFromBuffer[Long2ObjectOpenHashMap[String]](directBuf)

    for (key <- l2oAbo.keySet().toLongArray) {
      assert(l2oAbo.get(key) sameElements l2oAbo_.get(key))
    }
    for (key <- l2oAby.keySet().toLongArray) {
      assert(l2oAby.get(key) sameElements l2oAby_.get(key))
    }
    for (key <- l2oAc.keySet().toLongArray) {
      assert(l2oAc.get(key) sameElements l2oAc_.get(key))
    }
    for (key <- l2oAs.keySet().toLongArray) {
      assert(l2oAs.get(key) sameElements l2oAs_.get(key))
    }
    for (key <- l2oAi.keySet().toLongArray) {
      assert(l2oAi.get(key) sameElements l2oAi_.get(key))
    }
    for (key <- l2oAl.keySet().toLongArray) {
      assert(l2oAl.get(key) sameElements l2oAl_.get(key))
    }
    for (key <- l2oAf.keySet().toLongArray) {
      assert(l2oAf.get(key) sameElements l2oAf_.get(key))
    }
    for (key <- l2oAd.keySet().toLongArray) {
      assert(l2oAd.get(key) sameElements l2oAd_.get(key))
    }
    for (key <- l2oStr.keySet().toLongArray) {
      assert(l2oStr.get(key) sameElements l2oStr_.get(key))
    }
    println("test 05 OK")
  }

  test("06 Object FastHashMap Int") {
    val fasti2bo = new FastHashMap[Int, Boolean](10)
    val fasti2c = new FastHashMap[Int, Char](10)
    val fasti2by = new FastHashMap[Int, Byte](10)
    val fasti2s = new FastHashMap[Int, Short](10)
    val fasti2i = new FastHashMap[Int, Int](10)
    val fasti2l = new FastHashMap[Int, Long](10)
    val fasti2f = new FastHashMap[Int, Float](10)
    val fasti2d = new FastHashMap[Int, Double](10)

    val fasti2Abo = new FastHashMap[Int, Array[Boolean]](10)
    val fasti2Ac = new FastHashMap[Int, Array[Char]](10)
    val fasti2Aby = new FastHashMap[Int, Array[Byte]](10)
    val fasti2As = new FastHashMap[Int, Array[Short]](10)
    val fasti2Ai = new FastHashMap[Int, Array[Int]](10)
    val fasti2Al = new FastHashMap[Int, Array[Long]](10)
    val fasti2Af = new FastHashMap[Int, Array[Float]](10)
    val fasti2Ad = new FastHashMap[Int, Array[Double]](10)

    val fasti2Str = new FastHashMap[Int, String](10)

    val fasti2G = new FastHashMap[Int, GData](10)
    val fasti2S = new FastHashMap[Int, Serializable](10)

    val fasti2V = new FastHashMap[Int, Vector](10)

    (0 until rand.nextInt(10)).foreach { _ =>
      fasti2bo.put(rand.nextInt(), rand.nextBoolean())
      fasti2c.put(rand.nextInt(), rand.nextPrintableChar())
      fasti2by.put(rand.nextInt(), rand.nextInt().toByte)
      fasti2s.put(rand.nextInt(), rand.nextInt().toShort)
      fasti2i.put(rand.nextInt(), rand.nextInt())
      fasti2l.put(rand.nextInt(), rand.nextLong())
      fasti2f.put(rand.nextInt(), rand.nextFloat())
      fasti2d.put(rand.nextInt(), rand.nextDouble())

      fasti2Abo.put(rand.nextInt(), Array.tabulate[Boolean](5)(_ => rand.nextBoolean()))
      fasti2Ac.put(rand.nextInt(), Array.tabulate[Char](5)(_ => rand.nextPrintableChar()))
      fasti2Aby.put(rand.nextInt(), Array.tabulate[Byte](5)(_ => rand.nextInt().toByte))
      fasti2As.put(rand.nextInt(), Array.tabulate[Short](5)(_ => rand.nextInt().toShort))
      fasti2Ai.put(rand.nextInt(), Array.tabulate[Int](5)(_ => rand.nextInt()))
      fasti2Al.put(rand.nextInt(), Array.tabulate[Long](5)(_ => rand.nextLong()))
      fasti2Af.put(rand.nextInt(), Array.tabulate[Float](5)(_ => rand.nextFloat()))
      fasti2Ad.put(rand.nextInt(), Array.tabulate[Double](5)(_ => rand.nextDouble()))

      fasti2Str.put(rand.nextInt(), rand.nextString(10))

      fasti2G.put(rand.nextInt(), new ABC())
      fasti2S.put(rand.nextInt(), new DEF())

      fasti2V.put(rand.nextInt(), new IntDummyVector(10, Array(1, 2, 3, 7)))
    }

    //    println(SerDe.serFastMapBufSize(fasti2bo))
    //    println(SerDe.serFastMapBufSize(fasti2c))
    //    println(SerDe.serFastMapBufSize(fasti2by))
    //    println(SerDe.serFastMapBufSize(fasti2s))
    //    println(SerDe.serFastMapBufSize(fasti2i))
    //    println(SerDe.serFastMapBufSize(fasti2l))
    //    println(SerDe.serFastMapBufSize(fasti2f))
    //    println(SerDe.serFastMapBufSize(fasti2d))
    //
    //    println("fast Map array")
    //    println(SerDe.serFastMapBufSize(fasti2Abo))
    //    println(SerDe.serFastMapBufSize(fasti2Ac))
    //    println(SerDe.serFastMapBufSize(fasti2Aby))
    //    println(SerDe.serFastMapBufSize(fasti2As))
    //    println(SerDe.serFastMapBufSize(fasti2Ai))
    //    println(SerDe.serFastMapBufSize(fasti2Al))
    //    println(SerDe.serFastMapBufSize(fasti2Af))
    //    println(SerDe.serFastMapBufSize(fasti2Ad))
    //
    //    println(SerDe.serFastMapBufSize(fasti2Str))
    //
    //    println(SerDe.serFastMapBufSize(fasti2G))
    //    println(SerDe.serFastMapBufSize(fasti2S))
    //    println(SerDe.serFastMapBufSize(fasti2V))

    var bufSize = 0
    SerDe.serFastMap(fasti2bo, directBuf)
    bufSize += SerDe.serFastMapBufSize(fasti2bo)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fasti2c, directBuf)
    bufSize += SerDe.serFastMapBufSize(fasti2c)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fasti2by, directBuf)
    bufSize += SerDe.serFastMapBufSize(fasti2by)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fasti2s, directBuf)
    bufSize += SerDe.serFastMapBufSize(fasti2s)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fasti2i, directBuf)
    bufSize += SerDe.serFastMapBufSize(fasti2i)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fasti2l, directBuf)
    bufSize += SerDe.serFastMapBufSize(fasti2l)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fasti2f, directBuf)
    bufSize += SerDe.serFastMapBufSize(fasti2f)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fasti2d, directBuf)
    bufSize += SerDe.serFastMapBufSize(fasti2d)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fasti2Abo, directBuf)
    bufSize += SerDe.serFastMapBufSize(fasti2Abo)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fasti2Ac, directBuf)
    bufSize += SerDe.serFastMapBufSize(fasti2Ac)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fasti2Aby, directBuf)
    bufSize += SerDe.serFastMapBufSize(fasti2Aby)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fasti2As, directBuf)
    bufSize += SerDe.serFastMapBufSize(fasti2As)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fasti2Ai, directBuf)
    bufSize += SerDe.serFastMapBufSize(fasti2Ai)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fasti2Al, directBuf)
    bufSize += SerDe.serFastMapBufSize(fasti2Al)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fasti2Af, directBuf)
    bufSize += SerDe.serFastMapBufSize(fasti2Af)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fasti2Ad, directBuf)
    bufSize += SerDe.serFastMapBufSize(fasti2Ad)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fasti2Str, directBuf)
    bufSize += SerDe.serFastMapBufSize(fasti2Str)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())


    //    SerDe.serFastMap(fasti2G, directBuf)
    //    SerDe.serFastMap(fasti2S, directBuf)
    //    SerDe.serFastMap(fasti2V, directBuf)

    val fasti2bo_ = SerDe.fastMapFromBuffer[FastHashMap[Int, Boolean]](directBuf)
    val fasti2c_ = SerDe.fastMapFromBuffer[FastHashMap[Int, Char]](directBuf)
    val fasti2by_ = SerDe.fastMapFromBuffer[FastHashMap[Int, Byte]](directBuf)
    val fasti2s_ = SerDe.fastMapFromBuffer[FastHashMap[Int, Short]](directBuf)
    val fasti2i_ = SerDe.fastMapFromBuffer[FastHashMap[Int, Int]](directBuf)
    val fasti2l_ = SerDe.fastMapFromBuffer[FastHashMap[Int, Long]](directBuf)
    val fasti2f_ = SerDe.fastMapFromBuffer[FastHashMap[Int, Float]](directBuf)
    val fasti2d_ = SerDe.fastMapFromBuffer[FastHashMap[Int, Double]](directBuf)

    val fasti2Abo_ = SerDe.fastMapFromBuffer[FastHashMap[Int, Array[Boolean]]](directBuf)
    val fasti2Ac_ = SerDe.fastMapFromBuffer[FastHashMap[Int, Array[Char]]](directBuf)
    val fasti2Aby_ = SerDe.fastMapFromBuffer[FastHashMap[Int, Array[Byte]]](directBuf)
    val fasti2As_ = SerDe.fastMapFromBuffer[FastHashMap[Int, Array[Short]]](directBuf)
    val fasti2Ai_ = SerDe.fastMapFromBuffer[FastHashMap[Int, Array[Int]]](directBuf)
    val fasti2Al_ = SerDe.fastMapFromBuffer[FastHashMap[Int, Array[Long]]](directBuf)
    val fasti2Af_ = SerDe.fastMapFromBuffer[FastHashMap[Int, Array[Float]]](directBuf)
    val fasti2Ad_ = SerDe.fastMapFromBuffer[FastHashMap[Int, Array[Double]]](directBuf)

    val fasti2Str_ = SerDe.fastMapFromBuffer[FastHashMap[Int, String]](directBuf)

    for (idx <- fasti2bo.keyArray) {
      assert(fasti2bo.get(idx) == fasti2bo_.get(idx))
    }
    for (idx <- fasti2by.keyArray) {
      assert(fasti2by.get(idx) == fasti2by_.get(idx))
    }
    for (idx <- fasti2c.keyArray) {
      assert(fasti2c.get(idx) == fasti2c_.get(idx))
    }
    for (idx <- fasti2s.keyArray) {
      assert(fasti2s.get(idx) == fasti2s_.get(idx))
    }
    for (idx <- fasti2i.keyArray) {
      assert(fasti2i.get(idx) == fasti2i_.get(idx))
    }
    for (idx <- fasti2l.keyArray) {
      assert(fasti2l.get(idx) == fasti2l_.get(idx))
    }
    for (idx <- fasti2f.keyArray) {
      assert(fasti2f.get(idx) == fasti2f_.get(idx))
    }
    for (idx <- fasti2d.keyArray) {
      assert(fasti2d.get(idx) == fasti2d_.get(idx))
    }

    for (idx <- fasti2Abo.keyArray) {
      assert(fasti2Abo.get(idx) sameElements fasti2Abo_.get(idx))
    }
    for (idx <- fasti2Aby.keyArray) {
      assert(fasti2Aby.get(idx) sameElements fasti2Aby_.get(idx))
    }
    for (idx <- fasti2Ac.keyArray) {
      assert(fasti2Ac.get(idx) sameElements fasti2Ac_.get(idx))
    }
    for (idx <- fasti2As.keyArray) {
      assert(fasti2As.get(idx) sameElements fasti2As_.get(idx))
    }
    for (idx <- fasti2Ai.keyArray) {
      assert(fasti2Ai.get(idx) sameElements fasti2Ai_.get(idx))
    }
    for (idx <- fasti2Al.keyArray) {
      assert(fasti2Al.get(idx) sameElements fasti2Al_.get(idx))
    }
    for (idx <- fasti2Af.keyArray) {
      assert(fasti2Af.get(idx) sameElements fasti2Af_.get(idx))
    }
    for (idx <- fasti2Ad.keyArray) {
      assert(fasti2Ad.get(idx) sameElements fasti2Ad_.get(idx))
    }

    for (idx <- fasti2Str.keyArray) {
      assert(fasti2Str.get(idx) sameElements fasti2Str_.get(idx))
    }


    //    val fasti2G_ = SerDe.fastMapFromBuffer[FastHashMap[Int, ABC]](directBuf)
    //    val fasti2S_ = SerDe.fastMapFromBuffer[FastHashMap[Int, Serializable]](directBuf)
    //    val fasti2V_ = SerDe.fastMapFromBuffer[FastHashMap[Int, IntDummyVector]](directBuf)


    println("test 06 OK")

  }

  test("07 Object FastHashMap Long") {
    val fastl2bo = new FastHashMap[Long, Boolean](10)
    val fastl2c = new FastHashMap[Long, Char](10)
    val fastl2by = new FastHashMap[Long, Byte](10)
    val fastl2s = new FastHashMap[Long, Short](10)
    val fastl2i = new FastHashMap[Long, Int](10)
    val fastl2l = new FastHashMap[Long, Long](10)
    val fastl2f = new FastHashMap[Long, Float](10)
    val fastl2d = new FastHashMap[Long, Double](10)

    val fastl2Abo = new FastHashMap[Long, Array[Boolean]](10)
    val fastl2Ac = new FastHashMap[Long, Array[Char]](10)
    val fastl2Aby = new FastHashMap[Long, Array[Byte]](10)
    val fastl2As = new FastHashMap[Long, Array[Short]](10)
    val fastl2Ai = new FastHashMap[Long, Array[Int]](10)
    val fastl2Al = new FastHashMap[Long, Array[Long]](10)
    val fastl2Af = new FastHashMap[Long, Array[Float]](10)
    val fastl2Ad = new FastHashMap[Long, Array[Double]](10)

    val fastl2Str = new FastHashMap[Long, String](10)

    val fastl2G = new FastHashMap[Long, GData](10)
    val fastl2S = new FastHashMap[Long, Serializable](10)

    val fastl2V = new FastHashMap[Long, Vector](10)

    (0 until rand.nextInt(10)).foreach { _ =>
      fastl2bo.put(rand.nextLong(), rand.nextBoolean())
      fastl2c.put(rand.nextLong(), rand.nextPrintableChar())
      fastl2by.put(rand.nextLong(), rand.nextInt().toByte)
      fastl2s.put(rand.nextLong(), rand.nextInt().toShort)
      fastl2i.put(rand.nextLong(), rand.nextInt())
      fastl2l.put(rand.nextLong(), rand.nextLong())
      fastl2f.put(rand.nextLong(), rand.nextFloat())
      fastl2d.put(rand.nextLong(), rand.nextDouble())

      fastl2Abo.put(rand.nextLong(), Array.tabulate[Boolean](5)(_ => rand.nextBoolean()))
      fastl2Ac.put(rand.nextLong(), Array.tabulate[Char](5)(_ => rand.nextPrintableChar()))
      fastl2Aby.put(rand.nextLong(), Array.tabulate[Byte](5)(_ => rand.nextInt().toByte))
      fastl2As.put(rand.nextLong(), Array.tabulate[Short](5)(_ => rand.nextInt().toShort))
      fastl2Ai.put(rand.nextLong(), Array.tabulate[Int](5)(_ => rand.nextInt()))
      fastl2Al.put(rand.nextLong(), Array.tabulate[Long](5)(_ => rand.nextLong()))
      fastl2Af.put(rand.nextLong(), Array.tabulate[Float](5)(_ => rand.nextFloat()))
      fastl2Ad.put(rand.nextLong(), Array.tabulate[Double](5)(_ => rand.nextDouble()))

      fastl2Str.put(rand.nextLong(), rand.nextString(10))

      fastl2G.put(rand.nextLong(), new ABC())
      fastl2S.put(rand.nextLong(), new DEF())

      fastl2V.put(rand.nextLong(), new LongDummyVector(10, Array(1L, 2L, 3L, 7L)))
    }

    //    println(SerDe.serFastMapBufSize(fastl2bo))
    //    println(SerDe.serFastMapBufSize(fastl2c))
    //    println(SerDe.serFastMapBufSize(fastl2by))
    //    println(SerDe.serFastMapBufSize(fastl2s))
    //    println(SerDe.serFastMapBufSize(fastl2i))
    //    println(SerDe.serFastMapBufSize(fastl2l))
    //    println(SerDe.serFastMapBufSize(fastl2f))
    //    println(SerDe.serFastMapBufSize(fastl2d))
    //
    //    println("fastmap Array")
    //    println(SerDe.serFastMapBufSize(fastl2Abo))
    //    println(SerDe.serFastMapBufSize(fastl2Ac))
    //    println(SerDe.serFastMapBufSize(fastl2Aby))
    //    println(SerDe.serFastMapBufSize(fastl2As))
    //    println(SerDe.serFastMapBufSize(fastl2Ai))
    //    println(SerDe.serFastMapBufSize(fastl2Al))
    //    println(SerDe.serFastMapBufSize(fastl2Af))
    //    println(SerDe.serFastMapBufSize(fastl2Ad))
    //
    //    println(SerDe.serFastMapBufSize(fastl2Str))
    //
    //    println(SerDe.serFastMapBufSize(fastl2G))
    //    println(SerDe.serFastMapBufSize(fastl2S))
    //    println(SerDe.serFastMapBufSize(fastl2V))

    var bufSize = 0
    SerDe.serFastMap(fastl2bo, directBuf)
    bufSize += SerDe.serFastMapBufSize(fastl2bo)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fastl2c, directBuf)
    bufSize += SerDe.serFastMapBufSize(fastl2c)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fastl2by, directBuf)
    bufSize += SerDe.serFastMapBufSize(fastl2by)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fastl2s, directBuf)
    bufSize += SerDe.serFastMapBufSize(fastl2s)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fastl2i, directBuf)
    bufSize += SerDe.serFastMapBufSize(fastl2i)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fastl2l, directBuf)
    bufSize += SerDe.serFastMapBufSize(fastl2l)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fastl2f, directBuf)
    bufSize += SerDe.serFastMapBufSize(fastl2f)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fastl2d, directBuf)
    bufSize += SerDe.serFastMapBufSize(fastl2d)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fastl2Abo, directBuf)
    bufSize += SerDe.serFastMapBufSize(fastl2Abo)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fastl2Ac, directBuf)
    bufSize += SerDe.serFastMapBufSize(fastl2Ac)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fastl2Aby, directBuf)
    bufSize += SerDe.serFastMapBufSize(fastl2Aby)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fastl2As, directBuf)
    bufSize += SerDe.serFastMapBufSize(fastl2As)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fastl2Ai, directBuf)
    bufSize += SerDe.serFastMapBufSize(fastl2Ai)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fastl2Al, directBuf)
    bufSize += SerDe.serFastMapBufSize(fastl2Al)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fastl2Af, directBuf)
    bufSize += SerDe.serFastMapBufSize(fastl2Af)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fastl2Ad, directBuf)
    bufSize += SerDe.serFastMapBufSize(fastl2Ad)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fastl2Str, directBuf)
    bufSize += SerDe.serFastMapBufSize(fastl2Str)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    val fastl2bo_ = SerDe.fastMapFromBuffer[FastHashMap[Long, Boolean]](directBuf)
    val fastl2c_ = SerDe.fastMapFromBuffer[FastHashMap[Long, Char]](directBuf)
    val fastl2by_ = SerDe.fastMapFromBuffer[FastHashMap[Long, Byte]](directBuf)
    val fastl2s_ = SerDe.fastMapFromBuffer[FastHashMap[Long, Short]](directBuf)
    val fastl2i_ = SerDe.fastMapFromBuffer[FastHashMap[Long, Int]](directBuf)
    val fastl2l_ = SerDe.fastMapFromBuffer[FastHashMap[Long, Long]](directBuf)
    val fastl2f_ = SerDe.fastMapFromBuffer[FastHashMap[Long, Float]](directBuf)
    val fastl2d_ = SerDe.fastMapFromBuffer[FastHashMap[Long, Double]](directBuf)

    val fastl2Abo_ = SerDe.fastMapFromBuffer[FastHashMap[Long, Array[Boolean]]](directBuf)
    val fastl2Ac_ = SerDe.fastMapFromBuffer[FastHashMap[Long, Array[Char]]](directBuf)
    val fastl2Aby_ = SerDe.fastMapFromBuffer[FastHashMap[Long, Array[Byte]]](directBuf)
    val fastl2As_ = SerDe.fastMapFromBuffer[FastHashMap[Long, Array[Short]]](directBuf)
    val fastl2Ai_ = SerDe.fastMapFromBuffer[FastHashMap[Long, Array[Int]]](directBuf)
    val fastl2Al_ = SerDe.fastMapFromBuffer[FastHashMap[Long, Array[Long]]](directBuf)
    val fastl2Af_ = SerDe.fastMapFromBuffer[FastHashMap[Long, Array[Float]]](directBuf)
    val fastl2Ad_ = SerDe.fastMapFromBuffer[FastHashMap[Long, Array[Double]]](directBuf)

    val fastl2Str_ = SerDe.fastMapFromBuffer[FastHashMap[Long, String]](directBuf)

    for (idx <- fastl2bo.keyArray) {
      assert(fastl2bo.get(idx) == fastl2bo_.get(idx))
    }
    for (idx <- fastl2by.keyArray) {
      assert(fastl2by.get(idx) == fastl2by_.get(idx))
    }
    for (idx <- fastl2c.keyArray) {
      assert(fastl2c.get(idx) == fastl2c_.get(idx))
    }
    for (idx <- fastl2s.keyArray) {
      assert(fastl2s.get(idx) == fastl2s_.get(idx))
    }
    for (idx <- fastl2i.keyArray) {
      assert(fastl2i.get(idx) == fastl2i_.get(idx))
    }
    for (idx <- fastl2l.keyArray) {
      assert(fastl2l.get(idx) == fastl2l_.get(idx))
    }
    for (idx <- fastl2f.keyArray) {
      assert(fastl2f.get(idx) == fastl2f_.get(idx))
    }
    for (idx <- fastl2d.keyArray) {
      assert(fastl2d.get(idx) == fastl2d_.get(idx))
    }

    for (idx <- fastl2Abo.keyArray) {
      assert(fastl2Abo.get(idx) sameElements fastl2Abo_.get(idx))
    }
    for (idx <- fastl2Aby.keyArray) {
      assert(fastl2Aby.get(idx) sameElements fastl2Aby_.get(idx))
    }
    for (idx <- fastl2Ac.keyArray) {
      assert(fastl2Ac.get(idx) sameElements fastl2Ac_.get(idx))
    }
    for (idx <- fastl2As.keyArray) {
      assert(fastl2As.get(idx) sameElements fastl2As_.get(idx))
    }
    for (idx <- fastl2Ai.keyArray) {
      assert(fastl2Ai.get(idx) sameElements fastl2Ai_.get(idx))
    }
    for (idx <- fastl2Al.keyArray) {
      assert(fastl2Al.get(idx) sameElements fastl2Al_.get(idx))
    }
    for (idx <- fastl2Af.keyArray) {
      assert(fastl2Af.get(idx) sameElements fastl2Af_.get(idx))
    }
    for (idx <- fastl2Ad.keyArray) {
      assert(fastl2Ad.get(idx) sameElements fastl2Ad_.get(idx))
    }

    for (idx <- fastl2Str.keyArray) {
      assert(fastl2Str.get(idx) sameElements fastl2Str_.get(idx))
    }


    //    val fasti2G_ = SerDe.fastMapFromBuffer[FastHashMap[Int, ABC]](directBuf)
    //    val fasti2S_ = SerDe.fastMapFromBuffer[FastHashMap[Int, Serializable]](directBuf)
    //    val fasti2V_ = SerDe.fastMapFromBuffer[FastHashMap[Int, IntDummyVector]](directBuf)


    println("test 07 OK")
  }

  test("08 Object FastHashMap GSV") {
    val fasti2G = new FastHashMap[Int, GData](10)
    val fasti2S = new FastHashMap[Int, Serializable](10)
    val fasti2V = new FastHashMap[Int, Vector](10)

    val fastl2G = new FastHashMap[Long, GData](10)
    val fastl2S = new FastHashMap[Long, Serializable](10)
    val fastl2V = new FastHashMap[Long, Vector](10)

    (0 until rand.nextInt(10)).foreach { _ =>
      fasti2G.put(rand.nextInt(), new ABC())
      fasti2S.put(rand.nextInt(), new DEF())
      fasti2V.put(rand.nextInt(), new IntDummyVector(10, Array(1, 3, 5, 7)))

      fastl2G.put(rand.nextLong(), new ABC())
      fastl2S.put(rand.nextLong(), new DEF())
      fastl2V.put(rand.nextLong(), new IntDummyVector(10, Array(1, 3, 5, 7)))
    }

    //    SerDe.serFastMapBufSize(fasti2G)
    //    SerDe.serFastMapBufSize(fasti2V)
    //    SerDe.serFastMapBufSize(fasti2S)
    //    SerDe.serFastMapBufSize(fastl2G)
    //    SerDe.serFastMapBufSize(fastl2V)
    //    SerDe.serFastMapBufSize(fastl2S)
    var bufSize = 0
    SerDe.serFastMap(fasti2G, directBuf)
    bufSize += SerDe.serFastMapBufSize(fasti2G)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fasti2V, directBuf)
    bufSize += SerDe.serFastMapBufSize(fasti2V)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fasti2S, directBuf)
    bufSize += SerDe.serFastMapBufSize(fasti2S)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fastl2G, directBuf)
    bufSize += SerDe.serFastMapBufSize(fastl2G)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fastl2V, directBuf)
    bufSize += SerDe.serFastMapBufSize(fastl2V)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fastl2S, directBuf)
    bufSize += SerDe.serFastMapBufSize(fastl2S)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    val fasti2G_ = SerDe.fastMapFromBuffer[FastHashMap[Int, ABC]](directBuf)
    val fasti2V_ = SerDe.fastMapFromBuffer[FastHashMap[Int, Vector]](directBuf)
    val fasti2S_ = SerDe.fastMapFromBuffer[FastHashMap[Int, Serializable]](directBuf)
    val fastl2G_ = SerDe.fastMapFromBuffer[FastHashMap[Long, ABC]](directBuf)
    val fastl2V_ = SerDe.fastMapFromBuffer[FastHashMap[Long, Vector]](directBuf)
    val fastl2S_ = SerDe.fastMapFromBuffer[FastHashMap[Long, Serializable]](directBuf)

    println("test 08 OK")
  }

  // map with start end pos
  test("09 map with start end") {
    // 1. create data
    val fastp = new FastHashMap[Int, Int](10)
    val i2bo = new Int2BooleanOpenHashMap(10)
    val i2by = new Int2ByteOpenHashMap(10)
    val i2c = new Int2CharOpenHashMap(10)
    val i2s = new Int2ShortOpenHashMap(10)
    val i2i = new Int2IntOpenHashMap(10)
    val i2l = new Int2LongOpenHashMap(10)
    val i2f = new Int2FloatOpenHashMap(10)
    val i2d = new Int2DoubleOpenHashMap(10)
    val i2o = new Int2ObjectOpenHashMap[Array[Long]](10)

    val intKeys = new ArrayBuffer[Int]()
    (0 until 10).foreach { _ =>
      val key = rand.nextInt()
      intKeys += key
      fastp.put(key, rand.nextInt())
      i2bo.put(key, rand.nextBoolean())
      i2by.put(key, rand.nextPrintableChar().toByte)
      i2c.put(key, rand.nextPrintableChar())
      i2s.put(key, rand.nextInt().toShort)
      i2i.put(key, rand.nextInt())
      i2l.put(key, rand.nextLong())
      i2f.put(key, rand.nextFloat())
      i2d.put(key, rand.nextDouble())
      i2o.put(key, Array.tabulate[Long](5)(_ => rand.nextLong()))

    }

    val l2bo = new Long2BooleanOpenHashMap(10)
    val l2by = new Long2ByteOpenHashMap(10)
    val l2c = new Long2CharOpenHashMap(10)
    val l2s = new Long2ShortOpenHashMap(10)
    val l2i = new Long2IntOpenHashMap(10)
    val l2l = new Long2LongOpenHashMap(10)
    val l2f = new Long2FloatOpenHashMap(10)
    val l2d = new Long2DoubleOpenHashMap(10)
    val l2o = new Long2ObjectOpenHashMap[Array[Double]](10)

    val longKeys = new ArrayBuffer[Long]()
    (0 until 10).foreach { _ =>
      val key = rand.nextLong()
      longKeys += key
      l2bo.put(key, rand.nextBoolean())
      l2by.put(key, rand.nextInt().toByte)
      l2c.put(key, rand.nextPrintableChar())
      l2s.put(key, rand.nextInt().toShort)
      l2i.put(key, rand.nextInt())
      l2l.put(key, rand.nextLong())
      l2f.put(key, rand.nextFloat())
      l2d.put(key, rand.nextDouble())
      l2o.put(key, Array.tabulate[Double](5)(_ => rand.nextDouble()))
    }

    // 2. deserialize
    //    println("serFastMap with start end")
    var bufSize = 0
    SerDe.serFastMap(fastp, intKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(fastp, intKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(i2bo, intKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(i2bo, intKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(i2by, intKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(i2by, intKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(i2c, intKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(i2c, intKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(i2s, intKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(i2s, intKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(i2i, intKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(i2i, intKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(i2l, intKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(i2l, intKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(i2f, intKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(i2f, intKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(i2d, intKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(i2d, intKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(i2o, intKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(i2o, intKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(l2bo, longKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(l2bo, longKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(l2by, longKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(l2by, longKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(l2c, longKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(l2c, longKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(l2s, longKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(l2s, longKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(l2i, longKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(l2i, longKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(l2l, longKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(l2l, longKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(l2f, longKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(l2f, longKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(l2d, longKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(l2d, longKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(l2o, longKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(l2o, longKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())


    // 3. serialize
    //    println("fastMapFromBuffer")
    val fastp_ = SerDe.fastMapFromBuffer[FastHashMap[Int, Int]](directBuf)
    val i2bo_ = SerDe.fastMapFromBuffer[Int2BooleanOpenHashMap](directBuf)
    val i2by_  = SerDe.fastMapFromBuffer[Int2ByteOpenHashMap](directBuf)
    val i2c_ = SerDe.fastMapFromBuffer[Int2CharOpenHashMap](directBuf)
    val i2s_ = SerDe.fastMapFromBuffer[Int2ShortOpenHashMap](directBuf)
    val i2i_ = SerDe.fastMapFromBuffer[Int2IntOpenHashMap](directBuf)
    val i2l_ = SerDe.fastMapFromBuffer[Int2LongOpenHashMap](directBuf)
    val i2f_ = SerDe.fastMapFromBuffer[Int2FloatOpenHashMap](directBuf)
    val i2d_ = SerDe.fastMapFromBuffer[Int2DoubleOpenHashMap](directBuf)
    val i2o_ = SerDe.fastMapFromBuffer[Int2ObjectOpenHashMap[Array[Long]]](directBuf)

    val l2bo_ = SerDe.fastMapFromBuffer[Long2BooleanOpenHashMap](directBuf)
    val l2by_ = SerDe.fastMapFromBuffer[Long2ByteOpenHashMap](directBuf)
    val l2c_ = SerDe.fastMapFromBuffer[Long2CharOpenHashMap](directBuf)
    val l2s_ = SerDe.fastMapFromBuffer[Long2ShortOpenHashMap](directBuf)
    val l2i_ = SerDe.fastMapFromBuffer[Long2IntOpenHashMap](directBuf)
    val l2l_ = SerDe.fastMapFromBuffer[Long2LongOpenHashMap](directBuf)
    val l2f_ = SerDe.fastMapFromBuffer[Long2FloatOpenHashMap](directBuf)
    val l2d_ = SerDe.fastMapFromBuffer[Long2DoubleOpenHashMap](directBuf)
    val l2o_ = SerDe.fastMapFromBuffer[Long2ObjectOpenHashMap[Array[Double]]](directBuf)

    for (key <- fastp_.keyArray) {
      assert(fastp.get(key) == fastp_.get(key))
    }

    for (key <- i2bo_.keySet().toIntArray) {
      assert(i2bo.get(key) == i2bo_.get(key))
    }
    for (key <- i2by_.keySet().toIntArray) {
      assert(i2by.get(key) == i2by_.get(key))
    }
    for (key <- i2c_.keySet().toIntArray) {
      assert(i2c.get(key) == i2c_.get(key))
    }
    for (key <- i2s_.keySet().toIntArray) {
      assert(i2s.get(key) == i2s_.get(key))
    }
    for (key <- i2i_.keySet().toIntArray) {
      assert(i2i.get(key) == i2i_.get(key))
    }
    for (key <- i2l_.keySet().toIntArray) {
      assert(i2l.get(key) == i2l_.get(key))
    }
    for (key <- i2f_.keySet().toIntArray) {
      assert(i2f.get(key) == i2f_.get(key))
    }
    for (key <- i2d_.keySet().toIntArray) {
      assert(i2d.get(key) == i2d_.get(key))
    }

    for (key <- l2bo_.keySet().toLongArray) {
      assert(l2bo.get(key) == l2bo_.get(key))
    }
    for (key <- l2by_.keySet().toLongArray) {
      assert(l2by.get(key) == l2by_.get(key))
    }
    for (key <- l2c_.keySet().toLongArray) {
      assert(l2c.get(key) == l2c_.get(key))
    }
    for (key <- l2s_.keySet().toLongArray) {
      assert(l2s.get(key) == l2s_.get(key))
    }
    for (key <- l2i_.keySet().toLongArray) {
      assert(l2i.get(key) == l2i_.get(key))
    }
    for (key <- l2l_.keySet().toLongArray) {
      assert(l2l.get(key) == l2l_.get(key))
    }
    for (key <- l2f_.keySet().toLongArray) {
      assert(l2f.get(key) == l2f_.get(key))
    }
    for (key <- l2d_.keySet().toLongArray) {
      assert(l2d.get(key) == l2d_.get(key))
    }

    //    println(SerDe.serFastMapBufSize(fastp, intKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(i2bo, intKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(i2by, intKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(i2c, intKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(i2s, intKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(i2i, intKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(i2l, intKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(i2f, intKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(i2d, intKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(i2o, intKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(l2bo, longKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(l2by, longKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(l2c, longKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(l2s, longKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(l2i, longKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(l2l, longKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(l2f, longKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(l2d, longKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(l2o, longKeys.toArray, 3, 7))

    println("test 09 OK")
  }

  test("10 fastMap with object start end") {
    val i2oG = new Int2ObjectOpenHashMap[GData](10)
    val i2oS = new Int2ObjectOpenHashMap[Serializable](10)
    val i2oV = new Int2ObjectOpenHashMap[Vector](10)
    val i2oAbo = new Int2ObjectOpenHashMap[Array[Boolean]](10)
    val i2oAby = new Int2ObjectOpenHashMap[Array[Byte]](10)
    val i2oAc = new Int2ObjectOpenHashMap[Array[Char]](10)
    val i2oAs = new Int2ObjectOpenHashMap[Array[Short]](10)
    val i2oAi = new Int2ObjectOpenHashMap[Array[Int]](10)
    val i2oAl = new Int2ObjectOpenHashMap[Array[Long]](10)
    val i2oAf = new Int2ObjectOpenHashMap[Array[Float]](10)
    val i2oAd = new Int2ObjectOpenHashMap[Array[Double]](10)
    val i2oStr = new Int2ObjectOpenHashMap[String](10)

    var intKeys = new ArrayBuffer[Int]()
    (0 until 10).foreach { _ =>
      val key = rand.nextInt()
      intKeys += key
      i2oG.put(key, new ABC())
      i2oS.put(key, new DEF())
      i2oV.put(key, new IntDummyVector(10, Array(1, 3, 5, 7)))
      i2oAbo.put(key, Array.tabulate[Boolean](5)(_ => rand.nextBoolean()))
      i2oAby.put(key, Array.tabulate[Byte](5)(_ => rand.nextInt().toByte))
      i2oAc.put(key, Array.tabulate[Char](5)(_ => rand.nextPrintableChar()))
      i2oAs.put(key, Array.tabulate[Short](5)(_ => rand.nextInt.toShort))
      i2oAi.put(key, Array.tabulate[Int](5)(_ => rand.nextInt()))
      i2oAl.put(key, Array.tabulate[Long](5)(_ => rand.nextLong()))
      i2oAf.put(key, Array.tabulate[Float](5)(_ => rand.nextFloat()))
      i2oAd.put(key, Array.tabulate[Double](5)(_ => rand.nextDouble()))
      i2oStr.put(key, rand.nextString(7))
    }
    //    println(SerDe.serFastMapBufSize(i2oG, intKeys.toArray, 3, 7))
    //    //    println(SerDe.serFastMapBufSize(i2oS))
    //    println(SerDe.serFastMapBufSize(i2oAbo, intKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(i2oAby, intKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(i2oAc, intKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(i2oAs, intKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(i2oAi, intKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(i2oAl, intKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(i2oAf, intKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(i2oAd, intKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(i2oStr, intKeys.toArray, 3, 7))
    //
    var bufSize = 0
    SerDe.serFastMap(i2oG, intKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(i2oG, intKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    //    SerDe.serFastMap(i2oS, intKeys.toArray, 3, 7, directBuf)

    SerDe.serFastMap(i2oV, intKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(i2oV, intKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(i2oAbo, intKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(i2oAbo, intKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(i2oAby, intKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(i2oAby, intKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(i2oAc, intKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(i2oAc, intKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(i2oAs, intKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(i2oAs, intKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(i2oAi, intKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(i2oAi, intKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(i2oAl, intKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(i2oAl, intKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(i2oAf, intKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(i2oAf, intKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(i2oAd, intKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(i2oAd, intKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(i2oStr, intKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(i2oStr, intKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())


    val i2oG_ = SerDe.fastMapFromBuffer[Int2ObjectOpenHashMap[ABC]](directBuf)
    //    SerDe.fastMapFromBuffer[Int2ObjectOpenHashMap[Serializable]](directBuf)
    val i2oV_ = SerDe.fastMapFromBuffer[Int2ObjectOpenHashMap[IntDummyVector]](directBuf)
    val i2oAbo_ = SerDe.fastMapFromBuffer[Int2ObjectOpenHashMap[Array[Boolean]]](directBuf)
    val i2oAby_ = SerDe.fastMapFromBuffer[Int2ObjectOpenHashMap[Array[Byte]]](directBuf)
    val i2oAc_ = SerDe.fastMapFromBuffer[Int2ObjectOpenHashMap[Array[Char]]](directBuf)
    val i2oAs_ = SerDe.fastMapFromBuffer[Int2ObjectOpenHashMap[Array[Short]]](directBuf)
    val i2oAi_ = SerDe.fastMapFromBuffer[Int2ObjectOpenHashMap[Array[Int]]](directBuf)
    val i2oAl_ = SerDe.fastMapFromBuffer[Int2ObjectOpenHashMap[Array[Long]]](directBuf)
    val i2oAf_ = SerDe.fastMapFromBuffer[Int2ObjectOpenHashMap[Array[Float]]](directBuf)
    val i2oAd_ = SerDe.fastMapFromBuffer[Int2ObjectOpenHashMap[Array[Double]]](directBuf)
    val i2oStr_ = SerDe.fastMapFromBuffer[Int2ObjectOpenHashMap[String]](directBuf)

    for (idx <- (3 until 7)) {
      assert(i2oAbo.get(intKeys(idx)) sameElements  i2oAbo_.get(intKeys(idx)))
    }
    for (idx <- (3 until 7)) {
      assert(i2oAby.get(intKeys(idx)) sameElements  i2oAby_.get(intKeys(idx)))
    }
    for (idx <- (3 until 7)) {
      assert(i2oAc.get(intKeys(idx)) sameElements  i2oAc_.get(intKeys(idx)))
    }
    for (idx <- (3 until 7)) {
      assert(i2oAs.get(intKeys(idx)) sameElements  i2oAs_.get(intKeys(idx)))
    }
    for (idx <- (3 until 7)) {
      assert(i2oAi.get(intKeys(idx)) sameElements  i2oAi_.get(intKeys(idx)))
    }
    for (idx <- (3 until 7)) {
      assert(i2oAl.get(intKeys(idx)) sameElements  i2oAl_.get(intKeys(idx)))
    }
    for (idx <- (3 until 7)) {
      assert(i2oAf.get(intKeys(idx)) sameElements  i2oAf_.get(intKeys(idx)))
    }
    for (idx <- (3 until 7)) {
      assert(i2oAd.get(intKeys(idx)) sameElements  i2oAd_.get(intKeys(idx)))
    }
    for (idx <- (3 until 7)) {
      assert(i2oStr.get(intKeys(idx)) sameElements  i2oStr_.get(intKeys(idx)))
    }

    val l2oG = new Long2ObjectOpenHashMap[GData](10)
    val l2oS = new Long2ObjectOpenHashMap[Serializable](10)
    val l2oV = new Long2ObjectOpenHashMap[Vector](10)
    val l2oAbo = new Long2ObjectOpenHashMap[Array[Boolean]](10)
    val l2oAby = new Long2ObjectOpenHashMap[Array[Byte]](10)
    val l2oAc = new Long2ObjectOpenHashMap[Array[Char]](10)
    val l2oAs = new Long2ObjectOpenHashMap[Array[Short]](10)
    val l2oAi = new Long2ObjectOpenHashMap[Array[Int]](10)
    val l2oAl = new Long2ObjectOpenHashMap[Array[Long]](10)
    val l2oAf = new Long2ObjectOpenHashMap[Array[Float]](10)
    val l2oAd = new Long2ObjectOpenHashMap[Array[Double]](10)
    val l2oStr = new Long2ObjectOpenHashMap[String](10)

    val longKeys = new ArrayBuffer[Long]()
    (0 until 10).foreach { _ =>
      val key = rand.nextLong()
      longKeys += key
      l2oG.put(key, new ABC())
      l2oS.put(key, new GHI(4, 0.4f))
      l2oV.put(key, new IntDummyVector(10, Array(1, 3, 5 ,7)))
      l2oAbo.put(key, Array.tabulate[Boolean](5)(_ => rand.nextBoolean()))
      l2oAby.put(key, Array.tabulate[Byte](5)(_ => rand.nextInt().toByte))
      l2oAc.put(key, Array.tabulate[Char](5)(_ => rand.nextPrintableChar()))
      l2oAs.put(key, Array.tabulate[Short](5)(_ => rand.nextInt.toShort))
      l2oAi.put(key, Array.tabulate[Int](5)(_ => rand.nextInt()))
      l2oAl.put(key, Array.tabulate[Long](5)(_ => rand.nextLong()))
      l2oAf.put(key, Array.tabulate[Float](5)(_ => rand.nextFloat()))
      l2oAd.put(key, Array.tabulate[Double](5)(_ => rand.nextDouble()))
      l2oStr.put(key, rand.nextString(7))
    }

    //    println(SerDe.serFastMapBufSize(l2oG, longKeys.toArray, 3, 7))
    //    //    println(SerDe.serFastMapBufSize(l2oS))
    //    println(SerDe.serFastMapBufSize(l2oAbo, longKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(l2oAby, longKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(l2oAc, longKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(l2oAs, longKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(l2oAi, longKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(l2oAl, longKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(l2oAf, longKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(l2oAd, longKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(l2oStr, longKeys.toArray, 3, 7))
    //

    bufSize = 0
    SerDe.serFastMap(l2oG, longKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(l2oG, longKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    //    SerDe.serFastMap(l2oS, longKeys.toArray, 3, 7, directBuf)

    SerDe.serFastMap(l2oV, longKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(l2oV, longKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(l2oAbo, longKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(l2oAbo, longKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(l2oAby, longKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(l2oAby, longKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(l2oAc, longKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(l2oAc, longKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(l2oAs, longKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(l2oAs, longKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(l2oAi, longKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(l2oAi, longKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(l2oAl, longKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(l2oAl, longKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(l2oAf, longKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(l2oAf, longKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(l2oAd, longKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(l2oAd, longKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(l2oStr, longKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(l2oStr, longKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())


    val l2oG_ = SerDe.fastMapFromBuffer[Long2ObjectOpenHashMap[ABC]](directBuf)
    //    SerDe.fastMapFromBuffer[Long2ObjectOpenHashMap[Serializable]](directBuf)
    val l2oV_ = SerDe.fastMapFromBuffer[Long2ObjectOpenHashMap[IntDummyVector]](directBuf)
    val l2oAbo_ = SerDe.fastMapFromBuffer[Long2ObjectOpenHashMap[Array[Boolean]]](directBuf)
    val l2oAby_ = SerDe.fastMapFromBuffer[Long2ObjectOpenHashMap[Array[Byte]]](directBuf)
    val l2oAc_ = SerDe.fastMapFromBuffer[Long2ObjectOpenHashMap[Array[Char]]](directBuf)
    val l2oAs_ = SerDe.fastMapFromBuffer[Long2ObjectOpenHashMap[Array[Short]]](directBuf)
    val l2oAi_ = SerDe.fastMapFromBuffer[Long2ObjectOpenHashMap[Array[Int]]](directBuf)
    val l2oAl_ = SerDe.fastMapFromBuffer[Long2ObjectOpenHashMap[Array[Long]]](directBuf)
    val l2oAf_ = SerDe.fastMapFromBuffer[Long2ObjectOpenHashMap[Array[Float]]](directBuf)
    val l2oAd_ = SerDe.fastMapFromBuffer[Long2ObjectOpenHashMap[Array[Double]]](directBuf)
    val l2oStr_ = SerDe.fastMapFromBuffer[Long2ObjectOpenHashMap[String]](directBuf)


    for (idx <- (3 until 7)) {
      assert(l2oAbo.get(longKeys(idx)) sameElements  l2oAbo_.get(longKeys(idx)))
    }
    for (idx <- (3 until 7)) {
      assert(l2oAby.get(longKeys(idx)) sameElements  l2oAby_.get(longKeys(idx)))
    }
    for (idx <- (3 until 7)) {
      assert(l2oAc.get(longKeys(idx)) sameElements  l2oAc_.get(longKeys(idx)))
    }
    for (idx <- (3 until 7)) {
      assert(l2oAs.get(longKeys(idx)) sameElements  l2oAs_.get(longKeys(idx)))
    }
    for (idx <- (3 until 7)) {
      assert(l2oAi.get(longKeys(idx)) sameElements  l2oAi_.get(longKeys(idx)))
    }
    for (idx <- (3 until 7)) {
      assert(l2oAl.get(longKeys(idx)) sameElements  l2oAl_.get(longKeys(idx)))
    }
    for (idx <- (3 until 7)) {
      assert(l2oAf.get(longKeys(idx)) sameElements  l2oAf_.get(longKeys(idx)))
    }
    for (idx <- (3 until 7)) {
      assert(l2oAd.get(longKeys(idx)) sameElements  l2oAd_.get(longKeys(idx)))
    }
    for (idx <- (3 until 7)) {
      assert(l2oStr.get(longKeys(idx)) sameElements  l2oStr_.get(longKeys(idx)))
    }

    println("test 10 OK")
  }

  test("11 Object FastHashMap Int start end") {
    val fasti2bo = new FastHashMap[Int, Boolean](10)
    val fasti2c = new FastHashMap[Int, Char](10)
    val fasti2by = new FastHashMap[Int, Byte](10)
    val fasti2s = new FastHashMap[Int, Short](10)
    val fasti2i = new FastHashMap[Int, Int](10)
    val fasti2l = new FastHashMap[Int, Long](10)
    val fasti2f = new FastHashMap[Int, Float](10)
    val fasti2d = new FastHashMap[Int, Double](10)

    val fasti2Abo = new FastHashMap[Int, Array[Boolean]](10)
    val fasti2Ac = new FastHashMap[Int, Array[Char]](10)
    val fasti2Aby = new FastHashMap[Int, Array[Byte]](10)
    val fasti2As = new FastHashMap[Int, Array[Short]](10)
    val fasti2Ai = new FastHashMap[Int, Array[Int]](10)
    val fasti2Al = new FastHashMap[Int, Array[Long]](10)
    val fasti2Af = new FastHashMap[Int, Array[Float]](10)
    val fasti2Ad = new FastHashMap[Int, Array[Double]](10)

    val fasti2Str = new FastHashMap[Int, String](10)

    val fasti2G = new FastHashMap[Int, GData](10)
    val fasti2S = new FastHashMap[Int, Serializable](10)

    val fasti2V = new FastHashMap[Int, Vector](10)

    val intKeys = new ArrayBuffer[Int]()
    (0 until 10).foreach { _ =>
      val key = rand.nextInt()
      intKeys += key
      fasti2bo.put(key, rand.nextBoolean())
      fasti2c.put(key, rand.nextPrintableChar())
      fasti2by.put(key, rand.nextInt().toByte)
      fasti2s.put(key, rand.nextInt().toShort)
      fasti2i.put(key, rand.nextInt())
      fasti2l.put(key, rand.nextLong())
      fasti2f.put(key, rand.nextFloat())
      fasti2d.put(key, rand.nextDouble())

      fasti2Abo.put(key, Array.tabulate[Boolean](5)(_ => rand.nextBoolean()))
      fasti2Ac.put(key, Array.tabulate[Char](5)(_ => rand.nextPrintableChar()))
      fasti2Aby.put(key, Array.tabulate[Byte](5)(_ => rand.nextInt().toByte))
      fasti2As.put(key, Array.tabulate[Short](5)(_ => rand.nextInt().toShort))
      fasti2Ai.put(key, Array.tabulate[Int](5)(_ => rand.nextInt()))
      fasti2Al.put(key, Array.tabulate[Long](5)(_ => rand.nextLong()))
      fasti2Af.put(key, Array.tabulate[Float](5)(_ => rand.nextFloat()))
      fasti2Ad.put(key, Array.tabulate[Double](5)(_ => rand.nextDouble()))

      fasti2Str.put(key, rand.nextString(10))

      fasti2G.put(key, new ABC())
      fasti2S.put(key, new DEF())

      fasti2V.put(key, new IntDummyVector(10, Array(1, 2, 3, 7)))
    }

    //    println(SerDe.serFastMapBufSize(fasti2bo, intKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(fasti2c, intKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(fasti2by, intKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(fasti2s, intKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(fasti2i, intKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(fasti2l, intKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(fasti2f, intKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(fasti2d, intKeys.toArray, 3, 7))
    //
    //    println("fast Map array")
    //    println(SerDe.serFastMapBufSize(fasti2Abo, intKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(fasti2Ac, intKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(fasti2Aby, intKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(fasti2As, intKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(fasti2Ai, intKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(fasti2Al, intKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(fasti2Af, intKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(fasti2Ad, intKeys.toArray, 3, 7))
    //
    //    println(SerDe.serFastMapBufSize(fasti2Str, intKeys.toArray, 3, 7))
    //
    //    println(SerDe.serFastMapBufSize(fasti2G, intKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(fasti2S, intKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(fasti2V, intKeys.toArray, 3, 7))
    //
    var bufSize = 0
    SerDe.serFastMap(fasti2bo, intKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(fasti2bo, intKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fasti2c, intKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(fasti2c, intKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fasti2by, intKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(fasti2by, intKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fasti2s, intKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(fasti2s, intKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fasti2i, intKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(fasti2i, intKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fasti2l, intKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(fasti2l, intKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fasti2f, intKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(fasti2f, intKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fasti2d, intKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(fasti2d, intKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())


    SerDe.serFastMap(fasti2Abo, intKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(fasti2Abo, intKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fasti2Ac, intKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(fasti2Ac, intKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fasti2Aby, intKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(fasti2Aby, intKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fasti2As, intKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(fasti2As, intKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fasti2Ai, intKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(fasti2Ai, intKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fasti2Al, intKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(fasti2Al, intKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fasti2Af, intKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(fasti2Af, intKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fasti2Ad, intKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(fasti2Ad, intKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fasti2Str, intKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(fasti2Str, intKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    //    SerDe.serFastMap(fasti2G, directBuf)
    //    SerDe.serFastMap(fasti2S, directBuf)
    //    SerDe.serFastMap(fasti2V, directBuf)

    val fasti2bo_ = SerDe.fastMapFromBuffer[FastHashMap[Int, Boolean]](directBuf)
    val fasti2c_ = SerDe.fastMapFromBuffer[FastHashMap[Int, Char]](directBuf)
    val fasti2by_ = SerDe.fastMapFromBuffer[FastHashMap[Int, Byte]](directBuf)
    val fasti2s_ = SerDe.fastMapFromBuffer[FastHashMap[Int, Short]](directBuf)
    val fasti2i_ = SerDe.fastMapFromBuffer[FastHashMap[Int, Int]](directBuf)
    val fasti2l_ = SerDe.fastMapFromBuffer[FastHashMap[Int, Long]](directBuf)
    val fasti2f_ = SerDe.fastMapFromBuffer[FastHashMap[Int, Float]](directBuf)
    val fasti2d_ = SerDe.fastMapFromBuffer[FastHashMap[Int, Double]](directBuf)

    val fasti2Abo_ = SerDe.fastMapFromBuffer[FastHashMap[Int, Array[Boolean]]](directBuf)
    val fasti2Ac_ = SerDe.fastMapFromBuffer[FastHashMap[Int, Array[Char]]](directBuf)
    val fasti2Aby_ = SerDe.fastMapFromBuffer[FastHashMap[Int, Array[Byte]]](directBuf)
    val fasti2As_ = SerDe.fastMapFromBuffer[FastHashMap[Int, Array[Short]]](directBuf)
    val fasti2Ai_ = SerDe.fastMapFromBuffer[FastHashMap[Int, Array[Int]]](directBuf)
    val fasti2Al_ = SerDe.fastMapFromBuffer[FastHashMap[Int, Array[Long]]](directBuf)
    val fasti2Af_ = SerDe.fastMapFromBuffer[FastHashMap[Int, Array[Float]]](directBuf)
    val fasti2Ad_ = SerDe.fastMapFromBuffer[FastHashMap[Int, Array[Double]]](directBuf)

    val fasti2Str_ = SerDe.fastMapFromBuffer[FastHashMap[Int, String]](directBuf)

    //    val fasti2G_ = SerDe.fastMapFromBuffer[FastHashMap[Int, ABC]](directBuf)
    //    val fasti2S_ = SerDe.fastMapFromBuffer[FastHashMap[Int, Serializable]](directBuf)
    //    val fasti2V_ = SerDe.fastMapFromBuffer[FastHashMap[Int, IntDummyVector]](directBuf)

    for (idx <- (3 until 7)) {
      assert(fasti2bo.get(intKeys(idx)) == fasti2bo_.get(intKeys(idx)))
    }
    for (idx <- (3 until 7)) {
      assert(fasti2by.get(intKeys(idx)) == fasti2by_.get(intKeys(idx)))
    }
    for (idx <- (3 until 7)) {
      assert(fasti2c.get(intKeys(idx)) == fasti2c_.get(intKeys(idx)))
    }
    for (idx <- (3 until 7)) {
      assert(fasti2s.get(intKeys(idx)) == fasti2s_.get(intKeys(idx)))
    }
    for (idx <- (3 until 7)) {
      assert(fasti2i.get(intKeys(idx)) == fasti2i_.get(intKeys(idx)))
    }
    for (idx <- (3 until 7)) {
      assert(fasti2l.get(intKeys(idx)) == fasti2l_.get(intKeys(idx)))
    }
    for (idx <- (3 until 7)) {
      assert(fasti2f.get(intKeys(idx)) == fasti2f_.get(intKeys(idx)))
    }
    for (idx <- (3 until 7)) {
      assert(fasti2d.get(intKeys(idx)) == fasti2d_.get(intKeys(idx)))
    }

    for (idx <- (3 until 7)) {
      assert(fasti2Abo.get(intKeys(idx)) sameElements  fasti2Abo_.get(intKeys(idx)))
    }
    for (idx <- (3 until 7)) {
      assert(fasti2Aby.get(intKeys(idx)) sameElements  fasti2Aby_.get(intKeys(idx)))
    }
    for (idx <- (3 until 7)) {
      assert(fasti2Ac.get(intKeys(idx)) sameElements  fasti2Ac_.get(intKeys(idx)))
    }
    for (idx <- (3 until 7)) {
      assert(fasti2As.get(intKeys(idx)) sameElements  fasti2As_.get(intKeys(idx)))
    }
    for (idx <- (3 until 7)) {
      assert(fasti2Ai.get(intKeys(idx)) sameElements  fasti2Ai_.get(intKeys(idx)))
    }
    for (idx <- (3 until 7)) {
      assert(fasti2Al.get(intKeys(idx)) sameElements  fasti2Al_.get(intKeys(idx)))
    }
    for (idx <- (3 until 7)) {
      assert(fasti2Af.get(intKeys(idx)) sameElements  fasti2Af_.get(intKeys(idx)))
    }
    for (idx <- (3 until 7)) {
      assert(fasti2Ad.get(intKeys(idx)) sameElements  fasti2Ad_.get(intKeys(idx)))
    }
    for (idx <- (3 until 7)) {
      assert(fasti2Str.get(intKeys(idx)) sameElements  fasti2Str_.get(intKeys(idx)))
    }

    println("test 11 OK")
  }

  test("12 Object FastHashMap Long start end") {
    val fastl2bo = new FastHashMap[Long, Boolean](10)
    val fastl2c = new FastHashMap[Long, Char](10)
    val fastl2by = new FastHashMap[Long, Byte](10)
    val fastl2s = new FastHashMap[Long, Short](10)
    val fastl2i = new FastHashMap[Long, Int](10)
    val fastl2l = new FastHashMap[Long, Long](10)
    val fastl2f = new FastHashMap[Long, Float](10)
    val fastl2d = new FastHashMap[Long, Double](10)

    val fastl2Abo = new FastHashMap[Long, Array[Boolean]](10)
    val fastl2Ac = new FastHashMap[Long, Array[Char]](10)
    val fastl2Aby = new FastHashMap[Long, Array[Byte]](10)
    val fastl2As = new FastHashMap[Long, Array[Short]](10)
    val fastl2Ai = new FastHashMap[Long, Array[Int]](10)
    val fastl2Al = new FastHashMap[Long, Array[Long]](10)
    val fastl2Af = new FastHashMap[Long, Array[Float]](10)
    val fastl2Ad = new FastHashMap[Long, Array[Double]](10)

    val fastl2Str = new FastHashMap[Long, String](10)

    val fastl2G = new FastHashMap[Long, GData](10)
    val fastl2S = new FastHashMap[Long, Serializable](10)

    val fastl2V = new FastHashMap[Long, Vector](10)

    val longKeys = new ArrayBuffer[Long]()
    (0 until 10).foreach { _ =>
      val key = rand.nextLong()
      longKeys += key
      fastl2bo.put(key, rand.nextBoolean())
      fastl2c.put(key, rand.nextPrintableChar())
      fastl2by.put(key, rand.nextInt().toByte)
      fastl2s.put(key, rand.nextInt().toShort)
      fastl2i.put(key, rand.nextInt())
      fastl2l.put(key, rand.nextLong())
      fastl2f.put(key, rand.nextFloat())
      fastl2d.put(key, rand.nextDouble())

      fastl2Abo.put(key, Array.tabulate[Boolean](5)(_ => rand.nextBoolean()))
      fastl2Ac.put(key, Array.tabulate[Char](5)(_ => rand.nextPrintableChar()))
      fastl2Aby.put(key, Array.tabulate[Byte](5)(_ => rand.nextInt().toByte))
      fastl2As.put(key, Array.tabulate[Short](5)(_ => rand.nextInt().toShort))
      fastl2Ai.put(key, Array.tabulate[Int](5)(_ => rand.nextInt()))
      fastl2Al.put(key, Array.tabulate[Long](5)(_ => rand.nextLong()))
      fastl2Af.put(key, Array.tabulate[Float](5)(_ => rand.nextFloat()))
      fastl2Ad.put(key, Array.tabulate[Double](5)(_ => rand.nextDouble()))

      fastl2Str.put(key, rand.nextString(10))

      fastl2G.put(key, new ABC())
      fastl2S.put(key, new DEF())

      fastl2V.put(key, new LongDummyVector(10, Array(1L, 2L, 3L, 7L)))
    }
    //
    //    println(SerDe.serFastMapBufSize(fastl2bo, longKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(fastl2c, longKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(fastl2by, longKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(fastl2s, longKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(fastl2i, longKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(fastl2l, longKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(fastl2f, longKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(fastl2d, longKeys.toArray, 3, 7))
    //
    //    println("fastmap Array")
    //    println(SerDe.serFastMapBufSize(fastl2Abo, longKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(fastl2Ac, longKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(fastl2Aby, longKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(fastl2As, longKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(fastl2Ai, longKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(fastl2Al, longKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(fastl2Af, longKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(fastl2Ad, longKeys.toArray, 3, 7))
    //
    //    println(SerDe.serFastMapBufSize(fastl2Str, longKeys.toArray, 3, 7))
    //
    //    println(SerDe.serFastMapBufSize(fastl2G, longKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(fastl2S, longKeys.toArray, 3, 7))
    //    println(SerDe.serFastMapBufSize(fastl2V, longKeys.toArray, 3, 7))

    var bufSize = 0
    SerDe.serFastMap(fastl2bo, longKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(fastl2bo, longKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fastl2c, longKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(fastl2c, longKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fastl2by, longKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(fastl2by, longKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fastl2s, longKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(fastl2s, longKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fastl2i, longKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(fastl2i, longKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fastl2l, longKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(fastl2l, longKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fastl2f, longKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(fastl2f, longKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fastl2d, longKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(fastl2d, longKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())


    SerDe.serFastMap(fastl2Abo, longKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(fastl2Abo, longKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fastl2Ac, longKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(fastl2Ac, longKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fastl2Aby, longKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(fastl2Aby, longKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fastl2As, longKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(fastl2As, longKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fastl2Ai, longKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(fastl2Ai, longKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fastl2Al, longKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(fastl2Al, longKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fastl2Af, longKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(fastl2Af, longKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fastl2Ad, longKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(fastl2Ad, longKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())


    SerDe.serFastMap(fastl2Str, longKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(fastl2Str, longKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())


    //    SerDe.serFastMap(fastl2G, directBuf)
    //    SerDe.serFastMap(fastl2S, directBuf)
    //    SerDe.serFastMap(fastl2V, directBuf)

    val fastl2bo_ = SerDe.fastMapFromBuffer[FastHashMap[Long, Boolean]](directBuf)
    val fastl2c_ = SerDe.fastMapFromBuffer[FastHashMap[Long, Char]](directBuf)
    val fastl2by_ = SerDe.fastMapFromBuffer[FastHashMap[Long, Byte]](directBuf)
    val fastl2s_ = SerDe.fastMapFromBuffer[FastHashMap[Long, Short]](directBuf)
    val fastl2i_ = SerDe.fastMapFromBuffer[FastHashMap[Long, Int]](directBuf)
    val fastl2l_ = SerDe.fastMapFromBuffer[FastHashMap[Long, Long]](directBuf)
    val fastl2f_ = SerDe.fastMapFromBuffer[FastHashMap[Long, Float]](directBuf)
    val fastl2d_ = SerDe.fastMapFromBuffer[FastHashMap[Long, Double]](directBuf)

    val fastl2Abo_ = SerDe.fastMapFromBuffer[FastHashMap[Long, Array[Boolean]]](directBuf)
    val fastl2Ac_ = SerDe.fastMapFromBuffer[FastHashMap[Long, Array[Char]]](directBuf)
    val fastl2Aby_ = SerDe.fastMapFromBuffer[FastHashMap[Long, Array[Byte]]](directBuf)
    val fastl2As_ = SerDe.fastMapFromBuffer[FastHashMap[Long, Array[Short]]](directBuf)
    val fastl2Ai_ = SerDe.fastMapFromBuffer[FastHashMap[Long, Array[Int]]](directBuf)
    val fastl2Al_ = SerDe.fastMapFromBuffer[FastHashMap[Long, Array[Long]]](directBuf)
    val fastl2Af_ = SerDe.fastMapFromBuffer[FastHashMap[Long, Array[Float]]](directBuf)
    val fastl2Ad_ = SerDe.fastMapFromBuffer[FastHashMap[Long, Array[Double]]](directBuf)

    val fastl2Str_ = SerDe.fastMapFromBuffer[FastHashMap[Long, String]](directBuf)

    //    val fastl2G_ = SerDe.fastMapFromBuffer[FastHashMap[Long, ABC]](directBuf)
    //    val fastl2S_ = SerDe.fastMapFromBuffer[FastHashMap[Long, Serializable]](directBuf)
    //    val fastl2V_ = SerDe.fastMapFromBuffer[FastHashMap[Long, LongDummyVector]](directBuf)

    for (idx <- (3 until 7)) {
      assert(fastl2bo.get(longKeys(idx)) == fastl2bo_.get(longKeys(idx)))
    }
    for (idx <- (3 until 7)) {
      assert(fastl2by.get(longKeys(idx)) == fastl2by_.get(longKeys(idx)))
    }
    for (idx <- (3 until 7)) {
      assert(fastl2c.get(longKeys(idx)) == fastl2c_.get(longKeys(idx)))
    }
    for (idx <- (3 until 7)) {
      assert(fastl2s.get(longKeys(idx)) == fastl2s_.get(longKeys(idx)))
    }
    for (idx <- (3 until 7)) {
      assert(fastl2i.get(longKeys(idx)) == fastl2i_.get(longKeys(idx)))
    }
    for (idx <- (3 until 7)) {
      assert(fastl2l.get(longKeys(idx)) == fastl2l_.get(longKeys(idx)))
    }
    for (idx <- (3 until 7)) {
      assert(fastl2f.get(longKeys(idx)) == fastl2f_.get(longKeys(idx)))
    }
    for (idx <- (3 until 7)) {
      assert(fastl2d.get(longKeys(idx)) == fastl2d_.get(longKeys(idx)))
    }

    for (idx <- (3 until 7)) {
      assert(fastl2Abo.get(longKeys(idx)) sameElements fastl2Abo_.get(longKeys(idx)))
    }
    for (idx <- (3 until 7)) {
      assert(fastl2Aby.get(longKeys(idx)) sameElements fastl2Aby_.get(longKeys(idx)))
    }
    for (idx <- (3 until 7)) {
      assert(fastl2Ac.get(longKeys(idx)) sameElements fastl2Ac_.get(longKeys(idx)))
    }
    for (idx <- (3 until 7)) {
      assert(fastl2As.get(longKeys(idx)) sameElements fastl2As_.get(longKeys(idx)))
    }
    for (idx <- (3 until 7)) {
      assert(fastl2Ai.get(longKeys(idx)) sameElements fastl2Ai_.get(longKeys(idx)))
    }
    for (idx <- (3 until 7)) {
      assert(fastl2Al.get(longKeys(idx)) sameElements fastl2Al_.get(longKeys(idx)))
    }
    for (idx <- (3 until 7)) {
      assert(fastl2Af.get(longKeys(idx)) sameElements fastl2Af_.get(longKeys(idx)))
    }
    for (idx <- (3 until 7)) {
      assert(fastl2Ad.get(longKeys(idx)) sameElements fastl2Ad_.get(longKeys(idx)))
    }

    for (idx <- (3 until 7)) {
      assert(fastl2Str.get(longKeys(idx)) sameElements fastl2Str_.get(longKeys(idx)))
    }

    println("test 12 OK")
  }

  test("13 Object FastHashMap GSV start end") {
    val fasti2G = new FastHashMap[Int, GData](10)
    val fasti2S = new FastHashMap[Int, Serializable](10)
    val fasti2V = new FastHashMap[Int, Vector](10)

    val fastl2G = new FastHashMap[Long, GData](10)
    val fastl2S = new FastHashMap[Long, Serializable](10)
    val fastl2V = new FastHashMap[Long, Vector](10)

    val intKeys = new ArrayBuffer[Int]()
    val longKeys = new ArrayBuffer[Long]()
    (0 until 10).foreach { _ =>
      val ikey = rand.nextInt()
      intKeys += ikey
      fasti2G.put(ikey, new ABC())
      fasti2S.put(ikey, new DEF())
      fasti2V.put(ikey, new IntDummyVector(10, Array(1, 3, 5, 7)))

      val lkey = rand.nextLong()
      longKeys += lkey
      fastl2G.put(lkey, new ABC())
      fastl2S.put(lkey, new DEF())
      fastl2V.put(lkey, new IntDummyVector(10, Array(1, 3, 5, 7)))
    }

    //    SerDe.serFastMapBufSize(fasti2G, intKeys.toArray, 3, 7)
    //    SerDe.serFastMapBufSize(fasti2V, intKeys.toArray, 3, 7)
    //    SerDe.serFastMapBufSize(fasti2S, intKeys.toArray, 3, 7)
    //    SerDe.serFastMapBufSize(fastl2G, longKeys.toArray, 3, 7)
    //    SerDe.serFastMapBufSize(fastl2V, longKeys.toArray, 3, 7)
    //    SerDe.serFastMapBufSize(fastl2S, longKeys.toArray, 3, 7)
    //
    var bufSize = 0
    SerDe.serFastMap(fasti2G, intKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(fasti2G, intKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fasti2V, intKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(fasti2V, intKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fasti2S, intKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(fasti2S, intKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fastl2G, longKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(fastl2G, longKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fastl2V, longKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(fastl2V, longKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    SerDe.serFastMap(fastl2S, longKeys.toArray, 3, 7, directBuf)
    bufSize += SerDe.serFastMapBufSize(fastl2S, longKeys.toArray, 3, 7)
    assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())

    val fasti2G_ = SerDe.fastMapFromBuffer[FastHashMap[Int, ABC]](directBuf)
    val fasti2V_ = SerDe.fastMapFromBuffer[FastHashMap[Int, Vector]](directBuf)
    val fasti2S_ = SerDe.fastMapFromBuffer[FastHashMap[Int, Serializable]](directBuf)
    val fastl2G_ = SerDe.fastMapFromBuffer[FastHashMap[Long, ABC]](directBuf)
    val fastl2V_ = SerDe.fastMapFromBuffer[FastHashMap[Long, Vector]](directBuf)
    val fastl2S_ = SerDe.fastMapFromBuffer[FastHashMap[Long, Serializable]](directBuf)

    println("test 13 OK")
  }

  test("14 vector") {
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

    //    allVectors.foreach{ vec => println(SerDe.serVectorBufSize(vec)) }

    var bufSize = 0
    allVectors.foreach{ vec =>
      SerDe.serVector(vec, directBuf)
      bufSize += SerDe.serVectorBufSize(vec)
      assert(bufSize == directBuf.writerIndex() - directBuf.readerIndex())
    }

    allVectors.foreach{ vec =>
      val tpe = ReflectUtils.getType(vec)
      val desered = SerDe.vectorFromBuffer(tpe, directBuf)
    }

    print("test 14 OK")
  }

  test("15 FastHashMap1"){
    val i2d = new FastHashMap[Int, Double](10)
    (0 until 100).foreach{ _ =>
      i2d(rand.nextInt()) = rand.nextDouble()
    }

    i2d.serialize(directBuf)

    val i2d2 = new FastHashMap[Int, Double](10)
    i2d2.deserialize(directBuf)
    i2d.foreach{ case (k, v) => assert((v - i2d2(k)) < 1e-10)}
    println("test 15 OK")
  }

  test("16 FastHashMap2") {
    val i2d = new FastHashMap[Int, Array[Double]](10)
    (0 until 100).foreach { _ =>
      i2d(rand.nextInt()) = Array.tabulate[Double](rand.nextInt(20)+1){ _ => rand.nextDouble()}
    }

    i2d.serialize(directBuf)

    val i2d2 = new FastHashMap[Int, Array[Double]](10)
    i2d2.deserialize(directBuf)
    i2d.foreach { case (k, v) => v.zip(i2d2(k)).foreach{ case (x, y) => assert( (x - y)< 1e-10) } }
    println("test 16 OK")
  }

  test("17 FastHashMap3") {
    val i2d = new FastHashMap[Int, ABCD[Float]](10)
    val tpe = typeOf[FastHashMap[Int, ABCD[Float]]]

    val i2d2 = ReflectUtils.newFastHashMap(tpe)

    println("test 17 OK")
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

    println("test 18 OK")
  }

  test("19 current problems") {
    val i2oV = new Int2ObjectOpenHashMap[Vector](10)
    val i2oS = new Int2ObjectOpenHashMap[Serializable](10)
    val l2oV = new Long2ObjectOpenHashMap[Vector](10)
    val l2oS = new Long2ObjectOpenHashMap[Serializable](10)

    val intKeys = new ArrayBuffer[Int]()
    val longKeys = new ArrayBuffer[Long]()
    for (idx <- (0 until 10)) {
      val ikey = rand.nextInt()
      intKeys += ikey
      i2oV.put(ikey, new IntDummyVector(10, Array(1, 3, 5, 7)))
      i2oS.put(ikey, new GHI(1, 0.3f))

      val lkey = rand.nextLong()
      longKeys += lkey
      l2oV.put(lkey, new IntDummyVector(10, Array(1, 3, 5, 7)))
      l2oS.put(lkey, new GHI(1, 0.3f))
    }

    // 01 vector serialize and deserialize
    SerDe.serFastMap(i2oV, directBuf)
    SerDe.fastMapFromBuffer[Int2ObjectOpenHashMap[Vector]](directBuf)

    SerDe.serFastMap(i2oV, intKeys.toArray, 3, 7, directBuf)
    SerDe.fastMapFromBuffer[Int2ObjectOpenHashMap[Vector]](directBuf)

    SerDe.serFastMap(l2oV, directBuf)
    SerDe.fastMapFromBuffer[Long2ObjectOpenHashMap[Vector]](directBuf)

    SerDe.serFastMap(l2oV, longKeys.toArray, 3, 7, directBuf)
    SerDe.fastMapFromBuffer[Long2ObjectOpenHashMap[Vector]](directBuf)

    // 02 serialization of class extends serializable
    SerDe.serFastMapBufSize(i2oS)
    SerDe.serFastMapBufSize(i2oS, intKeys.toArray, 3, 7)
    SerDe.serFastMap(i2oS, intKeys.toArray, 3, 7, directBuf)
    SerDe.fastMapFromBuffer[Int2ObjectOpenHashMap[Serializable]](directBuf)

    SerDe.serFastMapBufSize(l2oS)
    SerDe.serFastMapBufSize(l2oS, longKeys.toArray, 3, 7)
    SerDe.serFastMap(l2oS, longKeys.toArray, 3, 7, directBuf)
    SerDe.fastMapFromBuffer[Long2ObjectOpenHashMap[Serializable]](directBuf)

    println("test 19 OK")
    println("pass testing, all problems solved...")
  }

}
