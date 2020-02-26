package com.tencent.angel.graph

import java.util.Random

import com.tencent.angel.graph.utils.{FastHashMap, RefHashMap}
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap
import it.unimi.dsi.fastutil.longs._
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable.ListBuffer

case class ReflectHashMap(keys: Array[Int], values: Array[Double],
                          mask: Int, containsNullKey: Boolean, maxFill: Int, n: Int, size: Int) {
  def isEeq(obj: ReflectHashMap): Boolean = {
    keys.zipWithIndex.forall { case (key, idx) => key == obj.keys(idx) } &&
      values.zipWithIndex.forall { case (key, idx) => key == obj.values(idx) } &&
      mask == obj.mask &&
      containsNullKey == obj.containsNullKey &&
      maxFill == obj.maxFill &&
      n == obj.n &&
      size == obj.size
  }
}

class FastUtilTest extends AnyFunSuite {
  val rand = new Random()

  test("put") {

    (10 until 500).foreach { idx =>
      val fhmap = new FastHashMap[Int, Double](idx)
      val fhmap2 = new Int2DoubleOpenHashMap(idx)

      val maxKey = rand.nextInt(300) + 10
      (0 until maxKey).foreach { idx =>
        val value = rand.nextDouble()
        fhmap.put(idx, value)
        fhmap2.put(idx, value)
      }

      val rmap1 = getAngel(fhmap)
      val rmap2 = getUnimi(fhmap2)

      assert(rmap1.isEeq(rmap2))
    }

    println("OK")
  }

  test("get") {
    (10 until 500).foreach { idx =>
      val fhmap = new FastHashMap[Int, Double](idx)
      val fhmap2 = new Int2DoubleOpenHashMap(idx)

      val maxKey = rand.nextInt(300) + 10
      (0 until maxKey).foreach { idx =>
        val value = rand.nextDouble()
        fhmap.put(idx, value)
        fhmap2.put(idx, value)
      }

      val iter = fhmap2.int2DoubleEntrySet().fastIterator()
      var (c1, c2) = (0, 0)
      while (iter.hasNext) {
        val entry = iter.next()

        val key = entry.getIntKey
        val value = entry.getDoubleValue

        assert(fhmap.get(key) == value)
        c1 += 1
      }

      val iter2 = fhmap.iterator
      while (iter2.hasNext) {
        val (key, value) = iter2.next()
        assert(fhmap2.get(key) == value)
        c2 += 1
      }

      assert(c1 == c2)
    }

    println("OK")
  }

  test("containsKey") {
    (10 until 500).foreach { idx =>
      val fhmap = new FastHashMap[Int, Double](idx)
      val fhmap2 = new Int2DoubleOpenHashMap(idx)

      val maxKey = rand.nextInt(300) + 10

      (0 until maxKey).foreach { idx =>
        val value = rand.nextDouble()
        fhmap.put(idx, value)
        fhmap2.put(idx, value)
      }

      (0 until rand.nextInt(20)).foreach { _ =>
        val key = rand.nextInt(maxKey)
        assert(fhmap2.containsKey(key) == fhmap.containsKey(key))
      }
    }
  }

  test("remove") {
    (10 until 500).foreach { idx =>
      val fhmap = new FastHashMap[Int, Double](idx)
      val fhmap2 = new Int2DoubleOpenHashMap(idx)

      val maxKey = rand.nextInt(300) + 10
      (0 until maxKey).foreach { idx =>
        val value = rand.nextDouble()
        fhmap.put(idx, value)
        fhmap2.put(idx, value)
      }

      assert(fhmap.size() == fhmap2.size())

      (0 until rand.nextInt(20)).foreach { _ =>
        val key = rand.nextInt(maxKey)
        fhmap.remove(key)
        fhmap2.remove(key)
        assert(!fhmap.containsKey(key))
        assert(!fhmap2.containsKey(key))
      }

      if (fhmap.size() != fhmap2.size()) {
        val iter1 = fhmap.KeyIterator()
        val buf = ListBuffer[Int]()
        while (iter1.hasNext) {
          val key = iter1.next()
          if (!fhmap2.containsKey(key)) {
            buf.append(key)
          }
        }

        val notIn = buf.toList
        println(maxKey, notIn.mkString("{", ",", "}"), fhmap.size() - fhmap2.size())

        notIn.foreach { key =>
          fhmap.get(key)
        }
        buf.clear()
      }


      val iter = fhmap2.int2DoubleEntrySet().fastIterator()
      var (c1, c2) = (0, 0)
      while (iter.hasNext) {
        val entry = iter.next()

        val key = entry.getIntKey
        val value = entry.getDoubleValue

        assert(fhmap(key) == value)
        c1 += 1
      }

      val iter2 = fhmap.iterator
      while (iter2.hasNext) {
        val (key, value) = iter2.next()
        assert(fhmap2.get(key) == value)
        c2 += 1
      }

      val rmap1 = getAngel(fhmap)
      val rmap2 = getUnimi(fhmap2)

      assert(rmap1.isEeq(rmap2))
      assert(c1 == c2)
    }

    println("OK")
  }

  test("unimi") {
    val temp = new Long2IntOpenHashMap()

    (0 until 100).foreach { _ =>
      val key = rand.nextLong()
      val value = rand.nextInt()
      temp.put(key, value)
    }

    // val (global2local, local2global) = FastHashMap.getIdMaps(temp)
    val fhMap = FastHashMap.fromUnimi[VertexId, Int](temp)
    val (global2local, local2global) = fhMap.asIdMaps
    val tmp2 = fhMap.toUnimi[Long2IntOpenHashMap]

    val iter = temp.long2IntEntrySet().fastIterator()
    while (iter.hasNext) {
      val entry = iter.next()
      val key = entry.getLongKey
      assert(key == local2global(global2local(key)))
      assert(tmp2.get(key) == entry.getIntValue)
    }

    println("OK")

  }

  test("refHashMap") {
    val temp = new Long2IntOpenHashMap()

    (0 until 1000).foreach { _ =>
      val key = rand.nextLong()
      val value = rand.nextInt()
      temp.put(key, value)
    }

    val (global2local, local2global) = FastHashMap.getIdMaps(temp)
    val ref = new RefHashMap[Double](global2local, local2global)

    println(ref.size())
    try {
      ref.put(10L, 1.3)
    } catch {
      case e: Exception =>
        println(e.getMessage)
    }

    val iter = temp.long2IntEntrySet().fastIterator()
    var key = 0L
    while (iter.hasNext) {
      val entry = iter.next()
      key = entry.getLongKey
      ref.put(key, rand.nextDouble())
    }

    assert(ref.size() == temp.size())
    println(ref.get(key))
    ref.remove(key)
    assert(ref.size() == temp.size() - 1)

    ref.foreachKey(key => assert(temp.containsKey(key)))

    val ref2 = ref.mapValues(d => d * 7)

    val iter2 = ref.iterator
    while (iter2.hasNext) {
      val (key, value) = iter2.next()
      assert((ref2(key) / value - 7) < 1e-10)
    }

    ref2.clear()


    println("hello world!")
  }

  def getField1[T](obj: Any, name: String): T = {
    val clz = obj.getClass
    val field = clz.getDeclaredField(name)
    field.setAccessible(true)
    field.get(obj).asInstanceOf[T]
  }

  def getField2[T](obj: Any, name: String): T = {
    val prefix = "com$tencent$angel$graph$utils$FastHashMap$$"
    val method = obj.getClass.getMethod(s"$prefix$name")
    method.setAccessible(true)
    method.invoke(obj).asInstanceOf[T]
  }

  def getUnimi(obj: Any): ReflectHashMap = {
    val keys = getField1[Array[Int]](obj, "key")
    val values = getField1[Array[Double]](obj, "value")
    val mask = getField1[Int](obj, "mask")
    val containsNullKey = getField1[Boolean](obj, "containsNullKey")
    val maxFill = getField1[Int](obj, "maxFill")
    val n = getField1[Int](obj, "n")
    val size = getField1[Int](obj, "size")

    ReflectHashMap(keys, values, mask, containsNullKey, maxFill, n, size)
  }

  def getAngel(obj: Any): ReflectHashMap = {
    val keys = getField1[Array[Int]](obj, "keys$mcI$sp")
    val values = getField1[Array[Double]](obj, "values$mcD$sp")

    val mask = getField2[Int](obj, "mask")
    val containsNullKey = getField2[Boolean](obj, "containsNullKey")
    val maxFill = getField2[Int](obj, "maxFill")

    val n = getField2[Int](obj, "n")
    val method = obj.getClass.getMethod("size")
    method.setAccessible(true)
    val size = method.invoke(obj).asInstanceOf[Int]

    ReflectHashMap(keys, values, mask, containsNullKey, maxFill, n, size)
  }
}
