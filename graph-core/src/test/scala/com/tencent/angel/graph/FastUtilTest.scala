package com.tencent.angel.graph

import java.util.Random

import com.tencent.angel.graph.utils.FastHashMap
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap
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
