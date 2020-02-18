package com.tencent.angel.graph

import com.tencent.angel.graph.core.data.NodeN
import com.tencent.angel.graph.core.psf.common.PSFGUCtx
import com.tencent.angel.graph.utils.psfConverters._
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.models.PSMatrix
import it.unimi.dsi.fastutil.longs._
import org.apache.spark.rdd.RDD

class PSFTest extends PSFunSuite with SharedPSContext {
  var data: RDD[Long2ObjectOpenHashMap[NodeN]] = _
  var maxId: Long = -1
  var minId: Long = -1

  override def beforeAll(): Unit = {
    super.beforeAll()
    data = sc.textFile("data/cora/cora.cites")
      .map { line =>
        val arr = line.split("\t").map(_.toLong)
        arr(0) -> arr(1)
      }.mapPartitions { iter =>
      val map = new Long2ObjectOpenHashMap[LongArrayList]()
      iter.foreach { case (key, value) =>
        if (map.containsKey(key)) {
          map.get(key).add(value)
        } else {
          val list = new LongArrayList()
          list.add(value)
          map.put(key, list)
        }
      }

      val partition = new Long2ObjectOpenHashMap[NodeN]()
      val it = map.long2ObjectEntrySet().fastIterator()
      while (it.hasNext) {
        val entry = it.next()
        partition.put(entry.getLongKey, NodeN(entry.getValue.toLongArray))
      }

      Iterator.single(partition)
    }

    maxId = data.map(m => m.keySet().toLongArray.max).max()
    minId = data.map(m => m.keySet().toLongArray.min).min()


  }

  test("read data") {
    val count = data.count()
    println(count, data.getNumPartitions)
  }

  test("create matrix") {
    val matrix: PSMatrix = createMatrix("neighbor", 1, minId, maxId + 1,
      RowType.T_ANY_LONGKEY_SPARSE, classOf[NodeN])

    println(data.getNumPartitions)

    data.foreachPartition { iter =>
      PSContext.instance()

      val pData = iter.next()
      val push = matrix.createUpdate { puParam: PSFGUCtx =>
        val tParam = puParam.getParam[Long2ObjectOpenHashMap[NodeN]]
        val rowData = puParam.getData[Long2ObjectOpenHashMap[NodeN]]

        rowData.putAll(tParam)
      }

      push(pData, 600)
    }

  }
}
