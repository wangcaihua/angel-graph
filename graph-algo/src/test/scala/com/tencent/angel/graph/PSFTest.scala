package com.tencent.angel.graph

import com.tencent.angel.graph.core.psf.common.{PSFGUCtx, PSFMCtx}
import com.tencent.angel.graph.utils.psfConverters._
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.models.PSMatrix
import it.unimi.dsi.fastutil.longs._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

class PSFTest extends PSFunSuite with SharedPSContext {
  var maxId: Long = -1
  var minId: Long = -1

  override def beforeAll(): Unit = {
    super.beforeAll()

  }

  test("read data") {
    println("hello world!")
  }

  test("pull/push") {
    println("hello world!")
  }
}
