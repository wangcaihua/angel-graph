package com.tencent.angel.graph

import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.ps.storage.vector.element.IElement
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.models.PSMatrix
import org.apache.spark.SparkConf
import org.scalatest.{BeforeAndAfterEach, Suite}

trait SharedPSContext extends WithSpark with BeforeAndAfterEach {
  self: Suite =>

  def doubleEps: Double = 1e-6

  override def getMoreConf(conf: SparkConf): SparkConf = {

    conf.set("spark.ps.mode", "LOCAL")
      .set("spark.ps.jars", "None")
      .set("spark.ps.tmp.path", "file:///tmp/stage")
      .set("spark.ps.out.path", "file:///tmp/output")
      .set("spark.ps.model.path", "file:///tmp/model")
      .set("spark.ps.instances", "1")
      .set("spark.ps.cores", "1")
      .set("spark.ps.out.tmp.path.prefix", "/E://temp")

    conf
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    // PS setup
    PSContext.getOrCreate(sc)
  }

  override def afterAll(): Unit = {
    PSContext.stop()
    super.afterAll()
  }

  protected override def beforeEach(): Unit = {
    super.beforeEach()
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
  }

  def createMatrix(name: String, numRow: Int, minId: Long, maxId: Long,
                   rowType: RowType, itemType: Class[_ <: IElement]): PSMatrix = {
    val matrix = new MatrixContext(name, numRow, minId, maxId)
    matrix.setValidIndexNum(-1)
    matrix.setRowType(rowType)
    if (itemType != null) {
      matrix.setValueType(itemType)
    }

    PSMatrix.matrix(matrix)
  }
}