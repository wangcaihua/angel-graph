package com.tencent.angel.utils

import com.tencent.angel.graph.VertexId
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.models.PSMatrix
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.Suite

trait WithSONA extends WithSpark {
  self: Suite =>

  override def beforeAll(): Unit = {
    val conf = new SparkConf(false)
    conf.set("spark.ps.mode", "LOCAL")
      .set("spark.ps.jars", "None")
      .set("spark.ps.tmp.path", "/tmp/stage")
      .set("spark.ps.out.path", "/tmp/output")
      .set("spark.ps.model.path", "/tmp/model")
      .set("spark.ps.instances", "1")
      .set("spark.ps.cores", "1")
      .set("spark.ps.out.tmp.path.prefix", "/tmp")

    // Spark setup
    val builder = SparkSession.builder()
      .master("local[4]")
      .appName("test")
      .config(conf)

    _spark = builder.getOrCreate()
    sc.setLogLevel("ERROR")

    // PS setup
    PSContext.getOrCreate(sc)
  }

  override def afterAll(): Unit = {
    try {
      if (_spark != null) {
        PSContext.stop()
        _spark.stop()
        _spark = null
      }
    } finally {
      super.afterAll()
    }
  }

  def createMatrix(name: String, minId: Long, maxId: Long): PSMatrix = {
    val matrix = new MatrixContext(name, 1, minId, maxId)
    matrix.setValidIndexNum(-1)
    if (classOf[VertexId] == classOf[Long]) {
      matrix.setRowType(RowType.T_INT_SPARSE_LONGKEY)
    } else {
      matrix.setRowType(RowType.T_INT_SPARSE)
    }

    PSMatrix.matrix(matrix)
  }
}