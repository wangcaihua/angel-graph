package com.tencent.angel.graph

import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.ps.storage.vector.element.IElement
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.models.PSMatrix
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

trait SharedPSContext extends BeforeAndAfterAll with BeforeAndAfterEach {
  self: Suite =>

  @transient private var _spark: SparkSession = _

  def doubleEps: Double = 1e-6

  def spark: SparkSession = _spark

  def sc: SparkContext = _spark.sparkContext

  var conf = new SparkConf(false)

  override def beforeAll() {
    super.beforeAll()

    // Angel config
    val psConf = new SparkConf()
      .set("spark.ps.mode", "LOCAL")
      .set("spark.ps.jars", "None")
      .set("spark.ps.tmp.path", "file:///tmp/stage")
      .set("spark.ps.out.path", "file:///tmp/output")
      .set("spark.ps.model.path", "file:///tmp/model")
      .set("spark.ps.instances", "1")
      .set("spark.ps.cores", "1")
      .set("spark.ps.out.tmp.path.prefix", "/E://temp")

    // Spark setup
    val builder = SparkSession.builder()
      .master("local[4]")
      .appName("test")
      .config(psConf)
      .config(conf)

    _spark = builder.getOrCreate()
    sc.setLogLevel("ERROR")

    // PS setup
    PSContext.getOrCreate(sc)
  }

  override def afterAll() {
    try {
      PSContext.stop()
      _spark.stop()
      _spark = null
    } finally {
      super.afterAll()
    }
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