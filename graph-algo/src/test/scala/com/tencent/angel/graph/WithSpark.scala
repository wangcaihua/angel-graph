package com.tencent.angel.graph

import com.tencent.angel.spark.context.PSContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, Suite}

trait WithSpark extends AnyFunSuite with BeforeAndAfterAll {
  self: Suite =>

  @transient private var _spark: SparkSession = _

  def spark: SparkSession = _spark

  def sc: SparkContext = _spark.sparkContext

  override def beforeAll() {
    super.beforeAll()
    val conf = new SparkConf()
    // Spark setup
    val builder = SparkSession.builder()
      .master("local[4]")
      .appName("test")
      .config(getMoreConf(conf))

    _spark = builder.getOrCreate()
    sc.setLogLevel("ERROR")
  }

  protected def getMoreConf(conf: SparkConf):SparkConf = {
    conf
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
}
