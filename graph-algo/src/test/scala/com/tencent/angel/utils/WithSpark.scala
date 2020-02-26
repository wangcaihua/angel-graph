package com.tencent.angel.utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Outcome, Suite}

trait WithSpark extends AnyFunSuite with BeforeAndAfterAll with BeforeAndAfterEach {
  self: Suite =>

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  @transient protected var _spark: SparkSession = _

  def spark: SparkSession = _spark

  def sc: SparkContext = _spark.sparkContext

  def doubleEps: Double = 1e-6

  override def beforeAll() {
    if (_spark == null) {
      _spark = SparkSession.builder()
        .master("local[4]")
        .appName("test")
        .getOrCreate()
    }
  }

  override def afterAll() {
    try {
      if (_spark != null) {
        _spark.stop()
        _spark = null
      }
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

  override def withFixture(test: NoArgTest): Outcome = {
    val testName = test.text
    val suiteName = this.getClass.getName
    try {
      println(s"\n\n===== TEST OUTPUT FOR $suiteName: '$testName' ======\n")
      test()
    } finally {
      println(s"\n===== FINISHED $suiteName: '$testName' ======\n")
    }
  }
}
