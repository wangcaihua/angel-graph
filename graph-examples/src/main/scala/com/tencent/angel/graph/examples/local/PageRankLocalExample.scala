package com.tencent.angel.graph.examples.local

import com.tencent.angel.graph.algo.pangerank.PageRank
import com.tencent.angel.graph.framework.GraphLoader
import com.tencent.angel.spark.context.PSContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object PageRankLocalExample {

  def main(args: Array[String]): Unit = {
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

    val spark = builder.getOrCreate()
    val sc = spark.sparkContext
    //sc.setLogLevel("ERROR")

    // PS setup
    PSContext.getOrCreate(sc)
    val path: String = "./data/cora/cora.cites"
    val graph = GraphLoader.edgeListFile[Float, Long](sc, path)
    PageRank(graph, 100, 500)
    PSContext.stop()
    sc.stop()
  }
}
