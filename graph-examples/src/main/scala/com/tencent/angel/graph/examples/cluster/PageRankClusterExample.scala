package com.tencent.angel.graph.examples.cluster

import com.tencent.angel.graph.algo.pangerank.PageRank
import com.tencent.angel.graph.examples.utils.ArgsUtil
import com.tencent.angel.graph.framework.GraphLoader
import com.tencent.angel.spark.context.PSContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object PageRankClusterExample {

  def main(args: Array[String]) = {
    val params = ArgsUtil.parse(args)
    val input = params.getOrElse("input", "")
    val maxIter = params.getOrElse("maxIter", "100").toInt
    val batchSize = params.getOrElse("batchSize", "500").toInt
    val resetProb = params.getOrElse("resetProb", "0.15").toFloat

    val conf = new SparkConf()
    val builder = SparkSession.builder()
      .appName("PageRank")
      .config(conf)
    val spark = builder.getOrCreate()
    val sc = spark.sparkContext
    PSContext.getOrCreate(sc)
    val graph = GraphLoader.edgeListFile[Float, Long](sc, input)
    PageRank(graph, maxIter, batchSize, resetProb)
    PSContext.stop()
    sc.stop()
  }
}
