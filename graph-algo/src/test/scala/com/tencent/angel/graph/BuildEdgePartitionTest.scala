package com.tencent.angel.graph


import com.tencent.angel.graph.framework.{Edge, EdgeDirection, EdgePartitionBuilder}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.scalatest.funsuite.AnyFunSuite

class BuildEdgePartitionTest extends AnyFunSuite {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  val spark: SparkSession = SparkSession.builder()
    .master("local[4]")
    .appName("test")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext
  sc.setLogLevel("ERROR")

  var data: RDD[Edge[Int]] = sc.textFile("data/cora/cora.cites")
    .map { line =>
      val arr = line.split("\t").map(_.toLong)
      Edge(arr(0), arr(1), 1)
    }

  data.persist(StorageLevel.MEMORY_ONLY)

  test("read data") {
    println(data.getNumPartitions)
    data.mapPartitions { iter => Iterator.single(iter.size) }.collect().foreach(println)
    println("OK")
  }

  test("in") {
    val edges = data.mapPartitions { iter =>
      val builder = new EdgePartitionBuilder[Int, Int](128, EdgeDirection.In)
      iter.foreach { edge => builder.add(edge) }
      Iterator.single(builder.build)
    }

    val reorderEdges = edges.mapPartitions { iter =>
      val part = iter.next()
      part.iterator
    }

    reorderEdges.saveAsTextFile("orderedIn")
  }

  test("out") {
    val edges = data.mapPartitions { iter =>
      val builder = new EdgePartitionBuilder[Int, Int](128, EdgeDirection.Out)
      iter.foreach { edge => builder.add(edge) }
      Iterator.single(builder.build)
    }

    val reorderEdges = edges.mapPartitions { iter =>
      val part = iter.next()
      part.iterator
    }

    reorderEdges.saveAsTextFile("orderedOut")
  }

  test("both") {
    val edges = data.mapPartitions { iter =>
      val builder = new EdgePartitionBuilder[Int, Int](128, EdgeDirection.Both)
      iter.foreach { edge => builder.add(edge) }
      Iterator.single(builder.build)
    }

    val reorderEdges = edges.mapPartitions { iter =>
      val part = iter.next()
      part.iterator
    }

    reorderEdges.saveAsTextFile("orderedBoth")
  }
}
