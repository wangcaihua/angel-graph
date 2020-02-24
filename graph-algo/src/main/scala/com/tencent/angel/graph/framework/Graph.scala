package com.tencent.angel.graph.framework


import com.tencent.angel.graph.VertexId
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.spark.models.PSMatrix
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

class Graph(val edges: RDD[_]) extends Serializable {

}

object Graph {
  /*
  def edgeListFile[N <: Node : ClassTag](sc: SparkContext, path: String, numEdgePartition: Int = -1,
                                          canonicalOrientation: Boolean = false,
                                          storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
                                         ): Graph[N] = {
    val lines = if (numEdgePartition > 0) {
      sc.textFile(path, numEdgePartition).coalesce(numEdgePartition)
    } else {
      sc.textFile(path)
    }

    val edges = lines.mapPartitions { iter =>
      val builder = new EdgePartitionBuilder

      iter.foreach { line =>
        if (!line.isEmpty && line(0) != '#') {
          val lineArray = line.split("\\s+")
          if (lineArray.length < 2) {
            throw new IllegalArgumentException("Invalid line: " + line)
          }
          val srcId = lineArray(0).asInstanceOf[VertexId]
          val dstId = lineArray(1).asInstanceOf[VertexId]
          if (canonicalOrientation && srcId > dstId) {
            builder.add(dstId, srcId, 1)
          } else {
            builder.add(srcId, dstId, 1)
          }
        }
      }

      Iterator.single(builder.build())
    }

    edges.persist(storageLevel).count()

    new Graph[N](edges)
  }

   */


}