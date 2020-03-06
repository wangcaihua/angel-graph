package com.tencent.angel.graph.framework

import com.tencent.angel.graph._
import com.tencent.angel.graph.framework.EdgeDirection.EdgeDirection
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkContext}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

object GraphLoader {

  def edgesFromFile[VD: ClassTag : TypeTag, ED: ClassTag](sc: SparkContext, path: String,
                                                          numEdgePartition: Int = -1,
                                                          edgeDirection: EdgeDirection = EdgeDirection.Both,
                                                          canonicalOrientation: Boolean = false,
                                                          storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
                                                         ): RDD[EdgePartition[VD, ED]] = {
    val lines = if (numEdgePartition > 0) {
      sc.textFile(path, numEdgePartition).coalesce(numEdgePartition)
    } else {
      sc.textFile(path)
    }

    val edges = lines.mapPartitions { iter =>
      val builder = new EdgePartitionBuilder[VD, ED](edgeDirection = edgeDirection)
      val defaultEdgeAttr = getDefaultEdgeAttr[ED]

      iter.foreach { line =>
        if (!line.isEmpty && line(0) != '#') {
          val lineArray = line.split("\\s+")
          if (lineArray.length < 2) {
            throw new IllegalArgumentException("Invalid line: " + line)
          }

          if (lineArray.length == 2) {
            val srcId = lineArray(0).toVertexId
            val dstId = lineArray(1).toVertexId
            if (canonicalOrientation) {
              if (srcId > dstId) {
                builder.add(Edge(dstId, srcId, defaultEdgeAttr))
              }
            } else if (srcId != dstId) {
              builder.add(Edge(srcId, dstId, defaultEdgeAttr))
            } else {
              println(s"$srcId = $dstId, error! ")
            }
          } else if (lineArray.length == 3) {
            val srcId = lineArray(0).toVertexId
            val dstId = lineArray(1).toVertexId
            val weight = lineArray(3).toEdgeAttr[ED]
            if (canonicalOrientation) {
              if (srcId > dstId) {
                builder.add(Edge(dstId, srcId, weight))
              }
            } else if (srcId != dstId) {
              builder.add(Edge(srcId, dstId, weight))
            } else {
              println(s"$srcId = $dstId, error! ")
            }
          } else {
            throw new Exception("not support!")
          }
        }
      }

      Iterator.single(builder.build)
    }

    edges.persist(storageLevel)
  }


  def edgesFromRDD[VD: ClassTag : TypeTag, ED: ClassTag](rdd: RDD[(VertexId, VertexId)],
                                                         edgeDirection: EdgeDirection = EdgeDirection.Out,
                                                         canonicalOrientation: Boolean = false,
                                                         storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): RDD[EdgePartition[VD, ED]] = {

    val edges = rdd.mapPartitions { iter =>
      val builder = new EdgePartitionBuilder[VD, ED](edgeDirection = edgeDirection)
      val defaultEdgeAttr = getDefaultEdgeAttr[ED]

      iter.foreach { case (srcId, dstId) =>
        builder.add(Edge(srcId, dstId, defaultEdgeAttr))
      }

      Iterator.single(builder.build)
    }

    edges.persist(storageLevel)
  }


  def edgesWithWeightFromRDD[VD: ClassTag : TypeTag](rdd: RDD[(VertexId, VertexId, WgtTpe)],
                                                     edgeDirection: EdgeDirection = EdgeDirection.Out,
                                                     canonicalOrientation: Boolean = false,
                                                     storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): RDD[EdgePartition[VD, WgtTpe]] = {

    val edges = rdd.mapPartitions { iter =>
      val builder = new EdgePartitionBuilder[VD, WgtTpe](edgeDirection = edgeDirection)

      iter.foreach { case (srcId, dstId, weight) =>
        builder.add(Edge[WgtTpe](srcId, dstId, weight))
      }

      Iterator.single(builder.build)
    }

    edges.persist(storageLevel)
  }


  def typedEdgesFromFile[VD: ClassTag : TypeTag](sc: SparkContext, path: String,
                                                 numEdgePartition: Int = -1,
                                                 edgeDirection: EdgeDirection = EdgeDirection.Both,
                                                 canonicalOrientation: Boolean = false,
                                                 storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
                                                ): RDD[EdgePartition[VD, Long]] = {
    val lines = if (numEdgePartition > 0) {
      sc.textFile(path, numEdgePartition).coalesce(numEdgePartition)
    } else {
      sc.textFile(path)
    }

    val edges = lines.mapPartitions { iter =>
      val builder = new EdgePartitionBuilder[VD, Long](edgeDirection = edgeDirection)
      iter.foreach { line =>
        if (!line.isEmpty && line(0) != '#') {
          val lineArray = line.split("\\s+")
          if (lineArray.length < 2) {
            throw new IllegalArgumentException("Invalid line: " + line)
          }

          val edgeAttrBuilder = new EdgeAttributeBuilder()
          if (lineArray.length == 4) {
            val srcId = lineArray(0).toVertexId
            val dstId = lineArray(2).toVertexId
            if (canonicalOrientation) {
              if (srcId > dstId) {
                builder.add(Edge(dstId, srcId,
                  edgeAttrBuilder.put(lineArray(1), lineArray(3)).build))
              }
            } else if (srcId != dstId) {
              builder.add(Edge(srcId, dstId,
                edgeAttrBuilder.put(lineArray(1), lineArray(3)).build))
            } else {
              println(s"$srcId = $dstId, error! ")
            }
          } else if (lineArray.length == 5) {
            val srcId = lineArray(0).toVertexId
            val dstId = lineArray(2).toVertexId
            if (canonicalOrientation) {
              if (srcId > dstId) {
                builder.add(Edge(dstId, srcId,
                  edgeAttrBuilder.put(lineArray(1), lineArray(3), lineArray(4)).build))
              }
            } else if (srcId != dstId) {
              builder.add(Edge(srcId, dstId,
                edgeAttrBuilder.put(lineArray(1), lineArray(3), lineArray(4)).build))
            } else {
              println(s"$srcId = $dstId, error! ")
            }
          } else {
            throw new Exception("not support!")
          }
        }
      }

      Iterator.single(builder.build)
    }

    edges.persist(storageLevel)
  }

  def edgesFromTypedRDD[VD: ClassTag : TypeTag](rdd: RDD[(VertexId, VertexType, VertexId, VertexType)],
                                                edgeDirection: EdgeDirection = EdgeDirection.Out,
                                                canonicalOrientation: Boolean = false,
                                                storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): RDD[EdgePartition[VD, Long]] = {

    val edges = rdd.mapPartitions { iter =>
      val builder = new EdgePartitionBuilder[VD, Long](edgeDirection = edgeDirection)
      val edgeAttrBuilder = new EdgeAttributeBuilder()
      iter.foreach { case (srcId, srcType, dstId, dstType) =>
        edgeAttrBuilder.put(srcType, dstType)
        builder.add(Edge[Long](srcId, dstId, edgeAttrBuilder.build))
      }

      Iterator.single(builder.build)
    }

    edges.persist(storageLevel)
  }

  def typedEdgeWithWeightFromRDD[VD: ClassTag : TypeTag](rdd: RDD[(VertexId, VertexType, VertexId, VertexType, WgtTpe)],
                                                         edgeDirection: EdgeDirection = EdgeDirection.Out,
                                                         canonicalOrientation: Boolean = false,
                                                         storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): RDD[EdgePartition[VD, Long]] = {

    val edges = rdd.mapPartitions { iter =>
      val builder = new EdgePartitionBuilder[VD, Long](edgeDirection = edgeDirection)
      val edgeAttrBuilder = new EdgeAttributeBuilder()
      iter.foreach { case (srcId, srcType, dstId, dstType, weight) =>
        edgeAttrBuilder.put(srcType, dstType, weight)
        builder.add(Edge[Long](srcId, dstId, edgeAttrBuilder.build))
      }

      Iterator.single(builder.build)
    }

    edges.persist(storageLevel)
  }

  def mergeTrainingLabel[VD: ClassTag : TypeTag, ED: ClassTag](edges: RDD[EdgePartition[VD, ED]],
                                                               labels: RDD[(VertexId, Float)]): RDD[EdgePartition[VD, ED]] = {
    val partitioner = new HashPartitioner(edges.getNumPartitions)
    val partitionedLabels = labels.partitionBy(partitioner)

    edges.zipPartitions(partitionedLabels) { case (iter1, iter2) =>
      val edgePartition = iter1.next()
      iter2.foreach { case (vid, label) =>
        edgePartition.addTrainingData(vid, label)
      }

      Iterator.single(edgePartition)
    }.persist(edges.getStorageLevel)
  }

  def mergeTestLabel[VD: ClassTag : TypeTag, ED: ClassTag](edges: RDD[EdgePartition[VD, ED]],
                                                           labels: RDD[(VertexId, Float)]): RDD[EdgePartition[VD, ED]] = {
    val partitioner = new HashPartitioner(edges.getNumPartitions)
    val partitionedLabels = labels.partitionBy(partitioner)

    edges.zipPartitions(partitionedLabels) { case (iter1, iter2) =>
      val edgePartition = iter1.next()
      iter2.foreach { case (vid, label) =>
        edgePartition.addTestData(vid, label)
      }

      Iterator.single(edgePartition)
    }.persist(edges.getStorageLevel)
  }

  def edgeListFile[VD: ClassTag : TypeTag, ED: ClassTag](sc: SparkContext, path: String,
                                                         numEdgePartition: Int = -1,
                                                         edgeDirection: EdgeDirection = EdgeDirection.Both,
                                                         canonicalOrientation: Boolean = false,
                                                         storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
                                                        ): Graph[VD, ED] = {
    val edges = edgesFromFile[VD, ED](sc, path, numEdgePartition, edgeDirection, canonicalOrientation, storageLevel)

    new Graph(edges)
  }
}
