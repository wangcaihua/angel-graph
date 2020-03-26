package com.tencent.angel.graph.framework

import com.tencent.angel.graph._
import com.tencent.angel.graph.core.data.{Typed, UnTyped}
import com.tencent.angel.graph.framework.EdgeDirection.EdgeDirection
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkContext}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

object GraphLoader {
  private val spliter: String = "\\s+"

  def edgesFromFile[VD: ClassTag : TypeTag, ED: ClassTag](sc: SparkContext, path: String,
                                                          numEdgePartition: Int = -1,
                                                          edgeDirection: EdgeDirection = EdgeDirection.Out,
                                                          canonicalOrientation: Boolean = false,
                                                          storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
                                                         ): RDD[EdgePartition[VD, ED]] = {
    val lines = if (numEdgePartition > 0) {
      sc.textFile(path, numEdgePartition).coalesce(numEdgePartition)
    } else {
      sc.textFile(path)
    }

    val edges = lines.mapPartitions { iter =>
      val builder = new StandardEdgePartitionBuilder[VD, ED](edgeDirection = edgeDirection)

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
                builder.add(dstId, srcId)
              }
            } else if (srcId != dstId) {
              builder.add(srcId, dstId)
            } else {
              println(s"$srcId = $dstId, error! ")
            }
          } else if (lineArray.length == 3) {
            val srcId = lineArray(0).toVertexId
            val dstId = lineArray(1).toVertexId
            val weight = lineArray(2).toEdgeAttr[ED]
            if (canonicalOrientation) {
              if (srcId > dstId) {
                builder.add(dstId, srcId, weight)
              }
            } else if (srcId != dstId) {
              builder.add(srcId, dstId, weight)
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

  def edgesWithNeighborAttrFromFile[VD <: UnTyped : ClassTag : TypeTag](sc: SparkContext, path: String,
                                                                        numEdgePartition: Int = -1,
                                                                        hasEdgesInPartition: Boolean = true,
                                                                        edgeDirection: EdgeDirection = EdgeDirection.Out,
                                                                        canonicalOrientation: Boolean = false,
                                                                        storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
                                                                       ): RDD[EdgePartition[VD, WgtTpe]] = {
    val lines = if (numEdgePartition > 0) {
      sc.textFile(path, numEdgePartition)
    } else {
      sc.textFile(path)
    }

    val numPartition = if (numEdgePartition <= 0) lines.getNumPartitions else numEdgePartition

    val numCol = lines.take(5).collectFirst {
      case line if line.nonEmpty && !line.startsWith("#") =>
        line.split(spliter).length
    }

    numCol match {
      case Some(x: Int) if x == 2 =>
        val typedLines = lines.mapPartitions { iter =>
          iter.collect {
            case line if line.nonEmpty && line(0) != '#' =>
              val lineArray = line.split(spliter)
              if (lineArray.length < 2) {
                throw new IllegalArgumentException("Invalid line: " + line)
              }
              val srcId = lineArray(0).toVertexId
              val dstId = lineArray(1).toVertexId

              srcId -> dstId
          }
        }.repartition(numPartition)

        edgesWithNeighborAttrFromRDD(typedLines, hasEdgesInPartition, edgeDirection,
          canonicalOrientation, storageLevel)
      case Some(x: Int) if x == 3 =>
        val typedLines = lines.mapPartitions { iter =>
          iter.collect {
            case line if line.nonEmpty && line(0) != '#' =>
              val lineArray = line.split(spliter)
              if (lineArray.length < 3) {
                throw new IllegalArgumentException("Invalid line: " + line)
              }
              val srcId = lineArray(0).toVertexId
              val dstId = lineArray(1).toVertexId
              val weight = lineArray(2).toEdgeAttr[WgtTpe]
              srcId -> (dstId, weight)
          }
        }.repartition(numPartition)
          .map { case (srcId, (dstId, weight)) => (srcId, dstId, weight) }

        edgesWithWeightedNeighborAttrFromRDD(typedLines, hasEdgesInPartition, edgeDirection,
          canonicalOrientation, storageLevel)
      case None =>
        throw new Exception("read data error!")
    }
  }

  def edgesWithNeighborSlotFromFile[VD: ClassTag : TypeTag, N <: UnTyped : ClassTag : TypeTag]
  (sc: SparkContext, path: String, numEdgePartition: Int = -1, hasEdgesInPartition: Boolean = true,
   edgeDirection: EdgeDirection = EdgeDirection.Out, canonicalOrientation: Boolean = false,
   storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
  ): RDD[EdgePartition[VD, WgtTpe]] = {
    val lines = if (numEdgePartition > 0) {
      sc.textFile(path, numEdgePartition)
    } else {
      sc.textFile(path)
    }

    val numPartition = if (numEdgePartition <= 0) lines.getNumPartitions else numEdgePartition

    val numCol = lines.take(5).collectFirst {
      case line if line.nonEmpty && !line.startsWith("#") =>
        line.split(spliter).length
    }

    numCol match {
      case Some(x:Int) if x == 2 =>
        val typedLines = lines.mapPartitions { iter =>
          iter.collect {
            case line if line.nonEmpty && line(0) != '#' =>
              val lineArray = line.split(spliter)
              if (lineArray.length < 2) {
                throw new IllegalArgumentException("Invalid line: " + line)
              }
              val srcId = lineArray(0).toVertexId
              val dstId = lineArray(1).toVertexId

              srcId -> dstId
          }
        }.repartition(numPartition)

        edgesWithNeighborSlotFromRDD(typedLines, hasEdgesInPartition, edgeDirection,
          canonicalOrientation, storageLevel)
      case Some(x:Int) if x == 3 =>
        val typedLines = lines.mapPartitions { iter =>
          iter.collect {
            case line if line.nonEmpty && line(0) != '#' =>
              val lineArray = line.split(spliter)
              if (lineArray.length < 3) {
                throw new IllegalArgumentException("Invalid line: " + line)
              }
              val srcId = lineArray(0).toVertexId
              val dstId = lineArray(1).toVertexId
              val weight = lineArray(2).toEdgeAttr[WgtTpe]
              srcId -> (dstId, weight)
          }
        }.repartition(numPartition)
          .map { case (srcId, (dstId, weight)) => (srcId, dstId, weight) }

        edgesWithWeightedNeighborSlotFromRDD(typedLines, hasEdgesInPartition, edgeDirection,
          canonicalOrientation, storageLevel)
      case None =>
        throw new Exception("read data error!")
    }
  }

  def edgesFromRDD[VD: ClassTag : TypeTag, ED: ClassTag](rdd: RDD[(VertexId, VertexId)],
                                                         edgeDirection: EdgeDirection = EdgeDirection.Out,
                                                         storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): RDD[EdgePartition[VD, ED]] = {

    val edges = rdd.mapPartitions { iter =>
      val builder = new StandardEdgePartitionBuilder[VD, ED](edgeDirection = edgeDirection)

      iter.foreach { case (srcId, dstId) =>
        if (srcId != dstId) {
          builder.add(srcId, dstId)
        }
      }

      Iterator.single(builder.build)
    }

    edges.persist(storageLevel)
  }

  def edgesWithNeighborAttrFromRDD[VD <: UnTyped : ClassTag : TypeTag](rdd: RDD[(VertexId, VertexId)],
                                                                       hasEdgesInPartition: Boolean = true,
                                                                       edgeDirection: EdgeDirection = EdgeDirection.Out,
                                                                       canonicalOrientation: Boolean = false,
                                                                       storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): RDD[EdgePartition[VD, WgtTpe]] = {

    val edges = rdd.mapPartitions { iter =>
      val builder = new EdgePartitionWithNeighborAttrBuilder[VD](
        hasEdges = hasEdgesInPartition,
        edgeDirection = edgeDirection)

      iter.foreach { case (srcId, dstId) =>
        if (canonicalOrientation) {
          if (srcId > dstId) {
            builder.add(dstId, srcId)
          }
        } else if (srcId != dstId) {
          builder.add(srcId, dstId)
        } else {
          println(s"$srcId = $dstId, error! ")
        }
      }

      Iterator.single(builder.build)
    }

    edges.persist(storageLevel)
  }

  def edgesWithNeighborSlotFromRDD[VD: ClassTag : TypeTag, N <: UnTyped : ClassTag : TypeTag]
  (rdd: RDD[(VertexId, VertexId)],
   hasEdgesInPartition: Boolean = true,
   edgeDirection: EdgeDirection = EdgeDirection.Out,
   canonicalOrientation: Boolean = false,
   storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): RDD[EdgePartition[VD, WgtTpe]] = {

    val edges = rdd.mapPartitions { iter =>
      val builder = new EdgePartitionWithNeighborSlotBuilder[VD, N](
        hasEdges = hasEdgesInPartition,
        edgeDirection = edgeDirection)

      iter.foreach { case (srcId, dstId) =>
        if (canonicalOrientation) {
          if (srcId > dstId) {
            builder.add(dstId, srcId)
          }
        } else if (srcId != dstId) {
          builder.add(srcId, dstId)
        } else {
          println(s"$srcId = $dstId, error! ")
        }
      }

      Iterator.single(builder.build)
    }

    edges.persist(storageLevel)
  }

  def graphFromFile[VD: ClassTag : TypeTag, ED: ClassTag: TypeTag](sc: SparkContext, path: String,
                                                                   numEdgePartition: Int = -1,
                                                                   edgeDirection: EdgeDirection = EdgeDirection.Both,
                                                                   canonicalOrientation: Boolean = false,
                                                                   storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
                                                                   ): Graph[VD, ED] = {
    val edges = sc.textFile(path).map { line =>
      val fields = line.split(",")
      (fields(0).toLong, Edge(fields(0).toLong, fields(1).toLong, fields(2).toEdgeAttr[ED]))
    }.groupByKey(numEdgePartition).mapPartitions{ iter =>
      val builder = new StandardEdgePartitionBuilder[VD, ED](edgeDirection = edgeDirection)

      while (iter.hasNext) {
        val pairs = iter.next()
        pairs._2.foreach{ edge =>
          builder.add(edge)
        }
      }
      val newEdgePartition = builder.build

      Iterator.single(newEdgePartition)
    }

    edges.persist(storageLevel)
    new Graph(edges)
  }


  def edgesWithWeightFromRDD[VD: ClassTag : TypeTag](rdd: RDD[(VertexId, VertexId, WgtTpe)],
                                                     edgeDirection: EdgeDirection = EdgeDirection.Out,
                                                     canonicalOrientation: Boolean = false,
                                                     storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): RDD[EdgePartition[VD, WgtTpe]] = {

    val edges = rdd.mapPartitions { iter =>
      val builder = new StandardEdgePartitionBuilder[VD, WgtTpe](edgeDirection = edgeDirection)

      iter.foreach { case (srcId, dstId, weight) =>
        builder.add(Edge[WgtTpe](srcId, dstId, weight))
      }

      Iterator.single(builder.build)
    }

    edges.persist(storageLevel)
  }

  def edgesWithWeightedNeighborAttrFromRDD[VD <: UnTyped : ClassTag : TypeTag](rdd: RDD[(VertexId, VertexId, WgtTpe)],
                                                                               hasEdgesInPartition: Boolean = true,
                                                                               edgeDirection: EdgeDirection = EdgeDirection.Out,
                                                                               canonicalOrientation: Boolean = false,
                                                                               storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): RDD[EdgePartition[VD, WgtTpe]] = {

    val edges = rdd.mapPartitions { iter =>
      val builder = new EdgePartitionWithNeighborAttrBuilder[VD](
        hasEdges = hasEdgesInPartition,
        edgeDirection = edgeDirection)

      iter.foreach { case (srcId, dstId, weight) =>
        if (canonicalOrientation) {
          if (srcId > dstId) {
            builder.add(dstId, srcId, weight)
          }
        } else if (srcId != dstId) {
          builder.add(dstId, srcId, weight)
        } else {
          println(s"$srcId = $dstId, error! ")
        }
      }

      Iterator.single(builder.build)
    }

    edges.persist(storageLevel)
  }

  def edgesWithWeightedNeighborSlotFromRDD[VD: ClassTag : TypeTag, N <: UnTyped : ClassTag : TypeTag]
  (rdd: RDD[(VertexId, VertexId, WgtTpe)],
   hasEdgesInPartition: Boolean = true,
   edgeDirection: EdgeDirection = EdgeDirection.Out,
   canonicalOrientation: Boolean = false,
   storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): RDD[EdgePartition[VD, WgtTpe]] = {

    val edges = rdd.mapPartitions { iter =>
      val builder = new EdgePartitionWithNeighborSlotBuilder[VD, N](
        hasEdges = hasEdgesInPartition,
        edgeDirection = edgeDirection)

      iter.foreach { case (srcId, dstId, weight) =>
        if (canonicalOrientation) {
          if (srcId > dstId) {
            builder.add(dstId, srcId, weight)
          }
        } else if (srcId != dstId) {
          builder.add(dstId, srcId, weight)
        } else {
          println(s"$srcId = $dstId, error! ")
        }
      }

      Iterator.single(builder.build)
    }

    edges.persist(storageLevel)
  }


  def typedEdgesFromFile[VD: ClassTag : TypeTag](sc: SparkContext, path: String,
                                                 numEdgePartition: Int = -1,
                                                 edgeDirection: EdgeDirection = EdgeDirection.Out,
                                                 canonicalOrientation: Boolean = false,
                                                 storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
                                                ): RDD[EdgePartition[VD, Long]] = {
    val lines = if (numEdgePartition > 0) {
      sc.textFile(path, numEdgePartition).coalesce(numEdgePartition)
    } else {
      sc.textFile(path)
    }

    val edges = lines.mapPartitions { iter =>
      val builder = new StandardEdgePartitionBuilder[VD, Long](edgeDirection = edgeDirection)
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

  def typedEdgesWithNeighborAttrFromFile[VD <: Typed : ClassTag : TypeTag](sc: SparkContext, path: String,
                                                                           numEdgePartition: Int = -1,
                                                                           hasEdgesInPartition: Boolean = true,
                                                                           edgeDirection: EdgeDirection = EdgeDirection.Out,
                                                                           canonicalOrientation: Boolean = false,
                                                                           storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
                                                                          ): RDD[EdgePartition[VD, Long]] = {
    val lines = if (numEdgePartition > 0) {
      sc.textFile(path, numEdgePartition)
    } else {
      sc.textFile(path)
    }

    val numPartition = if (numEdgePartition <= 0) lines.getNumPartitions else numEdgePartition

    val numCol = lines.take(5).collectFirst {
      case line if line.nonEmpty && !line.startsWith("#") =>
        line.split(spliter).length
    }

    numCol match {
      case Some(x:Int) if x == 4 =>
        val typedLines = lines.mapPartitions { iter =>
          iter.collect {
            case line if line.nonEmpty && line(0) != '#' =>
              val lineArray = line.split(spliter)
              if (lineArray.length < 4) {
                throw new IllegalArgumentException("Invalid line: " + line)
              }
              val srcId = lineArray(0).toVertexId
              val dstId = lineArray(2).toVertexId

              srcId -> (dstId, lineArray(1).toVertexType, lineArray(3).toVertexType)
          }
        }.repartition(numPartition)
          .map { case (srcId, (dstId, srcType, dstType)) => (srcId, srcType, dstId, dstType) }

        edgesWithNeighborAttrFromTypedRDD(typedLines, hasEdgesInPartition, edgeDirection,
          canonicalOrientation, storageLevel)
      case Some(x:Int) if x == 5 =>
        val typedLines = lines.mapPartitions { iter =>
          iter.collect {
            case line if line.nonEmpty && line(0) != '#' =>
              val lineArray = line.split(spliter)
              if (lineArray.length < 3) {
                throw new IllegalArgumentException("Invalid line: " + line)
              }
              val srcId = lineArray(0).toVertexId
              val dstId = lineArray(2).toVertexId

              srcId -> (dstId, lineArray(1).toVertexType, lineArray(3).toVertexType, lineArray(4).toWgtTpe)
          }
        }.repartition(numPartition)
          .map { case (srcId, (dstId, srcType, dstType, weight)) => (srcId, srcType, dstId, dstType, weight) }

        edgesWithWeightedNeighborAttrFromTypedRDD(typedLines, hasEdgesInPartition, edgeDirection,
          canonicalOrientation, storageLevel)
      case None =>
        throw new Exception("read data error!")
    }
  }

  def edgesFromTypedRDD[VD: ClassTag : TypeTag](rdd: RDD[(VertexId, VertexType, VertexId, VertexType)],
                                                edgeDirection: EdgeDirection = EdgeDirection.Out,
                                                canonicalOrientation: Boolean = false,
                                                storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): RDD[EdgePartition[VD, Long]] = {

    val edges = rdd.mapPartitions { iter =>
      val builder = new StandardEdgePartitionBuilder[VD, Long](edgeDirection = edgeDirection)
      val edgeAttrBuilder = new EdgeAttributeBuilder()
      iter.foreach { case (srcId, srcType, dstId, dstType) =>
        edgeAttrBuilder.put(srcType, dstType)
        builder.add(Edge[Long](srcId, dstId, edgeAttrBuilder.build))
      }

      Iterator.single(builder.build)
    }

    edges.persist(storageLevel)
  }

  def edgesWithNeighborAttrFromTypedRDD[VD <: Typed : ClassTag : TypeTag]
  (rdd: RDD[(VertexId, VertexType, VertexId, VertexType)],
   hasEdgesInPartition: Boolean = true,
   edgeDirection: EdgeDirection = EdgeDirection.Out,
   canonicalOrientation: Boolean = false,
   storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): RDD[EdgePartition[VD, Long]] = {

    val edges = rdd.mapPartitions { iter =>
      val builder = new TypedEdgePartitionWithNeighborAttrBuilder[VD](
        hasEdges = hasEdgesInPartition,
        edgeDirection = edgeDirection)

      val edgeAttrBuilder = new EdgeAttributeBuilder()
      iter.foreach { case (srcId, srcType, dstId, dstType) =>
        if (canonicalOrientation) {
          if (srcId > dstId) {
            builder.add(srcId, dstId, edgeAttrBuilder.put(srcType, dstType).build)
          }
        } else if (srcId != dstId) {
          builder.add(srcId, dstId, edgeAttrBuilder.put(srcType, dstType).build)
        } else {
          println(s"$srcId = $dstId, error! ")
        }
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
      val builder = new StandardEdgePartitionBuilder[VD, Long](edgeDirection = edgeDirection)
      val edgeAttrBuilder = new EdgeAttributeBuilder()
      iter.foreach { case (srcId, srcType, dstId, dstType, weight) =>
        edgeAttrBuilder.put(srcType, dstType, weight)
        builder.add(Edge[Long](srcId, dstId, edgeAttrBuilder.build))
      }

      Iterator.single(builder.build)
    }

    edges.persist(storageLevel)
  }

  def edgesWithWeightedNeighborAttrFromTypedRDD[VD <: Typed : ClassTag : TypeTag]
  (rdd: RDD[(VertexId, VertexType, VertexId, VertexType, WgtTpe)],
   hasEdgesInPartition: Boolean = true,
   edgeDirection: EdgeDirection = EdgeDirection.Out,
   canonicalOrientation: Boolean = false,
   storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): RDD[EdgePartition[VD, Long]] = {

    val edges = rdd.mapPartitions { iter =>
      val builder = new TypedEdgePartitionWithNeighborAttrBuilder[VD](
        hasEdges = hasEdgesInPartition,
        edgeDirection = edgeDirection)

      val edgeAttrBuilder = new EdgeAttributeBuilder()
      iter.foreach { case (srcId, srcType, dstId, dstType, weight) =>
        if (canonicalOrientation) {
          if (srcId > dstId) {
            builder.add(srcId, dstId, edgeAttrBuilder.put(srcType, dstType, weight).build)
          }
        } else if (srcId != dstId) {
          builder.add(srcId, dstId, edgeAttrBuilder.put(srcType, dstType, weight).build)
        } else {
          println(s"$srcId = $dstId, error! ")
        }

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

  def edgeListFile[VD: ClassTag : TypeTag, ED: ClassTag: TypeTag](sc: SparkContext, path: String,
                                                         numEdgePartition: Int = -1,
                                                         edgeDirection: EdgeDirection = EdgeDirection.Out,
                                                         canonicalOrientation: Boolean = false,
                                                         storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
                                                        ): Graph[VD, ED] = {
    val edges = edgesFromFile[VD, ED](sc, path, numEdgePartition, edgeDirection, canonicalOrientation, storageLevel)

    new Graph(edges)
  }
}
