package com.tencent.angel.graph.framework


import java.util.UUID

import com.tencent.angel.graph._
import com.tencent.angel.graph.core.data.{NeighN, NeighNW, NeighTN, NeighTNW, Neighbor}
import com.tencent.angel.graph.core.psf.common.{PSFGUCtx, PSFMCtx, Singular}
import com.tencent.angel.graph.core.psf.get.GetPSF
import com.tencent.angel.graph.core.psf.update.UpdatePSF
import com.tencent.angel.graph.framework.EdgeActiveness.EdgeActiveness
import com.tencent.angel.graph.framework.EdgeDirection.EdgeDirection
import com.tencent.angel.graph.utils.psfConverters._
import com.tencent.angel.graph.utils.{FastHashMap, Logging, PSFUtils, PartitionTypedNeighborBuilder, PartitionUnTypedNeighborBuilder, TypedNeighborBuilder, UnTypedNeighborBuilder}
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.spark.models.PSMatrix
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._


class Graph[VD: ClassTag : TypeTag, ED: ClassTag](val edges: RDD[EdgePartition[VD, ED]])
  extends Serializable with Logging {
  private val psMatrixName = s"PSPartition_${UUID.randomUUID()}"
  logInfo(s"psMatrixName is $psMatrixName")

  val maxVertexId: VertexId = edges.mapPartitions { iter =>
    val part = iter.next()
    Iterator.single(part.maxVertexId)
  }.max()

  val minVertexId: VertexId = edges.mapPartitions { iter =>
    val part = iter.next()
    Iterator.single(part.minVertexId)
  }.min()

  val psVertices: PSMatrix = {
    val psMatrix = PSFUtils.createPSMatrix(psMatrixName, minVertexId, maxVertexId)

    edges.foreachPartition { iter =>
      val edgePartition = iter.next()

      val init = psMatrix.createUpdate { ctx: PSFGUCtx =>
        ctx.put(ctx.getArrayParam)
      }

      init(edgePartition.localVertices)
      init.clear()
    }

    val createPSPartition = psMatrix.createUpdate { ctx: PSFGUCtx =>
      ctx.getOrCreatePartition[VD]
    }
    createPSPartition()
    createPSPartition.clear()

    logInfo("Finished to create PSMatrix")

    psMatrix
  }

  val numPsPartition: Int = PSAgentContext.get.getMatrixMetaManager
    .getPartitions(psVertices.id).size()
  logInfo(s"numPsPartition is $numPsPartition")

  var vertices: RDD[NodePartition[VD]] = edges.sparkContext
    .parallelize(0 until numPsPartition, numPsPartition).mapPartitions { iter =>
    val partId = iter.next()

    val pullAttr = psVertices.createGet { ctx: PSFGUCtx =>
      val pid = ctx.getParam[Singular]
      assert(pid.partition == ctx.partitionId)

      val partition = ctx.getPartition[VD]

      val result = new FastHashMap[VertexId, VD](partition.local2global.length)
      partition.local2global.foreach { vid => result(vid) = partition.getAttr(vid) }
      result
    } { ctx: PSFMCtx =>
      val last = ctx.getLast[FastHashMap[VertexId, VD]]
      val curr = ctx.getCurr[FastHashMap[VertexId, VD]]

      curr.foreach { case (k, v) => last(k) = v }

      last
    }

    val attrs = pullAttr(Singular(partId))
    pullAttr.clear()

    val nodePartition = new NodePartition[VD](attrs)
    Iterator.single(nodePartition)
  }

  private var updateVertexPSF: UpdatePSF = _

  def initVertex[M: TypeTag](vprog: (VD, M) => VD, active:(VD, M) => Boolean, defaultMsg: M): this.type = {
    if (updateVertexPSF == null) {
      updateVertexPSF = psVertices.createUpdate { ctx: PSFGUCtx =>
        val part = ctx.getPartition[VD]
        part.updateAttrs(vprog, active, defaultMsg)
      }
    }

    updateVertexPSF()

    this
  }

  def updateVertex[M: TypeTag](vprog: (VD, M) => VD, active:(VD, M) => Boolean): this.type = {
    initVertex(vprog, active, null.asInstanceOf[M])
  }

  def calDegree(slotName: String, direction: EdgeDirection = EdgeDirection.Both): Unit = {
    edges.foreachPartition { iter =>
      val edgePartition = iter.next()

      val localDegree = new FastHashMap[VertexId, Int]()
      direction match {
        case EdgeDirection.Out =>
          edgePartition.iterator.foreach { edge =>
            localDegree.putMerge(edge.srcId, 1, (v1, v2) => v1 + v2)
          }
        case EdgeDirection.In =>
          edgePartition.iterator.foreach { edge =>
            localDegree.putMerge(edge.dstId, 1, (v1, v2) => v1 + v2)
          }
        case EdgeDirection.Both =>
          edgePartition.iterator.foreach { edge =>
            localDegree.putMerge(edge.srcId, 1, (v1, v2) => v1 + v2)
            localDegree.putMerge(edge.dstId, 1, (v1, v2) => v1 + v2)
          }
        case _ =>
          throw new Exception("direction error")
      }

      val pushDegree = psVertices.createUpdate { ctx: PSFGUCtx =>
        val params = ctx.getMapParam[Int]
        val partition = ctx.getPartition[VD]
        val degree = partition.getOrCreateSlot[Int](slotName)

        def mergeF(v1: Int, v2: Int): Int = v1 + v2

        params.foreach { case (k, v) =>
          degree.putMerge(k, v, mergeF)
        }
      }

      pushDegree(localDegree)
      pushDegree.clear()
    }
  }

  private var activeMessageCountPSF: GetPSF[Int] = _

  def activeMessageCount(): Int = {
    if (activeMessageCountPSF == null) {
      activeMessageCountPSF = psVertices.createGet { ctx: PSFGUCtx =>
        ctx.getPartition[VD].activeVerticesCount()
      } { ctx: PSFMCtx => ctx.getLast[Int] + ctx.getCurr[Int] }
    }

    activeMessageCountPSF()
  }

  def adjacency[N <: Neighbor : ClassTag : TypeTag](direction: EdgeDirection): this.type = {
    edges.foreachPartition { iter =>
      Neighbor.register()

      val edgePartition = iter.next()

      // 1. create Adjacency in spark RDD partition
      var pos = 0
      val adjBuilder = new PartitionUnTypedNeighborBuilder[N](direction)
      typeOf[N] match {
        case nt if nt =:= typeOf[NeighN] =>
          while (pos < edgePartition.size) {
            adjBuilder.add(edgePartition.srcIdFromPos(pos), edgePartition.dstIdFromPos(pos))
            pos += 1
          }
        case nt if nt =:= typeOf[NeighNW] =>
          while (pos < edgePartition.size) {
            adjBuilder.add(edgePartition.srcIdFromPos(pos),
              edgePartition.dstIdFromPos(pos),
              edgePartition.attrs(pos).asInstanceOf[WgtTpe])
            pos += 1
          }
        case _ =>
          throw new Exception("Adjacency data error!")
      }

      // 2. create PSF to push Adjacency (from executor)
      val pushAdjacency = psVertices.createUpdate { ctx: PSFGUCtx =>
        val psPartition = ctx.getPartition[Int]
        val psAdjBuilder = psPartition.getOrCreateSlot[UnTypedNeighborBuilder]("adjacencyBuilder")
        ctx.getMapParam[N].foreach { case (vid, neigh) =>
          if (psAdjBuilder.containsKey(vid)) {
            psAdjBuilder(vid).add(neigh)
          } else {
            val builder = new UnTypedNeighborBuilder()
            builder.add(neigh)
            psAdjBuilder(vid) = builder
          }
        }
      }

      // 3. push adjacency (on executor)
      pushAdjacency(adjBuilder.build)

      // 4. remove PSF (on executor)
      pushAdjacency.clear()
    }

    // create adjacency on PS (from driver)
    val createAdjacency = psVertices.createUpdate { ctx: PSFGUCtx =>
      val psPartition = ctx.getPartition[Int]
      val psAdjBuilder = psPartition.getSlot[UnTypedNeighborBuilder]("adjacencyBuilder")
      if (psAdjBuilder != null) {
        val psAdjacency = psPartition.getOrCreateSlot[N]("adjacency")

        psAdjBuilder.foreach { case (vid, adj) =>
          psAdjacency(vid) = adj.clearAndBuild[N](vid)
        }

        psPartition.removeSlot("adjacencyBuilder")
      } else {
        psPartition.getOrCreateSlot[N]("adjacency")
      }
    }
    createAdjacency()
    createAdjacency.clear() // remove PSF (on driver)

    this
  }

  def typedAdjacency[N <: Neighbor : ClassTag : TypeTag](direction: EdgeDirection): this.type = {
    edges.foreachPartition { iter =>
      Neighbor.register()
      val edgePartition = iter.next()

      // 1. create Adjacency in spark RDD partition
      var pos = 0
      val adjBuilder = new PartitionTypedNeighborBuilder[N](direction)
      typeOf[N] match {
        case nt if nt =:= typeOf[NeighTN] =>
          while (pos < edgePartition.size) {
            val srcId = edgePartition.srcIdFromPos(pos)
            val dstId = edgePartition.dstIdFromPos(pos)
            val attr = edgePartition.attrs(pos).asInstanceOf[Long]
            val srcType = attr.srcType
            val dstType = attr.dstType
            adjBuilder.add(srcId, srcType, dstId, dstType)
            pos += 1
          }
        case nt if nt =:= typeOf[NeighTNW] =>
          while (pos < edgePartition.size) {
            val srcId = edgePartition.srcIdFromPos(pos)
            val dstId = edgePartition.dstIdFromPos(pos)
            val attr = edgePartition.attrs(pos).asInstanceOf[Long]
            val srcType = attr.srcType
            val dstType = attr.dstType
            val weight = attr.weight
            adjBuilder.add(srcId, srcType, dstId, dstType, weight)
            pos += 1
          }
        case _ =>
          throw new Exception("Adjacency data error!")
      }

      // 2. create PSF to push Adjacency (from executor)
      val pushAdjacency = psVertices.createUpdate { ctx: PSFGUCtx =>
        val psPartition = ctx.getPartition[VD]
        val psAdjBuilder = psPartition.getOrCreateSlot[TypedNeighborBuilder]("adjacencyBuilder")
        ctx.getMapParam[N].foreach { case (vid, neigh) =>
          if (psAdjBuilder.containsKey(vid)) {
            psAdjBuilder(vid).add(neigh)
          } else {
            val builder = new TypedNeighborBuilder()
            builder.add(neigh)
            psAdjBuilder(vid) = builder
          }
        }
      }

      // 3. push adjacency (on executor)
      pushAdjacency(adjBuilder.build)

      // 4. remove PSF (on executor)
      pushAdjacency.clear()
    }

    // create adjacency on PS (from driver)
    val createAdjacency = psVertices.createUpdate { ctx: PSFGUCtx =>
      val psPartition = ctx.getPartition[Int]
      val psAdjBuilder = psPartition.getSlot[TypedNeighborBuilder]("adjacencyBuilder")
      if (psAdjBuilder != null) {
        val psAdjacency = psPartition.getOrCreateSlot[N]("adjacency")

        psAdjBuilder.foreach { case (vid, adj) =>
          psAdjacency(vid) = adj.clearAndBuild[N](vid)
        }

        psPartition.removeSlot("adjacencyBuilder")
      } else {
        psPartition.getOrCreateSlot[N]("adjacency")
      }
    }
    createAdjacency()
    createAdjacency.clear() // remove PSF (on driver)

    this
  }

  private[graph] def aggregateMessagesWithActiveSet[M: ClassTag : TypeTag](sendMsg: EdgeContext[VD, ED, M] => Unit,
                                                                           mergeMsg: (M, M) => M,
                                                                           tripletFields: TripletFields,
                                                                           batchSize: Int = -1,
                                                                           activeness: EdgeActiveness): Unit = {
    edges.foreachPartition { iter =>
      val edgePartition: EdgePartition[VD, ED] = iter.next()
      edgePartition.setPSMatrix(psVertices)

      logInfo(s"updateVertexAttrs with batchSize $batchSize")
      edgePartition.updateVertexAttrs(batchSize)

      logInfo(s"updateActiveSet with batchSize $batchSize")
      edgePartition.updateActiveSet(batchSize)

      logInfo(s"aggregateMessagesScan with batchSize $batchSize")
      edgePartition.aggregateMessagesScan(sendMsg, mergeMsg, tripletFields, activeness, batchSize)
    }

  }
}

object Graph {
  def edgeListFile[VD: ClassTag : TypeTag, ED: ClassTag](sc: SparkContext, path: String,
                                                         numEdgePartition: Int = -1,
                                                         edgeDirection: EdgeDirection = EdgeDirection.Both,
                                                         canonicalOrientation: Boolean = false,
                                                         storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
                                                        ): Graph[VD, ED] = {
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

    edges.persist(storageLevel).count()

    new Graph(edges)
  }


  def fromEdgeRDD[VD: ClassTag : TypeTag, ED: ClassTag](rdd: RDD[(VertexId, VertexId)],
                                          edgeDirection: EdgeDirection = EdgeDirection.Out,
                                          canonicalOrientation: Boolean = false,
                                          storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): Graph[VD, ED] = {

    val edges = rdd.mapPartitions { iter =>
      val builder = new EdgePartitionBuilder[VD, ED](edgeDirection = edgeDirection)
      val defaultEdgeAttr = getDefaultEdgeAttr[ED]

      iter.foreach { case (srcId, dstId) =>
        builder.add(Edge(srcId, dstId, defaultEdgeAttr))
      }

      Iterator.single(builder.build)
    }

    edges.persist(storageLevel).count()

    new Graph(edges)
  }


  def fromEdgeWithWeightRDD[VD: ClassTag : TypeTag](rdd: RDD[(VertexId, VertexId, WgtTpe)],
                                                    edgeDirection: EdgeDirection = EdgeDirection.Out,
                                                    canonicalOrientation: Boolean = false,
                                                    storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): Graph[VD, WgtTpe] = {

    val edges = rdd.mapPartitions { iter =>
      val builder = new EdgePartitionBuilder[VD, WgtTpe](edgeDirection = edgeDirection)

      iter.foreach { case (srcId, dstId, weight) =>
        builder.add(Edge[WgtTpe](srcId, dstId, weight))
      }

      Iterator.single(builder.build)
    }

    edges.persist(storageLevel).count()

    new Graph(edges)
  }


  def typedEdgeListFile[VD: ClassTag : TypeTag](sc: SparkContext, path: String,
                                                numEdgePartition: Int = -1,
                                                edgeDirection: EdgeDirection = EdgeDirection.Both,
                                                canonicalOrientation: Boolean = false,
                                                storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
                                               ): Graph[VD, Long] = {
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

    edges.persist(storageLevel).count()

    new Graph(edges)
  }


  def fromTypedEdgeWithWeightRDD[VD: ClassTag : TypeTag](rdd: RDD[(VertexId, VertexType, VertexId, VertexType, WgtTpe)],
                                                         edgeDirection: EdgeDirection = EdgeDirection.Out,
                                                         canonicalOrientation: Boolean = false,
                                                         storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): Graph[VD, Long] = {

    val edges = rdd.mapPartitions { iter =>
      val builder = new EdgePartitionBuilder[VD, Long](edgeDirection = edgeDirection)
      val edgeAttrBuilder = new EdgeAttributeBuilder()
      iter.foreach { case (srcId, srcType, dstId, dstType, weight) =>
        edgeAttrBuilder.put(srcType, dstType, weight)
        builder.add(Edge[Long](srcId, dstId, edgeAttrBuilder.build))
      }

      Iterator.single(builder.build)
    }

    edges.persist(storageLevel).count()

    new Graph(edges)
  }

  def fromTypedEdgeRDD[VD: ClassTag : TypeTag](rdd: RDD[(VertexId, VertexType, VertexId, VertexType)],
                                               edgeDirection: EdgeDirection = EdgeDirection.Out,
                                               canonicalOrientation: Boolean = false,
                                               storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): Graph[VD, Long] = {

    val edges = rdd.mapPartitions { iter =>
      val builder = new EdgePartitionBuilder[VD, Long](edgeDirection = edgeDirection)
      val edgeAttrBuilder = new EdgeAttributeBuilder()
      iter.foreach { case (srcId, srcType, dstId, dstType) =>
        edgeAttrBuilder.put(srcType, dstType)
        builder.add(Edge[Long](srcId, dstId, edgeAttrBuilder.build))
      }

      Iterator.single(builder.build)
    }

    edges.persist(storageLevel).count()

    new Graph(edges)
  }

}

