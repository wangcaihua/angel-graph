package com.tencent.angel.graph.framework


import java.util.UUID

import com.tencent.angel.graph._
import com.tencent.angel.graph.core.psf.common.PSFGUCtx
import com.tencent.angel.graph.framework.EdgeDirection.EdgeDirection
import com.tencent.angel.graph.utils.PSFUtils
import com.tencent.angel.graph.utils.psfConverters._
import com.tencent.angel.spark.models.PSMatrix
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._


class Graph[VD: ClassTag, ED: ClassTag](val edges: RDD[EdgePartition[VD, ED]]) extends Serializable {
  private val psMatrixName = s"PSPartition_${UUID.randomUUID()}"
  var vertices: RDD[NodePartition[VD]] = _

  lazy val maxVertexId: VertexId = edges.mapPartitions { iter =>
    val part = iter.next()
    Iterator.single(part.maxVertexId)
  }.max()

  lazy val minVertexId: VertexId = edges.mapPartitions { iter =>
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
      ctx.getOrCreatePartition[Int]
    }
    createPSPartition()
    createPSPartition.clear()

    psMatrix
  }

  def initUntypedAdjacency[N <: Neighbor : ClassTag : TypeTag](direction: EdgeDirection): this.type = {
    edges.foreachPartition { iter =>
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
      if (classOf[VertexId] == classOf[Long]) {
        pushAdjacency[Long2ObjectOpenHashMap[N]](
          adjBuilder.build[Long2ObjectOpenHashMap[N]]
        )
      } else {
        pushAdjacency[Int2ObjectOpenHashMap[N]](
          adjBuilder.build[Int2ObjectOpenHashMap[N]]
        )
      }

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

  def initTypedAdjacency[N <: Neighbor : ClassTag : TypeTag](direction: EdgeDirection): this.type = {
    edges.foreachPartition { iter =>
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
        val psPartition = ctx.getPartition[Int]
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
      if (classOf[VertexId] == classOf[Long]) {
        pushAdjacency[Long2ObjectOpenHashMap[N]](
          adjBuilder.build[Long2ObjectOpenHashMap[N]]
        )
      } else {
        pushAdjacency[Int2ObjectOpenHashMap[N]](
          adjBuilder.build[Int2ObjectOpenHashMap[N]]
        )
      }

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
}

object Graph {
  def edgeListFile[VD: ClassTag](sc: SparkContext, path: String,
                                 numEdgePartition: Int = -1,
                                 edgeDirection: EdgeDirection = EdgeDirection.Both,
                                 canonicalOrientation: Boolean = false,
                                 storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
                                ): Graph[VD, WgtTpe] = {
    val lines = if (numEdgePartition > 0) {
      sc.textFile(path, numEdgePartition).coalesce(numEdgePartition)
    } else {
      sc.textFile(path)
    }

    val edges = lines.mapPartitions { iter =>
      val builder = new EdgePartitionBuilder[VD, WgtTpe](edgeDirection = edgeDirection)
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
                builder.add(Edge(dstId, srcId, randWeight))
              }
            } else if (srcId != dstId) {
              builder.add(Edge(srcId, dstId, randWeight))
            } else {
              println(s"$srcId = $dstId, error! ")
            }
          } else if (lineArray.length == 3) {
            val srcId = lineArray(0).toVertexId
            val dstId = lineArray(1).toVertexId
            val weight = lineArray(3).toWgtTpe
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


  def fromEdgeRDD[VD: ClassTag](rdd: RDD[(VertexId, VertexId)],
                                edgeDirection: EdgeDirection = EdgeDirection.Out,
                                canonicalOrientation: Boolean = false,
                                storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): Graph[VD, WgtTpe] = {

    val edges = rdd.mapPartitions { iter =>
      val builder = new EdgePartitionBuilder[VD, WgtTpe](edgeDirection = edgeDirection)

      iter.foreach { case (srcId, dstId) =>
        builder.add(Edge[WgtTpe](srcId, dstId, defaultWeight))
      }

      Iterator.single(builder.build)
    }

    edges.persist(storageLevel).count()

    new Graph(edges)
  }


  def fromEdgeRDD[VD: ClassTag](rdd: RDD[(VertexId, VertexId, WgtTpe)],
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


  def typedEdgeListFile[VD: ClassTag](sc: SparkContext, path: String,
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


  def fromTypedEdgeRDD[VD: ClassTag](rdd: RDD[(VertexId, VertexType, VertexId, VertexType, WgtTpe)],
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

  def fromTypedEdgeRDD[VD: ClassTag](rdd: RDD[(VertexId, VertexType, VertexId, VertexType)],
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

