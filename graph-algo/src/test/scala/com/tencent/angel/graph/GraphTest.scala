package com.tencent.angel.graph

import java.io.FileWriter
import java.util.Random

import com.tencent.angel.graph.algo.pangerank.PageRank
import com.tencent.angel.graph.algo.svdpp.{SVDPPVD, SVDPlusPlus}
import com.tencent.angel.graph.core.data.NeighN
import com.tencent.angel.graph.algo.walker.deepwalk.DeepWalk
import com.tencent.angel.graph.algo.walker.metapath2vec.MetaPath2Vec
import com.tencent.angel.graph.algo.walker.node2vec.Node2Vec
import com.tencent.angel.graph.core.data.{NeighN, NeighNW, NeighTN, NeighTNW}
import com.tencent.angel.graph.framework.{EdgeDirection, Graph, GraphLoader}
import com.tencent.angel.utils.WithSONA


class GraphTest extends WithSONA {
  val pathBase: String = "../data/cora"
  val path: String = s"$pathBase/cora.cites"

  test("add weight") {
    val data = sc.textFile(path).map { line =>
      val pair = line.split("\t").map(_.toLong)
      pair.head -> pair(1)
    }

    val outFile = s"$pathBase/cora.cites.weighted"
    val fw = new FileWriter(outFile)
    val rand = new Random()
    data.collect().foreach { case (src, dst) =>
      fw.write(f"$src\t$dst\t${rand.nextDouble() % .6f}\n")
    }

    fw.flush()
    fw.close()
  }

  test("add type") {
    val data = sc.textFile(path).map { line =>
      val pair = line.split("\t").map(_.toLong)
      pair.head -> pair(1)
    }

    val typeFlag = data.flatMap(pair => Iterator(pair._1, pair._2)).distinct()
      .map(id => (id, (id % 5).toShort)).cache()

    val result = data.join(typeFlag).map { case (src, (dst, srcType)) =>
      dst -> (src, srcType)
    }.join(typeFlag).map { case (dst, ((src, srcType), dstType)) =>
      (src, srcType, dst, dstType)
    }

    val outFile = s"$pathBase/cora.cites.weighted"
    val fw = new FileWriter(outFile)
    result.collect().foreach { case (src, srcType, dst, dstType) =>
      fw.write(f"$src\t$srcType\t$dst\t$dstType\n")
    }

    fw.flush()
    fw.close()
  }

  test("add type and weight") {
    val data = sc.textFile(path).map { line =>
      val pair = line.split("\t").map(_.toLong)
      pair.head -> pair(1)
    }

    val typeFlag = data.flatMap(pair => Iterator(pair._1, pair._2)).distinct()
      .map(id => (id, (id % 5).toShort)).cache()

    val result = data.join(typeFlag).map { case (src, (dst, srcType)) =>
      dst -> (src, srcType)
    }.join(typeFlag).map { case (dst, ((src, srcType), dstType)) =>
      (src, srcType, dst, dstType)
    }

    val outFile = s"$pathBase/cora.cites.weighted"
    val fw = new FileWriter(outFile)
    val rand = new Random()
    result.collect().foreach { case (src, srcType, dst, dstType) =>
      fw.write(f"$src\t$srcType\t$dst\t$dstType\t${rand.nextDouble() % .6f}\n")
    }

    fw.flush()
    fw.close()
  }

  test("edgeListFile") {
    val edge = GraphLoader.edgeListFile[Float, Long](sc, path)
  }

  test("edgesWithNeighborAttrFromFile") {
    val edges = GraphLoader.edgesWithNeighborAttrFromFile[NeighNW](sc, s"$pathBase/cora.cites.weighted",
      hasEdgesInPartition = false)

    edges.foreachPartition { iter =>
      val edgePartition = iter.next()
      edgePartition.maxVertexId
    }
  }

  test("edgesWithNeighborSlotFromFile") {
    val edges = GraphLoader.edgesWithNeighborSlotFromFile[Byte, NeighNW](sc, s"$pathBase/cora.cites",
      hasEdgesInPartition = true)

    edges.foreachPartition { iter =>
      val edgePartition = iter.next()

      edgePartition.maxVertexId
    }
  }

  test("typedEdgesWithNeighborAttrFromFile") {
    val edges = GraphLoader.typedEdgesWithNeighborAttrFromFile[NeighTN](sc, s"$pathBase/cora.cites.typed.weighted",
      hasEdgesInPartition = false)

    edges.foreachPartition { iter =>
      val edgePartition = iter.next()

      edgePartition.maxVertexId
    }
  }

  test("cache") {
    val graph = GraphLoader.edgeListFile[Float, Long](sc, path)
    graph.edges.cache()
    val edges2 = graph.edges.mapPartitions { iter =>
      val edgePartition = iter.next()
      (0 until 10).foreach { idx =>
        edgePartition.localSrcIds(idx) = -idx
      }

      Iterator.single(edgePartition)
    }

    edges2.count()

    val edges3 = graph.edges.mapPartitions { iter =>
      val edgePartition = iter.next()
      (0 until 10).foreach { idx =>
        assert(edgePartition.localSrcIds(idx) == -idx)
      }

      Iterator.single(edgePartition)
    }

    edges3.count()
  }

  test("calDegree") {
    val graph = GraphLoader.edgeListFile[Float, Long](sc, path)
    graph.calDegree("degree", direction = EdgeDirection.Both)

    println(graph.psVertices.name)
  }

  test("adjacency") {
    val graph = GraphLoader.edgeListFile[Float, Long](sc, path)
    graph.computeAndPushNeigh2Slot[NeighN](direction = EdgeDirection.Both)

    println(graph.psVertices.name)
  }

  test("PageRank") {
    val graph = GraphLoader.edgeListFile[Float, Long](sc, path)
    PageRank(graph, 100, 500)

    println("OK")
  }

  test("SVD++") {
    val svdppErr = 8.0
    val graph = GraphLoader
      .graphFromFile[SVDPPVD, Double](sc, "../data/als-test.data", 2)

    val conf = new SVDPlusPlus.Conf(10, 2, 0.0, 5.0, 0.007,
      0.007, 0.005, 0.015) // 2 iterations
    val svd = SVDPlusPlus(graph, conf)
    val err = svd.vertices.mapPartitions { iter =>
      val nodePartition = iter.next()
      nodePartition.iterator.flatMap { node =>
        val (vid, vd) = (node.id, node.attr)
        if (vid > 5) Iterator.single(vd.v4) else Iterator.single(0.0)
      }
    }.reduce(_ + _) / graph.numEdges
    assert(err <= svdppErr)
  }

  test("deep walk") {
    val edges = GraphLoader.edgesWithNeighborAttrFromFile[NeighNW](sc, s"$pathBase/cora.cites.weighted",
      hasEdgesInPartition = false, edgeDirection = EdgeDirection.Both)

    val graph = Graph(edges)
    graph.pushNeighInAttr()
    graph.pullNeigh2Attr()

//    val dw = new DeepWalk(graph)

//    dw.samplePath()
  }

  test("node2vec") {
    val edges = GraphLoader.edgesWithNeighborAttrFromFile[NeighNW](sc, s"$pathBase/cora.cites.weighted",
      hasEdgesInPartition = false, edgeDirection = EdgeDirection.Both)

    val graph = Graph(edges)
    graph.pushNeighInAttr()
    graph.pullNeigh2Attr()

//    val dw = new Node2Vec(graph)

//    dw.samplePath()
  }

  test("metapath2vec") {
    println("start to load file")
    val edges = GraphLoader.typedEdgesWithNeighborAttrFromFile[NeighTNW](sc, s"$pathBase/cora.cites.typed.weighted",
      hasEdgesInPartition = false, edgeDirection = EdgeDirection.Both)
    println("start to create graph")
    val graph = Graph(edges)
    println("start push neighbors")
    graph.pushNeighInAttr()
    println("start pull neighbors")
    graph.pullNeigh2Attr()

    //val dw = new MetaPath2Vec(graph, Array[VertexType](1.toShort, 2.toShort, 3.toShort))

    //dw.samplePath()

    println("OK")
  }
}
