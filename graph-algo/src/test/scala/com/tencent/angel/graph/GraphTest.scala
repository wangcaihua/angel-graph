package com.tencent.angel.graph

import com.tencent.angel.graph.algo.pangerank.PageRank
import com.tencent.angel.graph.core.data.NeighN
import com.tencent.angel.graph.framework.{EdgeDirection, Graph, GraphLoader}
import com.tencent.angel.utils.WithSONA

class GraphTest extends WithSONA{
  val path: String = "E:\\github\\fitzwang\\angel-graph\\data\\cora\\cora.cites"

  test("cache") {
    val graph = GraphLoader.edgeListFile[Float, Long](sc, path)
    graph.edges.cache()
    val edges2 = graph.edges.mapPartitions{ iter =>
      val edgePartition = iter.next()
      (0 until 10).foreach{ idx =>
        edgePartition.localSrcIds(idx) = -idx
      }

      Iterator.single(edgePartition)
    }

    edges2.count()

    val edges3 =graph.edges.mapPartitions{ iter =>
      val edgePartition = iter.next()
      (0 until 10).foreach{ idx =>
        assert(edgePartition.localSrcIds(idx) == -idx)
      }

      Iterator.single(edgePartition)
    }

    edges3.count()
  }

  test("calDegree") {
    val graph = GraphLoader.edgeListFile[Float, Long](sc, path)
    graph.calDegree("degree", direction=EdgeDirection.Both)

    println(graph.psVertices.name)
  }

  test("adjacency") {
    val graph = GraphLoader.edgeListFile[Float, Long](sc, path)
    graph.adjacency[NeighN](direction=EdgeDirection.Both)

    println(graph.psVertices.name)
  }

  test("PageRank") {
    val graph = GraphLoader.edgeListFile[Float, Long](sc, path)
    PageRank(graph, 100, 500)

    println("OK")
  }
}
