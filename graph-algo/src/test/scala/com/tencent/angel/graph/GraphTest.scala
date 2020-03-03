package com.tencent.angel.graph

import com.tencent.angel.graph.algo.pangerank.PageRank
import com.tencent.angel.graph.framework.{EdgeDirection, Graph}
import com.tencent.angel.utils.WithSONA

class GraphTest extends WithSONA{
  val path: String = "E:\\github\\fitzwang\\angel-graph\\data\\cora\\cora.cites"

  test("loadData") {
    val graph = Graph.edgeListFile[Float, Long](sc, path)
    graph.calDegree("degree", direction=EdgeDirection.Both)

    println(graph.psVertices.name)
  }

  test("PageRank") {
    val graph = Graph.edgeListFile[Float, Long](sc, path)
    PageRank(graph, 100, 500)

    println("OK")
  }
}
