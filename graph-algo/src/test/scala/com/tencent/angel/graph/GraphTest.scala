package com.tencent.angel.graph

import com.tencent.angel.graph.framework.{EdgeDirection, Graph, NeighN, NeighNW}
import com.tencent.angel.utils.WithSONA

class GraphTest extends WithSONA{

  test("loadData") {
    val path = "E:\\github\\fitzwang\\angel-graph\\data\\cora\\cora.cites"
    val graph = Graph.edgeListFile[Float, Long](sc, path)
    graph.calDegree("degree", direction=EdgeDirection.Both)

    println(graph.psVertices.name)
  }
}
