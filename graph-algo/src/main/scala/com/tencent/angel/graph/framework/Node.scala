package com.tencent.angel.graph.framework

import com.tencent.angel.graph.VertexId

case class Node[VD](id: VertexId, attr: VD)
