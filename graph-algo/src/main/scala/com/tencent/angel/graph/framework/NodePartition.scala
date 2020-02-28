package com.tencent.angel.graph.framework

import com.tencent.angel.graph.VertexId
import com.tencent.angel.graph.utils.FastHashMap

import scala.reflect.ClassTag

class NodePartition[VD: ClassTag](val attrs: FastHashMap[VertexId, VD]) {

}
