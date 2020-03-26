package com.tencent.angel.graph.framework

import com.tencent.angel.graph.VertexId
import com.tencent.angel.graph.utils.FastHashMap

import scala.reflect.ClassTag

class NodePartition[VD: ClassTag](val attrs: FastHashMap[VertexId, VD]) {

  def size(): Int = attrs.size()

  def iterator: Iterator[Node[VD]] = new Iterator[Node[VD]] {
    private val inner = attrs.iterator
    override def hasNext: Boolean = inner.hasNext

    override def next(): Node[VD] = {
      val (vid, attr) = inner.next()
      Node(vid, attr)
    }
  }
}
