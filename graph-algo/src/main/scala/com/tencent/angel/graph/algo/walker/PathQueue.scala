package com.tencent.angel.graph.algo.walker

import com.tencent.angel.graph.VertexId
import com.tencent.angel.graph.utils.FastArray

import scala.collection.mutable

class PathQueue(val pathLength: Int) {
  private val queue = new mutable.Queue[FastArray[VertexId]]()

  def pushBatch(batch: Array[FastArray[VertexId]]): this.type = synchronized {
    batch.foreach { e => queue.enqueue(e) }
    this
  }

  def popBath(num: Int): Array[FastArray[VertexId]] = synchronized {
    val buf = new FastArray[FastArray[VertexId]](num)
    var count = 0
    while (queue.nonEmpty && count < num) {
      val array = queue.dequeue()
      buf += array
      count += 1
    }

    buf.trim().array
  }

  def isEmpty: Boolean = synchronized {
    queue.isEmpty
  }

  def clear(): Unit = synchronized {
    queue.clear()
  }
}
