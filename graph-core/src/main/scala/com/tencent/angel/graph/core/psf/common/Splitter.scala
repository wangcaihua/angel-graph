package com.tencent.angel.graph.core.psf.common

import java.util

import com.tencent.angel.PartitionKey
import com.tencent.angel.graph.VertexId

import scala.collection.mutable.ListBuffer

trait Splitter

case class NonSplitter() extends Splitter

class RangeSplitter(val part: PartitionKey, val arr: Array[VertexId], val start: Int, val end: Int)
  extends Splitter

object RangeSplitter {
  def getSplit(arr: Array[VertexId], parts: util.List[PartitionKey]): List[RangeSplitter] = {
    val result = new ListBuffer[RangeSplitter]()

    var partIndex: Int = 0
    var part: PartitionKey = parts.get(partIndex)

    val nodeIds = arr.sorted
    var lastNodeIndex: Int = 0
    var currNodeIndex: Int = 0
    nodeIds.foreach { value =>
      if (part.getStartCol <= value && value < part.getEndCol) {
        currNodeIndex += 1
      } else {
        // 1. add a new split
        if (currNodeIndex - lastNodeIndex > 1) {
          val split = new RangeSplitter(part, nodeIds, lastNodeIndex, currNodeIndex)
          result.append(split)
        }

        // 2. update index and part
        partIndex += 1
        part = parts.get(partIndex)
        lastNodeIndex = currNodeIndex
        currNodeIndex += 1
      }
    }

    if (currNodeIndex - lastNodeIndex > 1 && currNodeIndex <= part.getEndCol) {
      val split = new RangeSplitter(part, nodeIds, lastNodeIndex, currNodeIndex)
      result.append(split)
    }

    result.toList
  }

}