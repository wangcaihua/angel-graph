package com.tencent.angel.graph.core.psf.common
import java.util

import com.tencent.angel.PartitionKey

import scala.collection.mutable.ListBuffer
import scala.reflect.runtime.universe._

trait Splitter

case class NonSplitter() extends Splitter

class RangeSplitter[T:TypeTag](val part:PartitionKey, val arr: Array[T], val start:Int, val end: Int)
  extends Splitter {
  val tpe = typeOf[T]
}

object RangeSplitter {
  def getSplit[T:TypeTag](arr: Array[T], parts:util.List[PartitionKey]): List[RangeSplitter[T]] = {
    val result = new ListBuffer[RangeSplitter[T]]()

    typeOf[T] match {
      case t if t =:= typeOf[Int] =>
        val nodeIds = arr.asInstanceOf[Array[Int]].sorted

        var partIndex: Int = 0
        var part: PartitionKey = parts.get(partIndex)

        var lastNodeIndex: Int = 0
        var currNodeIndex: Int = 0
        nodeIds.foreach{ value =>
          if (part.getStartCol >= value && value < part.getEndCol) {
            currNodeIndex += 1
          } else {
            // 0. update currNodeIndex
            currNodeIndex += 1

            // 1. add a new split
            if (currNodeIndex - lastNodeIndex > 1)  {
              val split = new RangeSplitter[Int](part, nodeIds, lastNodeIndex, currNodeIndex)
              result.append(split.asInstanceOf[RangeSplitter[T]])
            }

            // 2. update index and part
            partIndex += 1
            part = parts.get(partIndex)
            lastNodeIndex = currNodeIndex
          }
        }
      case t if t =:= typeOf[Long] =>
        val nodeIds = arr.asInstanceOf[Array[Long]].sorted

        var partIndex: Int = 0
        var part: PartitionKey = parts.get(partIndex)

        var lastNodeIndex: Int = 0
        var currNodeIndex: Int = 0
        nodeIds.foreach{ value =>
          if (part.getStartCol >= value && value < part.getEndCol) {
            currNodeIndex += 1
          } else {
            // 0. update currNodeIndex
            currNodeIndex += 1

            // 1. add a new split
            if (currNodeIndex - lastNodeIndex > 1)  {
              val split = new RangeSplitter[Long](part, nodeIds, lastNodeIndex, currNodeIndex)
              result.append(split.asInstanceOf[RangeSplitter[T]])
            }

            // 2. update index and part
            partIndex += 1
            part = parts.get(partIndex)
            lastNodeIndex = currNodeIndex
          }
        }
    }

    result.toList
  }
}