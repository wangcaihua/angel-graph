package com.tencent.angel.graph.algo.walker.node2vec

import com.tencent.angel.graph.core.data.Neighbor
import com.tencent.angel.graph.framework.EdgeDirection.EdgeDirection
import com.tencent.angel.graph.framework.{EdgeDirection, Graph}
import com.tencent.angel.graph.utils.Logging

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

class Node2Vec[N <: Neighbor : ClassTag : TypeTag](graph: Graph[N, Byte]) extends Logging with Serializable {

  def apply(graph: Graph[N, Byte], pathLength: Int, epoch: Int,
            batchSize: Int, pValue: Float, qValue: Float,
            activeDirection: EdgeDirection = EdgeDirection.Both): Graph[N, Byte] = {

    null
  }
}
