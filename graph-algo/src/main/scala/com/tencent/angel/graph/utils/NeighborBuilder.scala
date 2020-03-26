package com.tencent.angel.graph.utils

import com.tencent.angel.graph.core.data.{Typed, UnTyped, _}
import com.tencent.angel.graph.framework.EdgeDirection
import com.tencent.angel.graph.framework.EdgeDirection.EdgeDirection
import com.tencent.angel.graph.{VertexId, VertexType, WgtTpe}

import scala.reflect.ClassTag


class PartitionUnTypedNeighborBuilder[N <: UnTyped : ClassTag]
(direction: EdgeDirection, private val neighTable: FastHashMap[VertexId, UnTypedNeighborBuilder[N]]) {
  val neighClass: Class[_] = implicitly[ClassTag[N]].runtimeClass

  private def createBuilder(): UnTypedNeighborBuilder[N] = {
    val builder = neighClass match {
      case clz if clz == classOf[NeighN] => NeighN.builder()
      case clz if clz == classOf[NeighNW] => NeighNW.builder()
    }

    builder.asInstanceOf[UnTypedNeighborBuilder[N]]
  }

  def this(direction: EdgeDirection) = {
    this(direction, new FastHashMap[VertexId, UnTypedNeighborBuilder[N]]())
  }

  def add(src: VertexId, dst: VertexId): this.type = {
    direction match {
      case EdgeDirection.Both =>
        if (neighTable.containsKey(src)) {
          neighTable(src).add(dst)
        } else {
          val neighborBuilder = createBuilder()
          neighborBuilder.add(dst)
          neighTable(src) = neighborBuilder
        }

        if (neighTable.containsKey(dst)) {
          neighTable(dst).add(src)
        } else {
          val neighborBuilder = createBuilder()
          neighborBuilder.add(src)
          neighTable(dst) = neighborBuilder
        }
      case EdgeDirection.Out =>
        if (neighTable.containsKey(src)) {
          neighTable(src).add(dst)
        } else {
          val neighborBuilder = createBuilder()
          neighborBuilder.add(dst)
          neighTable(src) = neighborBuilder
        }
      case EdgeDirection.In =>
        if (neighTable.containsKey(dst)) {
          neighTable(dst).add(src)
        } else {
          val neighborBuilder = createBuilder()
          neighborBuilder.add(src)
          neighTable(dst) = neighborBuilder
        }
      case _ =>
        throw new Exception("EdgeDirection not support!")
    }
    this
  }

  def add(src: VertexId, dst: VertexId, weight: WgtTpe): this.type = {
    direction match {
      case EdgeDirection.Both =>
        if (neighTable.containsKey(src)) {
          neighTable(src).add(dst, weight)
        } else {
          val neighborBuilder = createBuilder()
          neighborBuilder.add(dst, weight)
          neighTable(src) = neighborBuilder
        }

        if (neighTable.containsKey(dst)) {
          neighTable(dst).add(src, weight)
        } else {
          val neighborBuilder = createBuilder()
          neighborBuilder.add(src, weight)
          neighTable(dst) = neighborBuilder
        }
      case EdgeDirection.Out =>
        if (neighTable.containsKey(src)) {
          neighTable(src).add(dst, weight)
        } else {
          val neighborBuilder = createBuilder()
          neighborBuilder.add(dst, weight)
          neighTable(src) = neighborBuilder
        }
      case EdgeDirection.In =>
        if (neighTable.containsKey(dst)) {
          neighTable(dst).add(src, weight)
        } else {
          val neighborBuilder = createBuilder()
          neighborBuilder.add(src, weight)
          neighTable(dst) = neighborBuilder
        }
      case _ =>
        throw new Exception("EdgeDirection not support!")
    }
    this
  }

  def build: FastHashMap[VertexId, N] = {
    neighTable.mapValues[N](value => value.build)
  }

  def build(fast: FastHashMap[VertexId, N]): Unit = {
    neighTable.foreach { case (key, value) => fast(key) = value.build }
  }
}

class PartitionTypedNeighborBuilder[N <: Typed: ClassTag]
(direction: EdgeDirection, private val neighTable: FastHashMap[VertexId, TypedNeighborBuilder[N]]) {
  val neighClass: Class[_] = implicitly[ClassTag[N]].runtimeClass

  private def createBuilder(): TypedNeighborBuilder[N] = {
    val builder = neighClass match {
      case clz if clz == classOf[NeighTN] => NeighTN.builder()
      case clz if clz == classOf[NeighTNW] => NeighTNW.builder()
    }

    builder.asInstanceOf[TypedNeighborBuilder[N]]
  }

  def this(direction: EdgeDirection) = {
    this(direction, new FastHashMap[VertexId, TypedNeighborBuilder[N]]())
  }

  def add(src: VertexId, srcType: VertexType, dst: VertexId, dstType: VertexType): this.type = {
    direction match {
      case EdgeDirection.Both =>
        if (neighTable.containsKey(src)) {
          neighTable(src).add(dstType, dst)
        } else {
          val neighborBuilder = createBuilder()
          neighborBuilder.add(dstType, dst)
          neighborBuilder.setTpe(srcType)
          neighTable(src) = neighborBuilder
        }

        if (neighTable.containsKey(dst)) {
          neighTable(dst).add(srcType, src)
        } else {
          val neighborBuilder = createBuilder()
          neighborBuilder.add(srcType, src)
          neighborBuilder.setTpe(dstType)
          neighTable(dst) = neighborBuilder
        }
      case EdgeDirection.Out =>
        if (neighTable.containsKey(src)) {
          neighTable(src).add(dstType, dst)
        } else {
          val neighborBuilder = createBuilder()
          neighborBuilder.add(dstType, dst)
          neighborBuilder.setTpe(srcType)
          neighTable(src) = neighborBuilder
        }
      case EdgeDirection.In =>
        if (neighTable.containsKey(dst)) {
          neighTable(dst).add(srcType, src)
        } else {
          val neighborBuilder = createBuilder()
          neighborBuilder.add(srcType, src)
          neighborBuilder.setTpe(dstType)
          neighTable(dst) = neighborBuilder
        }
      case _ =>
        throw new Exception("EdgeDirection not support!")
    }
    this
  }

  def add(src: VertexId, srcType: VertexType, dst: VertexId, dstType: VertexType, weight: WgtTpe): this.type = {
    direction match {
      case EdgeDirection.Both =>
        if (neighTable.containsKey(src)) {
          neighTable(src).add(dstType, dst, weight)
        } else {
          val neighborBuilder = createBuilder()
          neighborBuilder.add(dstType, dst, weight)
          neighborBuilder.setTpe(srcType)
          neighTable(src) = neighborBuilder
        }

        if (neighTable.containsKey(dst)) {
          neighTable(dst).add(srcType, src, weight)
        } else {
          val neighborBuilder = createBuilder()
          neighborBuilder.add(srcType, src, weight)
          neighborBuilder.setTpe(dstType)
          neighTable(dst) = neighborBuilder
        }
      case EdgeDirection.Out =>
        if (neighTable.containsKey(src)) {
          neighTable(src).add(dstType, dst, weight)
        } else {
          val neighborBuilder = createBuilder()
          neighborBuilder.add(dstType, dst, weight)
          neighborBuilder.setTpe(srcType)
          neighTable(src) = neighborBuilder
        }
      case EdgeDirection.In =>
        if (neighTable.containsKey(dst)) {
          neighTable(dst).add(srcType, src, weight)
        } else {
          val neighborBuilder = createBuilder()
          neighborBuilder.add(srcType, src, weight)
          neighborBuilder.setTpe(dstType)
          neighTable(dst) = neighborBuilder
        }
      case _ =>
        throw new Exception("EdgeDirection not support!")
    }
    this
  }

  def build: FastHashMap[VertexId, N] = {
    neighTable.mapValues[N](value => value.build)
  }

  def build(fast: FastHashMap[VertexId, N]): Unit = {
    neighTable.foreach { case (key, value) => fast(key) = value.build }
  }
}
