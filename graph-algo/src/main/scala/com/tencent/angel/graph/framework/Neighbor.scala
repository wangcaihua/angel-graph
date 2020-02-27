package com.tencent.angel.graph.framework


import com.tencent.angel.graph.core.data.GData
import com.tencent.angel.graph.core.sampler._
import com.tencent.angel.graph.framework.EdgeDirection.EdgeDirection
import com.tencent.angel.graph.utils.{FastArray, FastHashMap}
import com.tencent.angel.graph.{VertexId, VertexType, WgtTpe}
import it.unimi.dsi.fastutil.shorts.Short2ObjectOpenHashMap

import scala.reflect.ClassTag

trait Neighbor extends GData

trait UnTyped {
  def sample(): VertexId

  def sample(k: Int): Array[VertexId]

  def hasNeighbor(neigh: VertexId): Boolean = ???
}

trait Typed {
  def sample(tpe: VertexType): VertexId

  def sample(tpe: VertexType, k: Int): Array[VertexId]

  def hasNeighbor(tpe: VertexType, neigh: VertexId): Boolean = ???
}

// 1. untyped node
case class NeighN(neighs: Array[VertexId]) extends Neighbor with UnTyped {
  def this() = this(null)

  @transient private lazy val sample1: SampleOne = new Simple(neighs)
  @transient private lazy val samplek: SampleK = new Reservoir(neighs)

  override def sample(): VertexId = sample1.sample()

  override def sample(k: Int): Array[VertexId] = samplek.sample(k)
}

case class NeighNW(neighs: Array[VertexId], weights: Array[WgtTpe])
  extends Neighbor with UnTyped {
  def this() = this(null, null)

  @transient private lazy val sample1: SampleOne = new BetWheel(neighs, weights)
  @transient private lazy val samplek: SampleK = new ARes(neighs, weights)

  override def sample(): VertexId = sample1.sample()

  override def sample(k: Int): Array[VertexId] = samplek.sample(k)
}

class UnTypedNeighborBuilder {
  private val neighs = new FastArray[VertexId]()
  private val weights = new FastArray[WgtTpe]()

  def add(neigh: VertexId): this.type = {
    neighs += neigh
    this
  }

  def add(neigh: VertexId, weight: WgtTpe): this.type = {
    neighs += neigh
    weights += weight
    this
  }

  def add[N](neigh: N): this.type = {
    neigh match {
      case ne: NeighN =>
        ne.neighs.foreach(n => neighs += n)
      case ne: NeighNW =>
        ne.neighs.foreach(n => neighs += n)
        ne.weights.foreach(w => weights += w)
    }

    this
  }

  def build[N: ClassTag]: N = {
    implicitly[ClassTag[N]].runtimeClass match {
      case nt if nt == classOf[NeighNW] =>
        assert(neighs.size == weights.size)
        NeighNW(neighs.trim().array, weights.trim().array)
          .asInstanceOf[N]
      case _ =>
        NeighN(neighs.trim().array)
          .asInstanceOf[N]
    }
  }

  def clearAndBuild[N: ClassTag](key: VertexId): N = {
    // remove duplicate && remove key
    implicitly[ClassTag[N]].runtimeClass match {
      case nt if nt == classOf[NeighNW] =>
        assert(neighs.size == weights.size)
        val sorted = neighs.trim().array.zip(weights.trim().array).sortBy { case (n, _) => n }
        val tempN = new FastArray[VertexId]
        val tempW = new FastArray[WgtTpe]

        var last = sorted.head._1
        tempN += last
        tempW += sorted.head._2

        sorted.foreach { case (vid, w) =>
          if (vid != key && vid != last) {
            tempN += vid
            tempW += w
            last = vid
          }
        }

        NeighNW(tempN.trim().array, tempW.trim().array)
          .asInstanceOf[N]
      case _ =>
        val sorted = neighs.trim().array.sorted
        val tempN = new FastArray[VertexId]

        var last = sorted.head
        tempN += last
        sorted.foreach { vid =>
          if (vid != key && vid != last) {
            tempN += vid
            last = vid
          }
        }
        NeighN(tempN.trim().array).asInstanceOf[N]
    }
  }
}

class PartitionUnTypedNeighborBuilder[N <: Neighbor : ClassTag]
(direction: EdgeDirection, private val neighTable: FastHashMap[VertexId, UnTypedNeighborBuilder]) {
  def this(direction: EdgeDirection) = {
    this(direction, new FastHashMap[VertexId, UnTypedNeighborBuilder]())
  }

  def add(src: VertexId, dst: VertexId): this.type = {
    direction match {
      case EdgeDirection.Both =>
        if (neighTable.containsKey(src)) {
          neighTable(src).add(dst)
        } else {
          val neighborBuilder = new UnTypedNeighborBuilder()
          neighborBuilder.add(dst)
          neighTable(src) = neighborBuilder
        }

        if (neighTable.containsKey(dst)) {
          neighTable(dst).add(src)
        } else {
          val neighborBuilder = new UnTypedNeighborBuilder()
          neighborBuilder.add(src)
          neighTable(dst) = neighborBuilder
        }
      case EdgeDirection.Out =>
        if (neighTable.containsKey(src)) {
          neighTable(src).add(dst)
        } else {
          val neighborBuilder = new UnTypedNeighborBuilder()
          neighborBuilder.add(dst)
          neighTable(src) = neighborBuilder
        }
      case EdgeDirection.In =>
        if (neighTable.containsKey(dst)) {
          neighTable(dst).add(src)
        } else {
          val neighborBuilder = new UnTypedNeighborBuilder()
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
          val neighborBuilder = new UnTypedNeighborBuilder()
          neighborBuilder.add(dst, weight)
          neighTable(src) = neighborBuilder
        }

        if (neighTable.containsKey(dst)) {
          neighTable(dst).add(src, weight)
        } else {
          val neighborBuilder = new UnTypedNeighborBuilder()
          neighborBuilder.add(src, weight)
          neighTable(dst) = neighborBuilder
        }
      case EdgeDirection.Out =>
        if (neighTable.containsKey(src)) {
          neighTable(src).add(dst, weight)
        } else {
          val neighborBuilder = new UnTypedNeighborBuilder()
          neighborBuilder.add(dst, weight)
          neighTable(src) = neighborBuilder
        }
      case EdgeDirection.In =>
        if (neighTable.containsKey(dst)) {
          neighTable(dst).add(src, weight)
        } else {
          val neighborBuilder = new UnTypedNeighborBuilder()
          neighborBuilder.add(src, weight)
          neighTable(dst) = neighborBuilder
        }
      case _ =>
        throw new Exception("EdgeDirection not support!")
    }
    this
  }

  def build[T]: T = {
    neighTable.mapValues[N](value => value.build[N]).toUnimi[T]
  }
}

// 2. typed node
case class NeighTN(tpe: VertexType, neighs: Short2ObjectOpenHashMap[Array[VertexId]])
  extends Neighbor with Typed {
  def this() = this(0.toShort, null)

  @transient private lazy val sample1: Short2ObjectOpenHashMap[SampleOne] =
    new Short2ObjectOpenHashMap[SampleOne](neighs.size())
  @transient private lazy val samplek: Short2ObjectOpenHashMap[SampleK] =
    new Short2ObjectOpenHashMap[SampleK](neighs.size())

  override def sample(tpe: VertexType): VertexId = {
    if (sample1.containsKey(tpe)) {
      sample1.get(tpe).sample()
    } else if (neighs.containsKey(tpe)) {
      val sampler = new Simple(neighs.get(tpe))
      sample1.put(tpe, sampler)
      sampler.sample()
    } else {
      throw new Exception(s"the type $tpe is not exist!")
    }
  }

  override def sample(tpe: VertexType, k: Int): Array[VertexId] = {
    if (samplek.containsKey(tpe)) {
      samplek.get(tpe).sample(k)
    } else if (neighs.containsKey(tpe)) {
      val sampler = new Reservoir(neighs.get(tpe))
      samplek.put(tpe, sampler)
      sampler.sample(k)
    } else {
      throw new Exception(s"the type $tpe is not exist!")
    }
  }
}

case class NeighTNW(tpe: VertexType, neighs: Short2ObjectOpenHashMap[Array[VertexId]],
                    weights: Short2ObjectOpenHashMap[Array[WgtTpe]])
  extends Neighbor with Typed {
  def this() = this(0.toShort, null, null)

  @transient private lazy val sample1: Short2ObjectOpenHashMap[SampleOne] =
    new Short2ObjectOpenHashMap[SampleOne](neighs.size())
  @transient private lazy val samplek: Short2ObjectOpenHashMap[SampleK] =
    new Short2ObjectOpenHashMap[SampleK](neighs.size())

  override def sample(tpe: VertexType): VertexId = {
    if (sample1.containsKey(tpe)) {
      sample1.get(tpe).sample()
    } else if (neighs.containsKey(tpe)) {
      val sampler = new BetWheel(neighs.get(tpe), weights.get(tpe))
      sample1.put(tpe, sampler)
      sampler.sample()
    } else {
      throw new Exception(s"the type $tpe is not exist!")
    }
  }

  override def sample(tpe: VertexType, k: Int): Array[VertexId] = {
    if (samplek.containsKey(tpe)) {
      samplek.get(tpe).sample(k)
    } else if (neighs.containsKey(tpe)) {
      val sampler = new ARes(neighs.get(tpe), weights.get(tpe))
      samplek.put(tpe, sampler)
      sampler.sample(k)
    } else {
      throw new Exception(s"the type $tpe is not exist!")
    }
  }
}

class TypedNeighborBuilder {
  private var tpe: VertexType = 0.toShort
  private val neighs = new FastHashMap[VertexType, FastArray[VertexId]]()
  private val weights = new FastHashMap[VertexType, FastArray[WgtTpe]]()

  def setTpe(tpe: VertexType): this.type = {
    this.tpe = tpe
    this
  }

  def add(vt: VertexType, neigh: VertexId): this.type = {
    if (neighs.containsKey(vt)) {
      neighs(vt) += neigh
    } else {
      val temp = new FastArray[VertexId]()
      temp += neigh
      neighs(vt) = temp
    }
    this
  }

  def add(vt: VertexType, neigh: VertexId, weight: WgtTpe): this.type = {
    if (neighs.containsKey(vt)) {
      neighs(vt) += neigh
      weights(vt) += weight
    } else {
      val tempN = new FastArray[VertexId]()
      tempN += neigh
      neighs(vt) = tempN

      val tempW = new FastArray[WgtTpe]()
      tempW += weight
      weights(vt) = tempW
    }

    this
  }

  def add[N](neigh: N): this.type = {
    neigh match {
      case tn: NeighTN =>
        tpe = tn.tpe
        val iter = tn.neighs.short2ObjectEntrySet().fastIterator()
        while (iter.hasNext) {
          val entry = iter.next()
          val vt = entry.getShortKey
          val vtNeighs = entry.getValue

          if (neighs.containsKey(vt)) {
            vtNeighs.foreach { n => neighs(vt) += n }
          } else {
            val tempN = new FastArray[VertexId]()
            vtNeighs.foreach { n => tempN += n }
            neighs(vt) = tempN
          }
        }
      case tn: NeighTNW =>
        tpe = tn.tpe
        val iter = tn.neighs.short2ObjectEntrySet().fastIterator()
        while (iter.hasNext) {
          val entry = iter.next()
          val vt = entry.getShortKey
          val vtNeighs = entry.getValue
          val vtWeights = tn.weights.get(vt)

          if (neighs.containsKey(vt)) {
            vtNeighs.foreach { n => neighs(vt) += n }
            vtWeights.foreach { w => weights(vt) += w }
          } else {
            val tempN = new FastArray[VertexId]()
            vtNeighs.foreach { n => tempN += n }
            neighs(vt) = tempN

            val tempW = new FastArray[WgtTpe]()
            vtWeights.foreach { w => tempW += w }
            weights(vt) = tempW
          }
        }
    }

    this
  }

  def build[N: ClassTag]: N = {
    implicitly[ClassTag[N]].runtimeClass match {
      case nt if nt == classOf[NeighTNW] =>
        assert(neighs.size == weights.size)
        val tempN = new Short2ObjectOpenHashMap[Array[VertexId]](neighs.size())
        val tempW = new Short2ObjectOpenHashMap[Array[WgtTpe]](neighs.size())

        neighs.foreach { case (vt, arr) =>
          tempN.put(vt, arr.trim().array)
          tempW.put(vt, weights(vt).trim().array)
        }

        NeighTNW(tpe, tempN, tempW).asInstanceOf[N]
      case _ =>
        assert(neighs.size == weights.size)
        val tempN = new Short2ObjectOpenHashMap[Array[VertexId]](neighs.size())
        neighs.foreach { case (vt, arr) =>
          tempN.put(vt, arr.trim().array)
        }

        NeighTN(tpe, tempN).asInstanceOf[N]
    }
  }

  def clearAndBuild[N: ClassTag](key: VertexId): N = {
    // remove duplicate && remove key
    implicitly[ClassTag[N]].runtimeClass match {
      case nt if nt == classOf[NeighTNW] =>
        assert(neighs.size == weights.size)
        val tempN = new Short2ObjectOpenHashMap[Array[VertexId]](neighs.size())
        val tempW = new Short2ObjectOpenHashMap[Array[WgtTpe]](neighs.size())

        neighs.foreach { case (vt, arr) =>
          val ns = arr.trim().array
          val ws = weights(vt).trim().array
          val sorted = ns.zip(ws).sortBy { case (n, _) => n }

          val tns = new FastArray[VertexId]
          val tws = new FastArray[WgtTpe]
          var last = sorted.head._1

          tns += last
          tws += sorted.head._2
          sorted.foreach { case (vid, w) =>
            if (vid != key && vid != last) {
              tns += vid
              tws += w
              last = vid
            }
          }

          tempN.put(vt, tns.trim().array)
          tempW.put(vt, tws.trim().array)
        }

        NeighTNW(tpe, tempN, tempW).asInstanceOf[N]
      case _ =>
        assert(neighs.size == weights.size)
        val tempN = new Short2ObjectOpenHashMap[Array[VertexId]](neighs.size())

        neighs.foreach { case (vt, arr) =>
          val ns = arr.trim().array
          val sorted = ns.sorted

          val tns = new FastArray[VertexId]
          var last = sorted.head
          tns += last
          sorted.foreach { vid =>
            if (vid != key && vid != last) {
              tns += vid
              last = vid
            }
          }

          tempN.put(vt, tns.trim().array)
        }

        NeighTN(tpe, tempN).asInstanceOf[N]
    }
  }
}

class PartitionTypedNeighborBuilder[N <: Neighbor : ClassTag]
(direction: EdgeDirection, private val neighTable: FastHashMap[VertexId, TypedNeighborBuilder]) {
  def this(direction: EdgeDirection) = {
    this(direction, new FastHashMap[VertexId, TypedNeighborBuilder]())
  }

  def add(src: VertexId, srcType: VertexType, dst: VertexId, dstType: VertexType): this.type = {
    direction match {
      case EdgeDirection.Both =>
        if (neighTable.containsKey(src)) {
          neighTable(src).add(dstType, dst)
        } else {
          val neighborBuilder = new TypedNeighborBuilder()
          neighborBuilder.add(dstType, dst)
          neighborBuilder.setTpe(srcType)
          neighTable(src) = neighborBuilder
        }

        if (neighTable.containsKey(dst)) {
          neighTable(dst).add(srcType, src)
        } else {
          val neighborBuilder = new TypedNeighborBuilder()
          neighborBuilder.add(srcType, src)
          neighborBuilder.setTpe(dstType)
          neighTable(dst) = neighborBuilder
        }
      case EdgeDirection.Out =>
        if (neighTable.containsKey(src)) {
          neighTable(src).add(dstType, dst)
        } else {
          val neighborBuilder = new TypedNeighborBuilder()
          neighborBuilder.add(dstType, dst)
          neighborBuilder.setTpe(srcType)
          neighTable(src) = neighborBuilder
        }
      case EdgeDirection.In =>
        if (neighTable.containsKey(dst)) {
          neighTable(dst).add(srcType, src)
        } else {
          val neighborBuilder = new TypedNeighborBuilder()
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
          val neighborBuilder = new TypedNeighborBuilder()
          neighborBuilder.add(dstType, dst, weight)
          neighborBuilder.setTpe(srcType)
          neighTable(src) = neighborBuilder
        }

        if (neighTable.containsKey(dst)) {
          neighTable(dst).add(srcType, src, weight)
        } else {
          val neighborBuilder = new TypedNeighborBuilder()
          neighborBuilder.add(srcType, src, weight)
          neighborBuilder.setTpe(dstType)
          neighTable(dst) = neighborBuilder
        }
      case EdgeDirection.Out =>
        if (neighTable.containsKey(src)) {
          neighTable(src).add(dstType, dst, weight)
        } else {
          val neighborBuilder = new TypedNeighborBuilder()
          neighborBuilder.add(dstType, dst, weight)
          neighborBuilder.setTpe(srcType)
          neighTable(src) = neighborBuilder
        }
      case EdgeDirection.In =>
        if (neighTable.containsKey(dst)) {
          neighTable(dst).add(srcType, src, weight)
        } else {
          val neighborBuilder = new TypedNeighborBuilder()
          neighborBuilder.add(srcType, src, weight)
          neighborBuilder.setTpe(dstType)
          neighTable(dst) = neighborBuilder
        }
      case _ =>
        throw new Exception("EdgeDirection not support!")
    }
    this
  }

  def build[T]: T = {
    neighTable.mapValues[N](value => value.build[N]).toUnimi[T]
  }
}
