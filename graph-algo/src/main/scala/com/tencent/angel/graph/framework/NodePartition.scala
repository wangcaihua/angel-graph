package com.tencent.angel.graph.framework

import java.util.concurrent.atomic.AtomicInteger

import com.tencent.angel.graph.VertexId
import com.tencent.angel.graph.core.data.ANode
import com.tencent.angel.graph.core.sampler.{Reservoir, SampleK, SampleOne, Simple}
import com.tencent.angel.graph.utils.{BitSet, FastHashMap}
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap

import scala.reflect.ClassTag

class NodePartition[VD: ClassTag](global2local: FastHashMap[VertexId, Int]) {
  private lazy val local2global: Array[VertexId] = {
    val l2g = new Array[VertexId](global2local.size())
    global2local.foreach { case (vid: VertexId, idx: Int) => l2g(idx) = vid }
    l2g
  }
  private lazy val attrs: Array[VD] = new Array[VD](global2local.size())
  private lazy val mask: BitSet = new BitSet(global2local.size())
  private lazy val message: Array[Any] = new Array[Any](global2local.size())
  private lazy val neighs: Array[ANode] = new Array[ANode](global2local.size())

  private var sampleOne: SampleOne = new Simple(local2global)
  private var sampleK: SampleK = new Reservoir(local2global)

  // attribution & message methods
  def getAttr(vid: VertexId): VD = attrs(global2local(vid))

  def setAttr(vid: VertexId, attr: VD): this.type = {
    attrs(global2local(vid)) = attr
    this
  }

  def getMessage[M](vid: VertexId): M = message(global2local(vid)).asInstanceOf[M]

  def mergeMessage[M](vid: VertexId, msg: M, mergeFunc: (M, M) => M): this.type = {
    val pos = global2local(vid)
    if (message(pos) == null) {
      message(pos) = msg
    } else {
      message(pos) = mergeFunc(message(pos).asInstanceOf[M], msg)
    }

    this
  }

  def updateAttrs[M](update: (VD, M) => VD): this.type = {
    var i = 0
    while (i < attrs.length) {
      attrs(i) = update(attrs(i), message(i))
      message(i) = null.asInstanceOf[M]
      i += 1
    }

    this
  }


  // neighbor methods
  def setNeigh(vid: VertexId, neigh: ANode): this.type = {
    neighs(global2local(vid)) = neigh
    this
  }

  def getNeigh(vid: VertexId): ANode = neighs(global2local(vid))

  // mask methods
  def setMask(vid: VertexId): this.type = {
    mask.set(global2local(vid))
    this
  }

  def unMask(vid: VertexId): this.type = {
    mask.unset(global2local(vid))
    this
  }

  def isMask(vid: VertexId): Boolean = mask.get(global2local(vid))

  def clearMask(): this.type = {
    mask.clear()
    this
  }

  def activeVertices(): Array[VertexId] = {
    mask.iterator.map(pos => local2global(pos)).toArray
  }

  // sample methods
  def sample(): VertexId = sampleOne.sample()

  def setSampleOne(one: SampleOne): this.type = {
    sampleOne = one
    this
  }

  def sample(k: Int): Array[VertexId] = sampleK.sample(k)

  def setSampleK(sk: SampleK): this.type = {
    sampleK = sk
    this
  }
}

object NodePartition {
  private val _nextId = new AtomicInteger(0)

  def nextId(): Int = {
    _nextId.getAndIncrement()
  }

  def toFastHashMap(itMap: Any): FastHashMap[VertexId, Int] = {
    itMap match {
      case i2i: Int2IntOpenHashMap if classOf[VertexId] == classOf[Int] =>
        null
      case i2i: Int2IntOpenHashMap if classOf[VertexId] == classOf[Long] =>
        null

    }

  }
}
