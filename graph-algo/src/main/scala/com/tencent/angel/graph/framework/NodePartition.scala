package com.tencent.angel.graph.framework

import java.util

import com.tencent.angel.graph.VertexId
import com.tencent.angel.graph.core.sampler.{Reservoir, SampleK, SampleOne, Simple}
import com.tencent.angel.graph.utils.{BitSet, FastHashMap, RefHashMap}

import scala.reflect.ClassTag

class NodePartition[VD: ClassTag, M: ClassTag](val global2local: FastHashMap[VertexId, Int],
                                               val local2global: Array[VertexId]) {
  private lazy val attrs: Array[VD] = new Array[VD](global2local.size())
  private lazy val mask: BitSet = new BitSet(global2local.size())
  private lazy val message: Array[M] = new Array[M](global2local.size())
  private lazy val slotRefMap = new util.HashMap[String, RefHashMap[_]]()

  private var sample1: SampleOne = new Simple(local2global)
  private var sampleK: SampleK = new Reservoir(local2global)

  // attribution & message methods
  def getAttr(vid: VertexId): VD = attrs(global2local(vid))

  def setAttr(vid: VertexId, attr: VD): this.type = {
    attrs(global2local(vid)) = attr
    this
  }

  def getMessage(vid: VertexId): M = message(global2local(vid))

  def mergeMessage(vid: VertexId, msg: M, mergeFunc: (M, M) => M): this.type = {
    val pos = global2local(vid)
    if (message(pos) == null) {
      message(pos) = msg
    } else {
      message(pos) = mergeFunc(message(pos), msg)
    }

    this
  }

  def updateAttrs(update: (VD, M) => VD): this.type = {
    var i = 0
    while (i < attrs.length) {
      attrs(i) = update(attrs(i), message(i))
      message(i) = null.asInstanceOf[M]
      i += 1
    }

    this
  }

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

  // slots operations
  def createSlot[V: ClassTag](name: String): this.type = {
    if (!slotRefMap.containsKey(name)) {
      val values = new Array[V](local2global.length)
      val refMap = new RefHashMap(global2local, local2global, values)
      slotRefMap.put(name, refMap)
    } else {
      throw new Exception(s"slot $name already exists!")
    }

    this
  }

  def setSlot[V: ClassTag](name: String, refMap: RefHashMap[V]): this.type = {
    if (!slotRefMap.containsKey(name)) {
      slotRefMap.put(name, refMap)
    } else {
      throw new Exception(s"slot $name already exists!")
    }

    this
  }

  def getOrCreateSlot[V: ClassTag](name: String): RefHashMap[V] = {
    if (!slotRefMap.containsKey(name)) {
      val values = new Array[V](local2global.length)
      val refMap = new RefHashMap(global2local, local2global, values)
      slotRefMap.put(name, refMap)
      refMap
    } else {
      slotRefMap.get(name).asInstanceOf[RefHashMap[V]]
    }
  }

  def removeSlot(name: String): this.type = {
    if (slotRefMap.containsKey(name)) {
      slotRefMap.remove(name)
    }
    this
  }

  def getSlot[V: ClassTag](name: String): RefHashMap[V] = {
    if (!slotRefMap.containsKey(name)) {
      val values = new Array[V](local2global.length)
      val refMap = new RefHashMap(global2local, local2global, values)
      slotRefMap.put(name, refMap)
      refMap
    } else {
      throw new Exception(s"slot $name is not exists!")
    }
  }

  // sample methods
  def sample(): VertexId = sample1.sample()

  def setSampleOne(one: SampleOne): this.type = {
    sample1 = one
    this
  }

  def sample(k: Int): Array[VertexId] = sampleK.sample(k)

  def setSampleK(sk: SampleK): this.type = {
    sampleK = sk
    this
  }
}

