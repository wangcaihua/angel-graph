package com.tencent.angel.graph.core.data

import com.tencent.angel.graph.VertexId
import com.tencent.angel.graph.core.sampler.{Reservoir, SampleK, SampleOne, Simple}
import com.tencent.angel.graph.utils.{BitSet, FastHashMap, Logging, RefHashMap}

import scala.collection.mutable
import scala.reflect.ClassTag


class PSPartition[VD: ClassTag](val global2local: FastHashMap[VertexId, Int],
                                val local2global: Array[VertexId]) extends Logging {
  private lazy val attrs: Array[VD] = new Array[VD](global2local.size())
  private lazy val mask: BitSet = {
    // active all in initial
    val bs = new BitSet(global2local.size())
    local2global.indices.foreach(idx => bs.set(idx))
    bs
  }

  private lazy val message: Array[Any] = new Array[Any](global2local.size())
  private lazy val slotRefMap = new mutable.HashMap[String, RefHashMap[_]]()

  @transient private var sample1: SampleOne = new Simple(local2global)
  @transient private var sampleK: SampleK = new Reservoir(local2global)

  def withAttrType[VD2: ClassTag]: PSPartition[VD2] = {
    new PSPartition[VD2](global2local, local2global)
  }

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

  def updateAttrs[M](vprog: (VD, M) => VD, active: (VD, M) => Boolean, defaultMsg: M): this.type = {
    attrs.indices.foreach { pos =>
      val msg = message(pos).asInstanceOf[M]
      val attr = attrs(pos)

      if (msg != null) {
        if (active(attr, msg)) {
          attrs(pos) = vprog(attr, msg)
        } else {
          setInactive(pos)
        }
      } else {
        attrs(pos) = vprog(attr, defaultMsg)
      }
    }

    this
  }

  // mask methods
  def setInactive(pos: Int): this.type = {
    mask.unset(pos)
    this
  }

  def setActive(pos: Int): this.type = {
    mask.set(pos)
    this
  }

  def isActive(pos: Int): Boolean = mask.get(pos)

  def clearMask(): this.type = {
    mask.clear()
    this
  }

  def activeVertices(): Array[VertexId] = {
    mask.iterator.map(pos => local2global(pos)).toArray
  }

  def activeVerticesCount(): Int = mask.capacity

  // slots operations
  def createSlot[V: ClassTag](name: String): this.type = slotRefMap.synchronized {
    if (!slotRefMap.contains(name)) {
      val refMap = new RefHashMap[V](global2local, local2global)
      slotRefMap(name) = refMap
    } else {
      throw new Exception(s"slot $name already exists!")
    }

    this
  }

  def setSlot[V: ClassTag](name: String, refMap: RefHashMap[V]): this.type = slotRefMap.synchronized {
    if (!slotRefMap.contains(name)) {
      slotRefMap(name) = refMap
    } else {
      throw new Exception(s"slot $name already exists!")
    }

    this
  }

  def getOrCreateSlot[V: ClassTag](name: String): RefHashMap[V] = slotRefMap.synchronized {
    if (!slotRefMap.contains(name)) {
      val refMap = new RefHashMap[V](global2local, local2global)
      slotRefMap(name) = refMap
      refMap
    } else {
      slotRefMap(name).asInstanceOf[RefHashMap[V]]
    }
  }

  def removeSlot(name: String): this.type = slotRefMap.synchronized {
    if (slotRefMap.contains(name)) {
      slotRefMap.remove(name)
    }
    this
  }

  def getSlot[V: ClassTag](name: String): RefHashMap[V] = slotRefMap.synchronized {
    if (slotRefMap.contains(name)) {
      slotRefMap(name).asInstanceOf[RefHashMap[V]]
    } else {
      println(s"slot $name is not exists!")
      null.asInstanceOf[RefHashMap[V]]
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

  def checkpoint(): Unit = ???
}

object PSPartition {
  private val partitionBuilders = new mutable.HashMap[String, PSPartitionBuilder]()
  private val partitions = new mutable.HashMap[String, PSPartition[_]]()

  def getOrCreate[VD: ClassTag](key: String): PSPartition[VD] = partitions.synchronized {
    if (partitions.contains(key)) {
      partitions(key).asInstanceOf[PSPartition[VD]]
    } else if (partitionBuilders.contains(key)) {
      val partition = partitionBuilders(key).build[VD]
      partitions(key) = partition
      partitionBuilders.remove(key)
      partition
    } else {
      throw new Exception(s"cannot find partition by key $key")
    }
  }

  def get[VD: ClassTag](key: String): PSPartition[VD] = partitions.synchronized {
    if (partitions.contains(key)) {
      partitions(key).asInstanceOf[PSPartition[VD]]
    } else {
      throw new Exception(s"cannot find partition by key $key")
    }
  }

  def set[VD: ClassTag](key: String, psPartition: PSPartition[VD]): Unit = partitions.synchronized {
    partitions.put(key, psPartition)
  }

  def contains(key: String): Boolean = {
    partitions.contains(key)
  }

  def getOrCreateBuilder(key: String): PSPartitionBuilder = partitionBuilders.synchronized {
    if (partitionBuilders.contains(key)) {
      partitionBuilders(key)
    } else {
      val builder = new PSPartitionBuilder()
      partitionBuilders.put(key, builder)
      builder
    }
  }

  def containsBuilder(key: String): Boolean = {
    partitionBuilders.contains(key)
  }
}

