package com.tencent.angel.graph.core.data

import java.util.concurrent.atomic.AtomicInteger

import com.tencent.angel.graph.VertexId
import com.tencent.angel.graph.utils.{FastArray, FastHashMap}

import scala.reflect.ClassTag

class PSPartitionBuilder {
  private val nextId = new AtomicInteger(0)
  private val global2local = new FastHashMap[VertexId, Int]()
  private val local2global = new FastArray[VertexId]()

  def put(vid: VertexId): this.type = {
    global2local.changeValue(vid, {val id = nextId.getAndIncrement(); local2global += vid; id}, identity)
    this
  }

  def put(vids: Array[VertexId]): this.type = {
    vids.foreach{ vid =>
      global2local.changeValue(vid, {val id = nextId.getAndIncrement(); local2global += vid; id}, identity)
    }
    this
  }

  def build[VD: ClassTag]: PSPartition[VD] = {
    new PSPartition[VD](global2local, local2global.trim().array)
  }

}
