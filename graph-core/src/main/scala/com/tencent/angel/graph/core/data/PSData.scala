package com.tencent.angel.graph.core.data

import java.util

import com.tencent.angel.graph.core.sampler.AliasTable
import com.tencent.angel.ps.PSContext
import com.tencent.angel.ps.storage.vector.storage.{IntElementMapStorage, LongElementMapStorage}
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap

import scala.collection.mutable

import scala.reflect.runtime.universe._

object PSData {
  private val nodeCache = new mutable.HashMap[(Int, Int), util.ArrayList[Any]]()
  private val refMapManagers = new mutable.HashMap[(Int, Int), RefHashMapManager]()
  private val aliasTables = new mutable.HashMap[(Int, Int), AliasTable]()

  def getData[T](psContext: PSContext, matrixId: Int, partitionId: Int): T = {
    val row = psContext.getMatrixStorageManager.getRow(matrixId, 0, partitionId)

    row.getStorage match {
      case s: IntElementMapStorage => s.getData.asInstanceOf[T]
      case s: LongElementMapStorage =>
        val field = classOf[LongElementMapStorage].getDeclaredField("data")
        field.get(s).asInstanceOf[T]
      case _ =>
        throw new Exception("Storage error")
    }
  }

  def toType[T: TypeTag](tpe: Type, value: Any): T = {
    assert(tpe =:= typeOf[T])
    value.asInstanceOf[T]
  }

  def initRefHashMapManager(ref: AnyRef, matrixId: Int, partitionId: Int): Unit = refMapManagers.synchronized {
    val manager = new RefHashMapManager(ref, matrixId, partitionId)
    refMapManagers.put((matrixId, partitionId), manager)
  }

  def getRefHashMapManager(matrixId: Int, partitionId: Int): RefHashMapManager = refMapManagers.synchronized {
    if (refMapManagers.contains((matrixId, partitionId))) {
      refMapManagers((matrixId, partitionId))
    } else {
      throw new Exception(s"no RefHashMapManager for ($matrixId, $partitionId)")
    }
  }

  def getAliasTable(matrixId: Int, partitionId: Int): AliasTable = aliasTables.synchronized {
    if (aliasTables.contains((matrixId, partitionId))) {
      aliasTables((matrixId, partitionId))
    } else {
      throw new Exception(s"no aliasTable for ($matrixId, $partitionId)")
    }
  }

  def addNodes[T <: ANode](matrixId: Int, partitionId: Int, nodes: Int2ObjectOpenHashMap[T]): Unit ={
    nodeCache.synchronized {
      if (nodeCache.contains((matrixId, partitionId))) {
        nodeCache((matrixId, partitionId)).add(nodes)
      } else {
        val list = new util.ArrayList[Any]()
        list.add(nodes)

        nodeCache.put((matrixId, partitionId), list)
      }
    }
  }

  def addNodes[T <: ANode](matrixId: Int, partitionId: Int, nodes: Long2ObjectOpenHashMap[T]): Unit ={
    nodeCache.synchronized {
      if (nodeCache.contains((matrixId, partitionId))) {
        nodeCache((matrixId, partitionId)).add(nodes)
      } else {
        val list = new util.ArrayList[Any]()
        list.add(nodes)

        nodeCache.put((matrixId, partitionId), list)
      }
    }
  }

}