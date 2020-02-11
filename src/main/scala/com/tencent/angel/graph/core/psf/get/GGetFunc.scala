package com.tencent.angel.graph.core.psf.get

import java.util

import scala.reflect.runtime.universe._
import com.tencent.angel.ml.matrix.psf.get.base._
import com.tencent.angel.ps.storage.vector.ServerRow

abstract class GGetFunc(gParam: GetParam) extends GetFunc(gParam) {
  override def merge(list: util.List[PartitionGetResult]): GetResult = {
    val gpRes = list.get(0).asInstanceOf[GPartitionGetResult]
    val tpe = gpRes.tpe
    val pResults = (0 until list.size()).map{ idx =>
      val pRes = list.get(idx).asInstanceOf[GPartitionGetResult]
      pRes.pResult
    }.toList

    val (oTpe, result) = doMerge(tpe, pResults)

    GGetResult(oTpe, result)
  }

  def doMerge(tpe: Type, pResults: List[Any]): (Type, Any)

  override def partitionGet(partitionGetParam: PartitionGetParam): PartitionGetResult = {
    val pp = partitionGetParam.asInstanceOf[GPartitionGetParam]
    val row: ServerRow = getRow(pp.getMatrixId, pp.getPartKey.getPartitionId)

    row.startRead()
    try {
      val (tpe, pGet) = doGet(pp.getPartKey.getPartitionId, row, pp.tpe, pp.params)
      new GPartitionGetResult(tpe, pGet)
    } finally {
      row.endRead()
    }
  }

  def doGet(pId:Int, row: ServerRow, tpe: Type, partParam: Any): (Type, Any)

  def getRow(mId: Int, pId: Int): ServerRow = {
    this.psContext.getMatrixStorageManager
      .getRow(mId, 0, pId)
  }
}