package com.tencent.angel.graph.core.psf.update

import scala.reflect.runtime.universe._
import com.tencent.angel.ml.matrix.psf.update.base.{PartitionUpdateParam, UpdateFunc, UpdateParam}
import com.tencent.angel.ps.storage.vector.ServerRow

abstract class GUpdateFunc(uParam: UpdateParam) extends UpdateFunc(uParam) {
  override def partitionUpdate(partitionUpdateParam: PartitionUpdateParam): Unit = {
    val pp = partitionUpdateParam.asInstanceOf[GPartitionUpdateParam]
    val row: ServerRow = getRow(pp.getMatrixId, pp.getPartKey.getPartitionId)

    row.startWrite()
    try {
      doUpdate(pp.getPartKey.getPartitionId, row, pp.tpe, pp.params)
    } finally {
      row.endWrite()
    }

  }

  def doUpdate(pId:Int, row: ServerRow, tpe: Type, partParam: Any): Unit

  def getRow(mId: Int, pId: Int): ServerRow = {
    this.psContext.getMatrixStorageManager
      .getRow(mId, 0, pId)
  }
}