package com.tencent.angel.graph.core.psf.update

import com.tencent.angel.ml.matrix.psf.update.base._
import com.tencent.angel.ps.storage.vector.ServerRow

class GUpdateFunc(uParam: UpdateParam) extends UpdateFunc(uParam) {
  def this() = this(null)

  override def partitionUpdate(partitionUpdateParam: PartitionUpdateParam): Unit = {
    val pp = partitionUpdateParam.asInstanceOf[GPartitionUpdateParam]
    val row: ServerRow = psContext.getMatrixStorageManager.getRow(
      pp.getMatrixId, 0, pp.getPartKey.getPartitionId)
    val op: UpdateOp = pp.operation.asInstanceOf[UpdateOp]

    row.startWrite()
    try {
      op(psContext, pp.getMatrixId, pp.getPartKey.getPartitionId, row, pp.tpe, pp.params)
    } finally {
      row.endWrite()
    }
  }
}