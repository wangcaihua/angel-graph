package com.tencent.angel.graph.core.psf.get

import java.util

import com.tencent.angel.graph.core.psf.common.{PSFGUCtx, PSFMCtx}
import com.tencent.angel.ml.matrix.psf.get.base._
import com.tencent.angel.ps.storage.vector.ServerRow



class GGetFunc(gParam: GetParam) extends GetFunc(gParam) {
  def this() = this(null)

  override def merge(list: util.List[PartitionGetResult]): GetResult = {
    val gpRes = list.get(0).asInstanceOf[GPartitionGetResult]
    val doMerge = MergeOp.get(gpRes.mergeFuncId)
    val tpe = gpRes.tpe

    var result: Any = GetPSF.getInit(gpRes.initId)
    (0 until list.size()).foreach { idx =>
      val pRes = list.get(idx).asInstanceOf[GPartitionGetResult]
      result = doMerge(PSFMCtx(tpe, result, pRes.pResult))
    }

    GGetResult(result)
  }

  override def partitionGet(partitionGetParam: PartitionGetParam): PartitionGetResult = {
    val pp = partitionGetParam.asInstanceOf[GPartitionGetParam]
    val row: ServerRow = psContext.getMatrixStorageManager.getRow(
      pp.getMatrixId, 0, pp.getPartKey.getPartitionId)
    val doGet = pp.getFunc.asInstanceOf[GetOp]

    row.startRead()
    try {
      val pgParam = PSFGUCtx(psContext, pp.getMatrixId, pp.getPartKey.getPartitionId, pp.tpe, pp.params)
      val (tpe, pGet) = doGet(pgParam)
      new GPartitionGetResult(tpe, pGet, pp.mergeFunc.asInstanceOf[Int], pp.initId)
    } finally {
      row.endRead()
    }
  }
}