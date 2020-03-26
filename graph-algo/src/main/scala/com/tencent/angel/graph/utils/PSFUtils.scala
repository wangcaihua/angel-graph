package com.tencent.angel.graph.utils

import com.tencent.angel.graph.VertexId
import com.tencent.angel.graph.core.psf.common.{PSFGUCtx, PSFMCtx}
import com.tencent.angel.graph.core.psf.get.{GetOp, GetPSF, MergeOp}
import com.tencent.angel.graph.core.psf.update.{UpdateOp, UpdatePSF}
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.spark.models.PSMatrix
import org.apache.spark.compat.CUtils

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

object PSFUtils {
  def createGet[U: ClassTag : TypeTag](getFunc: PSFGUCtx => U)
                                      (mergeFunc: PSFMCtx => U): GetPSF[U] = {
    // PrintObject(getFunc)
    val cleanedGetFunc = CUtils.clean(getFunc)

    val getOp = GetOp(cleanedGetFunc)
    val mergeOp = MergeOp(mergeFunc)

    new GetPSF[U](getOp, mergeOp)
  }

  def createUpdate(updateFunc: PSFGUCtx => Unit): UpdatePSF = {
    // PrintObject(updateFunc)
    val cleanedUpdateFunc = CUtils.clean(updateFunc)

    val updateOp = UpdateOp(cleanedUpdateFunc)

    new UpdatePSF(updateOp)
  }

  def createPSMatrix(psMatrixName: String, minVertexId: VertexId, maxVertexId: VertexId): PSMatrix = {
    val matrix = new MatrixContext(psMatrixName, 1, minVertexId, maxVertexId + 1)
    matrix.setValidIndexNum(-1)
    if (classOf[VertexId] == classOf[Long]) {
      matrix.setRowType(RowType.T_INT_SPARSE_LONGKEY)
    } else {
      matrix.setRowType(RowType.T_INT_SPARSE)
    }

    PSMatrix.matrix(matrix)
  }
}
