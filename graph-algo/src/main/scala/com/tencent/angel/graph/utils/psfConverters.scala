package com.tencent.angel.graph.utils

import com.tencent.angel.graph.core.psf.get._
import com.tencent.angel.graph.core.psf.update.{UpdateOp, UpdatePSF}
import com.tencent.angel.ps
import com.tencent.angel.psagent.matrix.{MatrixClient, MatrixClientFactory}
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.models.PSMatrix
import org.apache.spark.TaskContext
import org.apache.spark.compat.CUtils

import scala.language.implicitConversions
import scala.reflect.runtime.universe._

class PSFGen(psMat: PSMatrix) {
  PSContext.instance()
  protected val matClient: MatrixClient = MatrixClientFactory.get(psMat.id, getTaskId)

  private def getTaskId: Int = {
    val tc = TaskContext.get()
    if (tc == null) -1 else tc.partitionId()
  }
}

object psfConverters {

  implicit class GetPSFGen(psMatrix: PSMatrix) extends PSFGen(psMatrix) {
    def createGet[U](getFunc: (ps.PSContext, Int, Int, Type, Any) => (Type, Any))
                    (mergeFunc: (Type, Any, Any) => Any): GetPSF[U] = {
      val cleanedGetFunc = CUtils.clean(getFunc)

      val getFuncId = GetOp(cleanedGetFunc)
      val mergeFuncId = MergeOp(mergeFunc)

      new GetPSF[U](matClient, getFuncId, mergeFuncId)
    }

  }

  implicit class UpdatePSFGen(psMatrix: PSMatrix) extends PSFGen(psMatrix) {
    def createUpdate(updateFunc: (ps.PSContext, Int, Int, Type, Any) => Unit): Unit = {
      val cleanedUpdateFunc = CUtils.clean(updateFunc)

      val updateFuncId = UpdateOp(cleanedUpdateFunc)

      new UpdatePSF(matClient, updateFuncId)
    }
  }

}