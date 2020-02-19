package com.tencent.angel.graph.utils

import com.tencent.angel.graph.core.psf.common.{PSFGUCtx, PSFMCtx}
import com.tencent.angel.graph.core.psf.get._
import com.tencent.angel.graph.core.psf.update.UpdatePSF
import com.tencent.angel.psagent.matrix.{MatrixClient, MatrixClientFactory}
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.models.PSMatrix
import org.apache.spark.TaskContext

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
    def createGet[U: TypeTag](getFunc: PSFGUCtx => U)
                    (mergeFunc: PSFMCtx => Any): GetPSF[U] = {
      PSFUtils.createGet[U](getFunc)(mergeFunc).bind(matClient)
    }
  }

  implicit class UpdatePSFGen(psMatrix: PSMatrix) extends PSFGen(psMatrix) {
    def createUpdate(updateFunc: PSFGUCtx => Unit): UpdatePSF = {
      PSFUtils.createUpdate(updateFunc).bind(matClient)
    }
  }

}