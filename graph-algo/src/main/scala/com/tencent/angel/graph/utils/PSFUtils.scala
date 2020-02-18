package com.tencent.angel.graph.utils

import com.tencent.angel.graph.core.psf.common.{PSFGUCtx, PSFMCtx}
import com.tencent.angel.graph.core.psf.get.{GetOp, GetPSF, MergeOp}
import com.tencent.angel.graph.core.psf.update.{UpdateOp, UpdatePSF}
import org.apache.spark.compat.CUtils

import scala.reflect.runtime.universe._

object PSFUtils {
  def createGet[U](getFunc: PSFGUCtx => (Type, Any))
                  (mergeFunc: PSFMCtx => Any): GetPSF[U] = {
    val cleanedGetFunc = CUtils.clean(getFunc)

    val getOp = GetOp(cleanedGetFunc)
    val mergeOp = MergeOp(mergeFunc)

    new GetPSF[U](getOp, mergeOp)
  }

  def createUpdate(updateFunc: PSFGUCtx => Unit): UpdatePSF = {
    val cleanedUpdateFunc = CUtils.clean(updateFunc)

    val updateOp = UpdateOp(cleanedUpdateFunc)

    new UpdatePSF(updateOp)
  }
}
