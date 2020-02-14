package com.tencent.angel.graph.utils

import com.tencent.angel.graph.core.psf.get._
import com.tencent.angel.graph.core.psf.update.{GUpdateFunc, GUpdateParam, UpdateOp}
import com.tencent.angel.ps.PSContext
import com.tencent.angel.ps.storage.vector.ServerRow
import com.tencent.angel.spark.models.PSMatrix
import org.apache.spark.compat.CUtils

import scala.language.implicitConversions
import scala.reflect.runtime.universe._

class Get(psMatrix: PSMatrix) {
  def get[T: TypeTag, U](getParam: T, mergeParam: U)
                        (getFunc: (PSContext, Int, Int, ServerRow, Type, Any) => (Type, Any))
                        (mergeFunc: (Type, Any, Any) => Any): U = {
    val cleanedGetFunc = CUtils.clean(getFunc)
    val getFuncId = GetOp(cleanedGetFunc)
    val mergeFuncId = MergeOp(mergeFunc)
    MergeOp.setInit(mergeFuncId, mergeParam)

    val param = GGetParam[T](psMatrix.id, getParam, getFuncId, mergeFuncId)
    val func = new GGetFunc(param)
    val result = psMatrix.asyncPsfGet(func).get
      .asInstanceOf[GGetResult]

    GetOp.remove(getFuncId)
    MergeOp.remove(mergeFuncId)
    MergeOp.removeInit(mergeFuncId)

    result.value.asInstanceOf[U]
  }

  def get[U](mergeParam: U)
            (getFunc: (PSContext, Int, Int, ServerRow, Type, Any) => (Type, Any))
            (mergeFunc: (Type, Any, Any) => Any): U = {

    val cleanedGetFunc = CUtils.clean(getFunc)
    val getFuncId = GetOp(cleanedGetFunc)
    val mergeFuncId = MergeOp(mergeFunc)
    MergeOp.setInit(mergeFuncId, mergeParam)

    val param = GGetParam.empty(psMatrix.id, getFuncId, mergeFuncId)
    val func = new GGetFunc(param)
    val result = psMatrix.asyncPsfGet(func).get
      .asInstanceOf[GGetResult]

    GetOp.remove(getFuncId)
    MergeOp.remove(mergeFuncId)
    MergeOp.removeInit(mergeFuncId)

    result.value.asInstanceOf[U]
  }
}

class Update(psMatrix: PSMatrix) {
  def update[T: TypeTag](updateParam: T)
                        (updateFunc: (PSContext, Int, Int, ServerRow, Type, Any) => Unit): Unit = {
    val cleanedUpdateFunc = CUtils.clean(updateFunc)
    val updateFuncId = UpdateOp(cleanedUpdateFunc)

    val param = GUpdateParam[T](psMatrix.id, updateParam, updateFuncId)
    val func = new GUpdateFunc(param)
    psMatrix.asyncPsfUpdate(func).get

    UpdateOp.remove(updateFuncId)
  }

  def update(updateFunc: (PSContext, Int, Int, ServerRow, Type, Any) => Unit): Unit = {
    val cleanedUpdateFunc = CUtils.clean(updateFunc)
    val updateFuncId = UpdateOp(cleanedUpdateFunc)

    val param = GUpdateParam.empty(psMatrix.id, updateFuncId)
    val func = new GUpdateFunc(param)
    psMatrix.asyncPsfUpdate(func).get

    UpdateOp.remove(updateFuncId)
  }
}

object psfConverters {
  implicit def toGet(psMatrix: PSMatrix): Get = new Get(psMatrix)

  implicit def toUpdate(psMatrix: PSMatrix): Update = new Update(psMatrix)
}