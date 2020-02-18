package com.tencent.angel.graph.core.psf.common

import com.tencent.angel.ps.PSContext
import com.tencent.angel.ps.storage.vector.storage.{IntElementMapStorage, LongElementMapStorage}

import scala.reflect.runtime.universe._

case class PSFGUCtx(psContext: PSContext, matrixId: Int, partitionId: Int,
                    private val tpe: Type, private val param: Any) {
  def getParam[T: TypeTag]: T = {
    assert(tpe =:= typeOf[T])
    param.asInstanceOf[T]
  }

  def getData[T: TypeTag]: T = {
    val row = psContext.getMatrixStorageManager.getRow(matrixId, 0, partitionId)

    row.getStorage match {
      case s: IntElementMapStorage => s.getData.asInstanceOf[T]
      case s: LongElementMapStorage =>
        val field = classOf[LongElementMapStorage].getDeclaredField("data")
        field.setAccessible(true)
        field.get(s).asInstanceOf[T]
      case _ =>
        throw new Exception("Storage error")
    }
  }
}
