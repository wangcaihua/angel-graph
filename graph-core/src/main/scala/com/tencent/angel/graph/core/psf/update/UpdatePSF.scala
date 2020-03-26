package com.tencent.angel.graph.core.psf.update

import java.util.concurrent.Future

import com.tencent.angel.graph.VertexId
import com.tencent.angel.graph.utils.{FastHashMap, GUtils}
import com.tencent.angel.ml.matrix.psf.update.base.VoidResult
import com.tencent.angel.psagent.matrix.MatrixClient

import scala.language.implicitConversions
import scala.reflect.runtime.universe._

class UpdatePSF(updateOp: UpdateOp) extends Serializable {
  @transient private var updateFuncId: Int = -1

  @transient private var matClient: MatrixClient = _
  @transient private var hasBind: Boolean = false

  def bind(matClient: MatrixClient): this.type = {
    if (!hasBind) {
      this.matClient = matClient
      this.updateFuncId = UpdateOp.add(updateOp)

      hasBind = true
    }

    this
  }

  def apply[T: TypeTag](updateParam: T, batchSize: Int): Unit = {
    if (batchSize <= 0) {
      async[T](updateParam).get
    } else {
      updateParam match {
        case gParam: Array[VertexId] =>
          val size = gParam.length
          if (size <= batchSize) {
            this.apply(updateParam)
          } else {
            var (start, end) = (0, batchSize)

            while (end < size + batchSize) {
              val thisEnd = if (end < size) end else size
              val len = thisEnd - start
              val batchGParam = new Array[VertexId](len)
              Array.copy(gParam, start, batchGParam, 0, len)

              this.apply(batchGParam.asInstanceOf[T])

              start = end
              end += batchSize
            }
          }
        case gParam: FastHashMap[_, _] =>
          val size = gParam.size()
          if (size <= batchSize) {
            this.apply(updateParam)
          } else {
            val iter = gParam.iterator
            var curr = 0
            val batchGParam = gParam.emptyLike(batchSize)
            while (iter.hasNext) {
              curr += 1
              val (key, value) = iter.next()
              batchGParam.put(key, value)

              if (curr == size) {
                this.apply(batchGParam.asInstanceOf[T])
              } else if (curr % batchSize == 0) {
                this.apply(batchGParam.asInstanceOf[T])
                batchGParam.clear()
              }
            }
          }
        case _ =>
          throw new Exception("cannot execute batch!")
      }
    }

  }

  def apply[T: TypeTag](updateParam: T): Unit = async[T](updateParam).get

  def async[T: TypeTag](updateParam: T): Future[VoidResult] = {
    if (!hasBind) {
      throw new Exception("please init bind a matClient first!")
    }
    assert(GUtils.paramCheck(typeOf[T]))

    val param = GUpdateParam[T](matClient.getMatrixId, updateParam, updateFuncId)
    val func = new GUpdateFunc(param)
    matClient.asyncUpdate(func)
  }

  def apply(): Unit = async().get

  def async(): Future[VoidResult] = {
    if (!hasBind) {
      throw new Exception("please init bind a matClient first!")
    }

    val param = GUpdateParam.empty(matClient.getMatrixId, updateFuncId)
    val func = new GUpdateFunc(param)
    matClient.asyncUpdate(func)
  }

  def clear(): Unit = {
    UpdateOp.remove(updateFuncId)

    updateFuncId = -1
    matClient = null
    hasBind = false
  }
}

