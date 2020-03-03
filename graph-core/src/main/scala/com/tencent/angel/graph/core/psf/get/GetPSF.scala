package com.tencent.angel.graph.core.psf.get

import java.util.concurrent.Future

import com.tencent.angel.graph.VertexId
import com.tencent.angel.graph.core.psf.common.PSFMCtx
import com.tencent.angel.graph.utils.{FastArray, FastHashMap, GUtils}
import com.tencent.angel.ml.matrix.psf.get.base.GetResult
import com.tencent.angel.psagent.matrix.MatrixClient

import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

class GetPSF[U: ClassTag: TypeTag](getOp: GetOp, mergeOp: MergeOp) extends Serializable {
  @transient private var getFuncId: Int = -1
  @transient private var mergeFuncId: Int = -1

  @transient private var matClient: MatrixClient = _
  @transient private var hasBind: Boolean = false

  def bind(matClient: MatrixClient): this.type = this.synchronized {
    if (!hasBind) {
      this.matClient = matClient
      this.getFuncId = GetOp.add(getOp)
      this.mergeFuncId = MergeOp.add(mergeOp)

      hasBind = true
    }
    this
  }

  def apply[T: TypeTag](getParam: T, batchSize: Int): U = {
    val results = new FastArray[U]()

    getParam match {
      case gParam: Array[VertexId] =>
        val size = gParam.length
        if (size <= batchSize) {
          results += this.apply(getParam)
        } else {
          var (start, end) = (0, batchSize)

          while (end < size + batchSize) {
            val thisEnd = if (end < size) end else size
            val len = thisEnd - start
            val batchGParam = new Array[VertexId](len)
            Array.copy(gParam, start, batchGParam, 0, len)

            results += this.apply(batchGParam.asInstanceOf[T])

            start = end
            end += batchSize
          }
        }
      case gParam: FastHashMap[_, _] =>
        val size = gParam.size()
        if (size <= batchSize) {
          results += this.apply(getParam)
        } else {
          val iter = gParam.iterator
          var curr = 0
          val batchGParam = gParam.emptyLike(batchSize)
          while (iter.hasNext) {
            curr += 1
            val (key, value) = iter.next()
            batchGParam.put(key, value)
            if (curr == size) {
              results += this.apply(batchGParam.asInstanceOf[T])
            } else if (curr % batchSize == 0) {
              results += this.apply(batchGParam.asInstanceOf[T])
              batchGParam.clear()
            }
          }
        }
      case _ =>
        throw new Exception("cannot execute batch!")
    }

    val res = results.trim().array
    if (res.length == 1) {
      res.head
    } else {
      val mergeOp = MergeOp.get(mergeFuncId)
      var ctx = PSFMCtx(null, res.head, res(1))
      var result: U = mergeOp(ctx).asInstanceOf[U]
      (2 until res.length).foreach{ idx =>
        ctx = PSFMCtx(null, result, res(idx))
        result = mergeOp(ctx).asInstanceOf[U]
      }

      result
    }
  }

  def asyncGet[T: TypeTag](getParam: T): Future[GetResult] = this.synchronized {
    if (!hasBind) {
      throw new Exception("please init bind a matClient first!")
    }
    assert(GUtils.paramCheck(typeOf[T]))

    val param = GGetParam[T](matClient.getMatrixId, getParam, getFuncId, mergeFuncId)
    val func = new GGetFunc(param)
    matClient.asyncGet(func)
  }

  def apply[T: TypeTag](getParam: T): U = {
    asyncGet(getParam).get().asInstanceOf[GGetResult].value.asInstanceOf[U]
  }

  def asyncGet(): Future[GetResult] = this.synchronized {
    if (!hasBind) {
      throw new Exception("please init bind a matClient first!")
    }

    val param = GGetParam.empty(matClient.getMatrixId, getFuncId, mergeFuncId)
    val func = new GGetFunc(param)
    matClient.asyncGet(func)
  }

  def apply(): U = {
    asyncGet().get().asInstanceOf[GGetResult].value.asInstanceOf[U]
  }

  def clear(): Unit = this.synchronized {
    GetOp.remove(getFuncId)
    MergeOp.remove(mergeFuncId)

    getFuncId = -1
    mergeFuncId = -1
    matClient = null
    hasBind = false
  }
}
