package com.tencent.angel.graph.core.psf.get

import java.util.concurrent.Future

import com.tencent.angel.graph.utils.{GUtils, ReflectUtils}
import com.tencent.angel.ml.matrix.psf.get.base.GetResult
import com.tencent.angel.psagent.matrix.MatrixClient
import it.unimi.dsi.fastutil.ints._
import it.unimi.dsi.fastutil.longs._

import scala.language.implicitConversions
import scala.reflect.runtime.universe._

class GetPSF[U: TypeTag](getOp: GetOp, mergeOp: MergeOp) extends Serializable {
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

  def apply[T: TypeTag](getParam: T, mergeParam: U, batchSize: Int): U = {
    var result: U = mergeParam

    getParam match {
      case gParam: Array[Int] =>
        val size = gParam.length
        if (size <= batchSize) {
          result = this.apply(getParam, result)
        } else {
          var (start, end) = (0, batchSize)

          while (end < size + batchSize) {
            val thisEnd = if (end < size) end else size
            val len = thisEnd - start
            val batchGParam = new Array[Int](len)
            Array.copy(gParam, start, batchGParam, 0, len)

            result = this.apply(batchGParam.asInstanceOf[T], result)

            start = end
            end += batchSize
          }
        }
      case gParam: Array[Long] =>
        val size = gParam.length
        if (size <= batchSize) {
          result = this.apply(getParam, result)
        } else {
          var (start, end) = (0, batchSize)

          while (end < size + batchSize) {
            val thisEnd = if (end < size) end else size
            val len = thisEnd - start
            val batchGParam = new Array[Long](len)
            Array.copy(gParam, start, batchGParam, 0, len)

            result = this.apply(batchGParam.asInstanceOf[T], result)

            start = end
            end += batchSize
          }
        }
      case gParam: Int2BooleanOpenHashMap =>
        val size = gParam.size()
        if (size <= batchSize) {
          result = this.apply(getParam, result)
        } else {
          val iter = gParam.int2BooleanEntrySet().fastIterator()
          var curr = 0
          val batchGParam = new Int2BooleanOpenHashMap(batchSize)
          while (iter.hasNext) {
            curr += 1
            val entry = iter.next()
            batchGParam.put(entry.getIntKey, entry.getBooleanValue)
            if (curr == size) {
              result = this.apply(batchGParam.asInstanceOf[T], result)
            } else if (curr % batchSize == 0) {
              result = this.apply(batchGParam.asInstanceOf[T], result)
              batchGParam.clear()
            }
          }
        }
      case gParam: Int2ByteOpenHashMap =>
        val size = gParam.size()
        if (size <= batchSize) {
          result = this.apply(getParam, result)
        } else {
          val iter = gParam.int2ByteEntrySet().fastIterator()
          var curr = 0
          val batchGParam = new Int2ByteOpenHashMap(batchSize)
          while (iter.hasNext) {
            curr += 1
            val entry = iter.next()
            batchGParam.put(entry.getIntKey, entry.getByteValue)

            if (curr == size) {
              result = this.apply(batchGParam.asInstanceOf[T], result)
            } else if (curr % batchSize == 0) {
              result = this.apply(batchGParam.asInstanceOf[T], result)
              batchGParam.clear()
            }
          }
        }
      case gParam: Int2CharOpenHashMap =>
        val size = gParam.size()
        if (size <= batchSize) {
          result = this.apply(getParam, result)
        } else {
          val iter = gParam.int2CharEntrySet().fastIterator()
          var curr = 0
          val batchGParam = new Int2CharOpenHashMap(batchSize)
          while (iter.hasNext) {
            curr += 1
            val entry = iter.next()
            batchGParam.put(entry.getIntKey, entry.getCharValue)
            if (curr == size) {
              result = this.apply(batchGParam.asInstanceOf[T], result)
            } else if (curr % batchSize == 0) {
              result = this.apply(batchGParam.asInstanceOf[T], result)
              batchGParam.clear()
            }
          }
        }
      case gParam: Int2ShortOpenHashMap =>
        val size = gParam.size()
        if (size <= batchSize) {
          result = this.apply(getParam, result)
        } else {
          val iter = gParam.int2ShortEntrySet().fastIterator()
          var curr = 0
          val batchGParam = new Int2ShortOpenHashMap(batchSize)
          while (iter.hasNext) {
            curr += 1
            val entry = iter.next()
            batchGParam.put(entry.getIntKey, entry.getShortValue)

            if (curr == size) {
              result = this.apply(batchGParam.asInstanceOf[T], result)
            } else if (curr % batchSize == 0) {
              result = this.apply(batchGParam.asInstanceOf[T], result)
              batchGParam.clear()
            }
          }
        }
      case gParam: Int2IntOpenHashMap =>
        val size = gParam.size()
        if (size <= batchSize) {
          result = this.apply(getParam, result)
        } else {
          val iter = gParam.int2IntEntrySet().fastIterator()
          var curr = 0
          val batchGParam = new Int2IntOpenHashMap(batchSize)
          while (iter.hasNext) {
            curr += 1
            val entry = iter.next()
            batchGParam.put(entry.getIntKey, entry.getIntValue)

            if (curr == size) {
              result = this.apply(batchGParam.asInstanceOf[T], result)
            } else if (curr % batchSize == 0) {
              result = this.apply(batchGParam.asInstanceOf[T], result)
              batchGParam.clear()
            }
          }
        }
      case gParam: Int2LongOpenHashMap =>
        val size = gParam.size()
        if (size <= batchSize) {
          result = this.apply(getParam, result)
        } else {
          val iter = gParam.int2LongEntrySet().fastIterator()
          var curr = 0
          val batchGParam = new Int2LongOpenHashMap(batchSize)
          while (iter.hasNext) {
            curr += 1
            val entry = iter.next()

            batchGParam.put(entry.getIntKey, entry.getLongValue)
            if (curr == size) {
              result = this.apply(batchGParam.asInstanceOf[T], result)
            } else if (curr % batchSize == 0) {
              result = this.apply(batchGParam.asInstanceOf[T], result)
              batchGParam.clear()
            }
          }
        }
      case gParam: Int2FloatOpenHashMap =>
        val size = gParam.size()
        if (size <= batchSize) {
          result = this.apply(getParam, result)
        } else {
          val iter = gParam.int2FloatEntrySet().fastIterator()
          var curr = 0
          val batchGParam = new Int2FloatOpenHashMap(batchSize)
          while (iter.hasNext) {
            curr += 1
            val entry = iter.next()
            batchGParam.put(entry.getIntKey, entry.getFloatValue)

            if (curr == size) {
              result = this.apply(batchGParam.asInstanceOf[T], result)
            } else if (curr % batchSize == 0) {
              result = this.apply(batchGParam.asInstanceOf[T], result)
              batchGParam.clear()
              batchGParam.put(entry.getIntKey, entry.getFloatValue)
            }
          }
        }
      case gParam: Int2DoubleOpenHashMap =>
        val size = gParam.size()
        if (size <= batchSize) {
          result = this.apply(getParam, result)
        } else {
          val iter = gParam.int2DoubleEntrySet().fastIterator()
          var curr = 0
          val batchGParam = new Int2DoubleOpenHashMap(batchSize)
          while (iter.hasNext) {
            curr += 1
            val entry = iter.next()
            batchGParam.put(entry.getIntKey, entry.getDoubleValue)

            if (curr == size) {
              result = this.apply(batchGParam.asInstanceOf[T], result)
            } else if (curr % batchSize == 0) {
              result = this.apply(batchGParam.asInstanceOf[T], result)
              batchGParam.clear()
            }
          }
        }
      case gParam: Int2ObjectOpenHashMap[_] =>
        val size = gParam.size()
        if (size <= batchSize) {
          result = this.apply(getParam, result)
        } else {
          val iter = gParam.int2ObjectEntrySet().fastIterator()
          var curr = 0

          val constructor = ReflectUtils.constructor(typeOf[T], typeOf[Int])
          val batchGParam = constructor(batchSize)
          val put = ReflectUtils.method(batchGParam, "put", typeOf[Int])
          val clear = ReflectUtils.method(batchGParam, "clear")

          while (iter.hasNext) {
            curr += 1
            val entry = iter.next()
            put(entry.getIntKey, entry.getValue)

            if (curr == size) {
              result = this.apply(batchGParam.asInstanceOf[T], result)
            } else if (curr % batchSize == 0) {
              result = this.apply(batchGParam.asInstanceOf[T], result)
              clear()
              put(entry.getIntKey, entry.getValue)
            }
          }
        }
      case gParam: Long2BooleanOpenHashMap =>
        val size = gParam.size()
        if (size <= batchSize) {
          result = this.apply(getParam, result)
        } else {
          val iter = gParam.long2BooleanEntrySet().fastIterator()
          var curr = 0
          val batchGParam = new Long2BooleanOpenHashMap(batchSize)
          while (iter.hasNext) {
            curr += 1
            val entry = iter.next()
            batchGParam.put(entry.getLongKey, entry.getBooleanValue)

            if (curr == size) {
              result = this.apply(batchGParam.asInstanceOf[T], result)
            } else if (curr % batchSize == 0) {
              result = this.apply(batchGParam.asInstanceOf[T], result)
              batchGParam.clear()
            }
          }
        }
      case gParam: Long2ByteOpenHashMap =>
        val size = gParam.size()
        if (size <= batchSize) {
          result = this.apply(getParam, result)
        } else {
          val iter = gParam.long2ByteEntrySet().fastIterator()
          var curr = 0
          val batchGParam = new Long2ByteOpenHashMap(batchSize)
          while (iter.hasNext) {
            curr += 1
            val entry = iter.next()
            batchGParam.put(entry.getLongKey, entry.getByteValue)

            if (curr == size) {
              result = this.apply(batchGParam.asInstanceOf[T], result)
            } else if (curr % batchSize == 0) {
              result = this.apply(batchGParam.asInstanceOf[T], result)
              batchGParam.clear()
            }
          }
        }
      case gParam: Long2CharOpenHashMap =>
        val size = gParam.size()
        if (size <= batchSize) {
          result = this.apply(getParam, result)
        } else {
          val iter = gParam.long2CharEntrySet().fastIterator()
          var curr = 0
          val batchGParam = new Long2CharOpenHashMap(batchSize)
          while (iter.hasNext) {
            curr += 1
            val entry = iter.next()
            batchGParam.put(entry.getLongKey, entry.getCharValue)

            if (curr == size) {
              result = this.apply(batchGParam.asInstanceOf[T], result)
            } else if (curr % batchSize == 0) {
              result = this.apply(batchGParam.asInstanceOf[T], result)
              batchGParam.clear()
            }
          }
        }
      case gParam: Long2ShortOpenHashMap =>
        val size = gParam.size()
        if (size <= batchSize) {
          result = this.apply(getParam, result)
        } else {
          val iter = gParam.long2ShortEntrySet().fastIterator()
          var curr = 0
          val batchGParam = new Long2ShortOpenHashMap(batchSize)
          while (iter.hasNext) {
            curr += 1
            val entry = iter.next()
            batchGParam.put(entry.getLongKey, entry.getShortValue)

            if (curr == size) {
              result = this.apply(batchGParam.asInstanceOf[T], result)
            } else if (curr % batchSize == 0) {
              result = this.apply(batchGParam.asInstanceOf[T], result)
              batchGParam.clear()
            }
          }
        }
      case gParam: Long2IntOpenHashMap =>
        val size = gParam.size()
        if (size <= batchSize) {
          result = this.apply(getParam, result)
        } else {
          val iter = gParam.long2IntEntrySet().fastIterator()
          var curr = 0
          val batchGParam = new Long2IntOpenHashMap(batchSize)
          while (iter.hasNext) {
            curr += 1
            val entry = iter.next()
            batchGParam.put(entry.getLongKey, entry.getIntValue)

            if (curr == size) {
              result = this.apply(batchGParam.asInstanceOf[T], result)
            } else if (curr % batchSize == 0) {
              result = this.apply(batchGParam.asInstanceOf[T], result)
              batchGParam.clear()
            }
          }
        }
      case gParam: Long2LongOpenHashMap =>
        val size = gParam.size()
        if (size <= batchSize) {
          result = this.apply(getParam, result)
        } else {
          val iter = gParam.long2LongEntrySet().fastIterator()
          var curr = 0
          val batchGParam = new Long2LongOpenHashMap(batchSize)
          while (iter.hasNext) {
            curr += 1
            val entry = iter.next()
            batchGParam.put(entry.getLongKey, entry.getLongValue)

            if (curr == size) {
              result = this.apply(batchGParam.asInstanceOf[T], result)
            } else if (curr % batchSize == 0) {
              result = this.apply(batchGParam.asInstanceOf[T], result)
              batchGParam.clear()
            }
          }
        }
      case gParam: Long2FloatOpenHashMap =>
        val size = gParam.size()
        if (size <= batchSize) {
          result = this.apply(getParam, result)
        } else {
          val iter = gParam.long2FloatEntrySet().fastIterator()
          var curr = 0
          val batchGParam = new Long2FloatOpenHashMap(batchSize)
          while (iter.hasNext) {
            curr += 1
            val entry = iter.next()
            batchGParam.put(entry.getLongKey, entry.getFloatValue)

            if (curr == size) {
              result = this.apply(batchGParam.asInstanceOf[T], result)
            } else if (curr % batchSize == 0) {
              result = this.apply(batchGParam.asInstanceOf[T], result)
              batchGParam.clear()
            }
          }
        }
      case gParam: Long2DoubleOpenHashMap =>
        val size = gParam.size()
        if (size <= batchSize) {
          result = this.apply(getParam, result)
        } else {
          val iter = gParam.long2DoubleEntrySet().fastIterator()
          var curr = 0
          val batchGParam = new Long2DoubleOpenHashMap(batchSize)
          while (iter.hasNext) {
            curr += 1
            val entry = iter.next()
            batchGParam.put(entry.getLongKey, entry.getDoubleValue)

            if (curr == size) {
              result = this.apply(batchGParam.asInstanceOf[T], result)
            } else if (curr % batchSize == 0) {
              result = this.apply(batchGParam.asInstanceOf[T], result)
              batchGParam.clear()
            }
          }
        }
      case gParam: Long2ObjectOpenHashMap[_] =>
        val size = gParam.size()
        if (size <= batchSize) {
          result = this.apply(getParam, result)
        } else {
          val iter = gParam.long2ObjectEntrySet().fastIterator()
          var curr = 0

          val constructor = ReflectUtils.constructor(typeOf[T], typeOf[Int])
          val batchGParam = constructor(batchSize)
          val put = ReflectUtils.method(batchGParam, "put", typeOf[Long])
          val clear = ReflectUtils.method(batchGParam, "clear")

          while (iter.hasNext) {
            curr += 1
            val entry = iter.next()
            put(entry.getLongKey, entry.getValue)

            if (curr == size) {
              result = this.apply(batchGParam.asInstanceOf[T], result)
            } else if (curr % batchSize == 0) {
              result = this.apply(batchGParam.asInstanceOf[T], result)
              clear()
            }
          }
        }
      case _ =>
        throw new Exception("cannot execute batch!")
    }

    result
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
