package com.tencent.angel.graph.core.psf.update

import java.util.concurrent.Future

import com.tencent.angel.graph.utils.{GUtils, ReflectUtils}
import com.tencent.angel.ml.matrix.psf.update.base.VoidResult
import com.tencent.angel.psagent.matrix.MatrixClient
import it.unimi.dsi.fastutil.ints._
import it.unimi.dsi.fastutil.longs._

import scala.language.implicitConversions
import scala.reflect.runtime.universe._

class UpdatePSF(matClient: MatrixClient, updateFuncId: Int) {

  def batchUpdate[T: TypeTag](getParam: T, batchSize: Int): Unit = {
    getParam match {
      case gParam: Array[Int] =>
        val size = gParam.length
        if (size <= batchSize) {
          this.apply(getParam)
        } else {
          var (start, end) = (0, batchSize)

          while (end < size + batchSize) {
            val thisEnd = if (end < size) end else size
            val len = thisEnd - start
            val batchGParam = new Array[Int](len)
            Array.copy(gParam, start, batchGParam, 0, len)

            this.apply(batchGParam.asInstanceOf[T])

            start = end
            end += batchSize
          }
        }
      case gParam: Array[Long] =>
        val size = gParam.length
        if (size <= batchSize) {
          this.apply(getParam)
        } else {
          var (start, end) = (0, batchSize)

          while (end < size + batchSize) {
            val thisEnd = if (end < size) end else size
            val len = thisEnd - start
            val batchGParam = new Array[Long](len)
            Array.copy(gParam, start, batchGParam, 0, len)

            this.apply(batchGParam.asInstanceOf[T])

            start = end
            end += batchSize
          }
        }
      case gParam: Int2BooleanOpenHashMap =>
        val size = gParam.size()
        if (size <= batchSize) {
          this.apply(getParam)
        } else {
          val iter = gParam.int2BooleanEntrySet().fastIterator()
          var curr = 0
          val batchGParam = new Int2BooleanOpenHashMap(batchSize)
          while (iter.hasNext) {
            curr += 1
            val entry = iter.next()
            if (curr == size) {
              this.apply(batchGParam.asInstanceOf[T])
            } else if (curr % batchSize == 0) {
              this.apply(batchGParam.asInstanceOf[T])
              batchGParam.clear()
              batchGParam.put(entry.getIntKey, entry.getBooleanValue)
            } else {
              batchGParam.put(entry.getIntKey, entry.getBooleanValue)
            }
          }
        }
      case gParam: Int2ByteOpenHashMap =>
        val size = gParam.size()
        if (size <= batchSize) {
          this.apply(getParam)
        } else {
          val iter = gParam.int2ByteEntrySet().fastIterator()
          var curr = 0
          val batchGParam = new Int2ByteOpenHashMap(batchSize)
          while (iter.hasNext) {
            curr += 1
            val entry = iter.next()
            if (curr == size) {
              this.apply(batchGParam.asInstanceOf[T])
            } else if (curr % batchSize == 0) {
              this.apply(batchGParam.asInstanceOf[T])
              batchGParam.clear()
              batchGParam.put(entry.getIntKey, entry.getByteValue)
            } else {
              batchGParam.put(entry.getIntKey, entry.getByteValue)
            }
          }
        }
      case gParam: Int2CharOpenHashMap =>
        val size = gParam.size()
        if (size <= batchSize) {
          this.apply(getParam)
        } else {
          val iter = gParam.int2CharEntrySet().fastIterator()
          var curr = 0
          val batchGParam = new Int2CharOpenHashMap(batchSize)
          while (iter.hasNext) {
            curr += 1
            val entry = iter.next()
            if (curr == size) {
              this.apply(batchGParam.asInstanceOf[T])
            } else if (curr % batchSize == 0) {
              this.apply(batchGParam.asInstanceOf[T])
              batchGParam.clear()
              batchGParam.put(entry.getIntKey, entry.getCharValue)
            } else {
              batchGParam.put(entry.getIntKey, entry.getCharValue)
            }
          }
        }
      case gParam: Int2ShortOpenHashMap =>
        val size = gParam.size()
        if (size <= batchSize) {
          this.apply(getParam)
        } else {
          val iter = gParam.int2ShortEntrySet().fastIterator()
          var curr = 0
          val batchGParam = new Int2ShortOpenHashMap(batchSize)
          while (iter.hasNext) {
            curr += 1
            val entry = iter.next()
            if (curr == size) {
              this.apply(batchGParam.asInstanceOf[T])
            } else if (curr % batchSize == 0) {
              this.apply(batchGParam.asInstanceOf[T])
              batchGParam.clear()
              batchGParam.put(entry.getIntKey, entry.getShortValue)
            } else {
              batchGParam.put(entry.getIntKey, entry.getShortValue)
            }
          }
        }
      case gParam: Int2IntOpenHashMap =>
        val size = gParam.size()
        if (size <= batchSize) {
          this.apply(getParam)
        } else {
          val iter = gParam.int2IntEntrySet().fastIterator()
          var curr = 0
          val batchGParam = new Int2IntOpenHashMap(batchSize)
          while (iter.hasNext) {
            curr += 1
            val entry = iter.next()
            if (curr == size) {
              this.apply(batchGParam.asInstanceOf[T])
            } else if (curr % batchSize == 0) {
              this.apply(batchGParam.asInstanceOf[T])
              batchGParam.clear()
              batchGParam.put(entry.getIntKey, entry.getIntValue)
            } else {
              batchGParam.put(entry.getIntKey, entry.getIntValue)
            }
          }
        }
      case gParam: Int2LongOpenHashMap =>
        val size = gParam.size()
        if (size <= batchSize) {
          this.apply(getParam)
        } else {
          val iter = gParam.int2LongEntrySet().fastIterator()
          var curr = 0
          val batchGParam = new Int2LongOpenHashMap(batchSize)
          while (iter.hasNext) {
            curr += 1
            val entry = iter.next()
            if (curr == size) {
              this.apply(batchGParam.asInstanceOf[T])
            } else if (curr % batchSize == 0) {
              this.apply(batchGParam.asInstanceOf[T])
              batchGParam.clear()
              batchGParam.put(entry.getIntKey, entry.getLongValue)
            } else {
              batchGParam.put(entry.getIntKey, entry.getLongValue)
            }
          }
        }
      case gParam: Int2FloatOpenHashMap =>
        val size = gParam.size()
        if (size <= batchSize) {
          this.apply(getParam)
        } else {
          val iter = gParam.int2FloatEntrySet().fastIterator()
          var curr = 0
          val batchGParam = new Int2FloatOpenHashMap(batchSize)
          while (iter.hasNext) {
            curr += 1
            val entry = iter.next()
            if (curr == size) {
              this.apply(batchGParam.asInstanceOf[T])
            } else if (curr % batchSize == 0) {
              this.apply(batchGParam.asInstanceOf[T])
              batchGParam.clear()
              batchGParam.put(entry.getIntKey, entry.getFloatValue)
            } else {
              batchGParam.put(entry.getIntKey, entry.getFloatValue)
            }
          }
        }
      case gParam: Int2DoubleOpenHashMap =>
        val size = gParam.size()
        if (size <= batchSize) {
          this.apply(getParam)
        } else {
          val iter = gParam.int2DoubleEntrySet().fastIterator()
          var curr = 0
          val batchGParam = new Int2DoubleOpenHashMap(batchSize)
          while (iter.hasNext) {
            curr += 1
            val entry = iter.next()
            if (curr == size) {
              this.apply(batchGParam.asInstanceOf[T])
            } else if (curr % batchSize == 0) {
              this.apply(batchGParam.asInstanceOf[T])
              batchGParam.clear()
              batchGParam.put(entry.getIntKey, entry.getDoubleValue)
            } else {
              batchGParam.put(entry.getIntKey, entry.getDoubleValue)
            }
          }
        }
      case gParam: Int2ObjectOpenHashMap[_] =>
        val size = gParam.size()
        if (size <= batchSize) {
          this.apply(getParam)
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
            if (curr == size) {
              this.apply(batchGParam.asInstanceOf[T])
            } else if (curr % batchSize == 0) {
              this.apply(batchGParam.asInstanceOf[T])
              clear()
              put(entry.getIntKey, entry.getValue)
            } else {
              put(entry.getIntKey, entry.getValue)
            }
          }
        }
      case gParam: Long2BooleanOpenHashMap =>
        val size = gParam.size()
        if (size <= batchSize) {
          this.apply(getParam)
        } else {
          val iter = gParam.long2BooleanEntrySet().fastIterator()
          var curr = 0
          val batchGParam = new Long2BooleanOpenHashMap(batchSize)
          while (iter.hasNext) {
            curr += 1
            val entry = iter.next()
            if (curr == size) {
              this.apply(batchGParam.asInstanceOf[T])
            } else if (curr % batchSize == 0) {
              this.apply(batchGParam.asInstanceOf[T])
              batchGParam.clear()
              batchGParam.put(entry.getLongKey, entry.getBooleanValue)
            } else {
              batchGParam.put(entry.getLongKey, entry.getBooleanValue)
            }
          }
        }
      case gParam: Long2ByteOpenHashMap =>
        val size = gParam.size()
        if (size <= batchSize) {
          this.apply(getParam)
        } else {
          val iter = gParam.long2ByteEntrySet().fastIterator()
          var curr = 0
          val batchGParam = new Long2ByteOpenHashMap(batchSize)
          while (iter.hasNext) {
            curr += 1
            val entry = iter.next()
            if (curr == size) {
              this.apply(batchGParam.asInstanceOf[T])
            } else if (curr % batchSize == 0) {
              this.apply(batchGParam.asInstanceOf[T])
              batchGParam.clear()
              batchGParam.put(entry.getLongKey, entry.getByteValue)
            } else {
              batchGParam.put(entry.getLongKey, entry.getByteValue)
            }
          }
        }
      case gParam: Long2CharOpenHashMap =>
        val size = gParam.size()
        if (size <= batchSize) {
          this.apply(getParam)
        } else {
          val iter = gParam.long2CharEntrySet().fastIterator()
          var curr = 0
          val batchGParam = new Long2CharOpenHashMap(batchSize)
          while (iter.hasNext) {
            curr += 1
            val entry = iter.next()
            if (curr == size) {
              this.apply(batchGParam.asInstanceOf[T])
            } else if (curr % batchSize == 0) {
              this.apply(batchGParam.asInstanceOf[T])
              batchGParam.clear()
              batchGParam.put(entry.getLongKey, entry.getCharValue)
            } else {
              batchGParam.put(entry.getLongKey, entry.getCharValue)
            }
          }
        }
      case gParam: Long2ShortOpenHashMap =>
        val size = gParam.size()
        if (size <= batchSize) {
          this.apply(getParam)
        } else {
          val iter = gParam.long2ShortEntrySet().fastIterator()
          var curr = 0
          val batchGParam = new Long2ShortOpenHashMap(batchSize)
          while (iter.hasNext) {
            curr += 1
            val entry = iter.next()
            if (curr == size) {
              this.apply(batchGParam.asInstanceOf[T])
            } else if (curr % batchSize == 0) {
              this.apply(batchGParam.asInstanceOf[T])
              batchGParam.clear()
              batchGParam.put(entry.getLongKey, entry.getShortValue)
            } else {
              batchGParam.put(entry.getLongKey, entry.getShortValue)
            }
          }
        }
      case gParam: Long2IntOpenHashMap =>
        val size = gParam.size()
        if (size <= batchSize) {
          this.apply(getParam)
        } else {
          val iter = gParam.long2IntEntrySet().fastIterator()
          var curr = 0
          val batchGParam = new Long2IntOpenHashMap(batchSize)
          while (iter.hasNext) {
            curr += 1
            val entry = iter.next()
            if (curr == size) {
              this.apply(batchGParam.asInstanceOf[T])
            } else if (curr % batchSize == 0) {
              this.apply(batchGParam.asInstanceOf[T])
              batchGParam.clear()
              batchGParam.put(entry.getLongKey, entry.getIntValue)
            } else {
              batchGParam.put(entry.getLongKey, entry.getIntValue)
            }
          }
        }
      case gParam: Long2LongOpenHashMap =>
        val size = gParam.size()
        if (size <= batchSize) {
          this.apply(getParam)
        } else {
          val iter = gParam.long2LongEntrySet().fastIterator()
          var curr = 0
          val batchGParam = new Long2LongOpenHashMap(batchSize)
          while (iter.hasNext) {
            curr += 1
            val entry = iter.next()
            if (curr == size) {
              this.apply(batchGParam.asInstanceOf[T])
            } else if (curr % batchSize == 0) {
              this.apply(batchGParam.asInstanceOf[T])
              batchGParam.clear()
              batchGParam.put(entry.getLongKey, entry.getLongValue)
            } else {
              batchGParam.put(entry.getLongKey, entry.getLongValue)
            }
          }
        }
      case gParam: Long2FloatOpenHashMap =>
        val size = gParam.size()
        if (size <= batchSize) {
          this.apply(getParam)
        } else {
          val iter = gParam.long2FloatEntrySet().fastIterator()
          var curr = 0
          val batchGParam = new Long2FloatOpenHashMap(batchSize)
          while (iter.hasNext) {
            curr += 1
            val entry = iter.next()
            if (curr == size) {
              this.apply(batchGParam.asInstanceOf[T])
            } else if (curr % batchSize == 0) {
              this.apply(batchGParam.asInstanceOf[T])
              batchGParam.clear()
              batchGParam.put(entry.getLongKey, entry.getFloatValue)
            } else {
              batchGParam.put(entry.getLongKey, entry.getFloatValue)
            }
          }
        }
      case gParam: Long2DoubleOpenHashMap =>
        val size = gParam.size()
        if (size <= batchSize) {
          this.apply(getParam)
        } else {
          val iter = gParam.long2DoubleEntrySet().fastIterator()
          var curr = 0
          val batchGParam = new Long2DoubleOpenHashMap(batchSize)
          while (iter.hasNext) {
            curr += 1
            val entry = iter.next()
            if (curr == size) {
              this.apply(batchGParam.asInstanceOf[T])
            } else if (curr % batchSize == 0) {
              this.apply(batchGParam.asInstanceOf[T])
              batchGParam.clear()
              batchGParam.put(entry.getLongKey, entry.getDoubleValue)
            } else {
              batchGParam.put(entry.getLongKey, entry.getDoubleValue)
            }
          }
        }
      case gParam: Long2ObjectOpenHashMap[_] =>
        val size = gParam.size()
        if (size <= batchSize) {
          this.apply(getParam)
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
            if (curr == size) {
              this.apply(batchGParam.asInstanceOf[T])
            } else if (curr % batchSize == 0) {
              this.apply(batchGParam.asInstanceOf[T])
              clear()
              put(entry.getLongKey, entry.getValue)
            } else {
              put(entry.getLongKey, entry.getValue)
            }
          }
        }
      case _ =>
        throw new Exception("cannot execute batch!")
    }
  }

  def apply[T: TypeTag](updateParam: T): Unit = asyncUpdate[T](updateParam).get

  def asyncUpdate[T: TypeTag](updateParam: T): Future[VoidResult] = {
    assert(GUtils.paramCheck(typeOf[T]))

    val param = GUpdateParam[T](matClient.getMatrixId, updateParam, updateFuncId)
    val func = new GUpdateFunc(param)
    matClient.asyncUpdate(func)
  }

  def apply(): Unit = asyncUpdate().get

  def asyncUpdate(): Future[VoidResult] = {
    val param = GUpdateParam.empty(matClient.getMatrixId, updateFuncId)
    val func = new GUpdateFunc(param)
    matClient.asyncUpdate(func)
  }

  def removeFuncIds(): Unit = UpdateOp.remove(updateFuncId)
}

