package com.tencent.angel.graph.utils

import com.tencent.angel.common.Serialize
import com.tencent.angel.ml.math2.vector.Vector
import io.netty.buffer.{ByteBuf, Unpooled}
import it.unimi.dsi.fastutil.ints._
import it.unimi.dsi.fastutil.longs._

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.{TypeTag, _}


object SerDe {
  private val tmpBuf = Unpooled.buffer(8)
  val boolSize: Int = {
    tmpBuf.clear()
    tmpBuf.writeBoolean(true).readableBytes()
  }
  val shortSize: Int = {
    tmpBuf.clear()
    tmpBuf.writeShort(0).readableBytes()
  }
  val charSize: Int = {
    tmpBuf.clear()
    tmpBuf.writeChar(56).readableBytes()
  }

  private val clzByteBuf: Class[_] = Class.forName("io.netty.buffer.ByteBuf")

  // 1. for array
  def serArr[T: TypeTag](arr: Array[T], byteBuf: ByteBuf): Unit = {
    serArr(typeOf[T], arr, byteBuf)
  }

  def serArr(typ: Type, arr: Any, byteBuf: ByteBuf): Unit = {
    if (arr == null) {
      byteBuf.writeInt(0)
    } else {
      val temp = arr.asInstanceOf[Array[_]]
      if (temp.isEmpty) {
        byteBuf.writeInt(0)
      } else {
        byteBuf.writeInt(temp.length)
        typ match {
          case bool if bool =:= typeOf[Boolean] =>
            temp.asInstanceOf[Array[Boolean]].foreach(e => byteBuf.writeBoolean(e))
          case byte if byte =:= typeOf[Byte] =>
            temp.asInstanceOf[Array[Byte]].foreach(e => byteBuf.writeByte(e))
          case char if char =:= typeOf[Char] =>
            temp.asInstanceOf[Array[Char]].foreach(e => byteBuf.writeChar(e))
          case short if short =:= typeOf[Short] =>
            temp.asInstanceOf[Array[Short]].foreach(e => byteBuf.writeShort(e))
          case int if int =:= typeOf[Int] =>
            temp.asInstanceOf[Array[Int]].foreach(e => byteBuf.writeInt(e))
          case long if long =:= typeOf[Long] =>
            temp.asInstanceOf[Array[Long]].foreach(e => byteBuf.writeLong(e))
          case float if float =:= typeOf[Float] =>
            temp.asInstanceOf[Array[Float]].foreach(e => byteBuf.writeFloat(e))
          case double if double =:= typeOf[Double] =>
            temp.asInstanceOf[Array[Double]].foreach(e => byteBuf.writeDouble(e))
          case str if str =:= typeOf[String] =>
            temp.asInstanceOf[Array[String]].foreach { e =>
              byteBuf.writeInt(e.length)
              byteBuf.writeBytes(e.getBytes)
            }
          case ser if ser <:< typeOf[Serialize] =>
            temp.asInstanceOf[Array[Serialize]].foreach(e => e.serialize(byteBuf))
          case t =>
            throw new Exception(s"type ${t.toString} cannot serialize")
        }
      }
    }
  }

  def arrFromBuffer[T: ClassTag](byteBuf: ByteBuf): Array[T] = {
    val size = byteBuf.readInt()

    if (size == 0) {
      null.asInstanceOf[Array[T]]
    } else {
      val result = implicitly[ClassTag[T]].runtimeClass match {
        case bool if bool == classOf[Boolean] =>
          Array.tabulate[Boolean](size)(_ => byteBuf.readBoolean())
        case byte if byte == classOf[Byte] =>
          Array.tabulate[Byte](size)(_ => byteBuf.readByte())
        case char if char == classOf[Char] =>
          Array.tabulate[Char](size)(_ => byteBuf.readChar())
        case short if short == classOf[Short] =>
          Array.tabulate[Short](size)(_ => byteBuf.readShort())
        case int if int == classOf[Int] =>
          Array.tabulate[Int](size)(_ => byteBuf.readInt())
        case long if long == classOf[Long] =>
          Array.tabulate[Long](size)(_ => byteBuf.readLong())
        case float if float == classOf[Float] =>
          Array.tabulate[Float](size)(_ => byteBuf.readFloat())
        case double if double == classOf[Double] =>
          Array.tabulate[Double](size)(_ => byteBuf.readDouble())
        case double if double == classOf[String] =>
          Array.tabulate[String](size) { _ =>
            val size = byteBuf.readInt()
            val dst = new Array[Byte](size)
            byteBuf.readBytes(dst, 0, size)
            new String(dst)
          }
        case ser if classOf[Serialize].isAssignableFrom(ser) =>
          Array.tabulate[T](size) { _ =>
            val inRf = ser.newInstance()
            val method = ser.getMethod("deserialize", clzByteBuf)
            method.invoke(inRf, byteBuf)
            inRf.asInstanceOf[T]
          }
        case t =>
          throw new Exception(s"type ${t.toString} cannot deserialize")
      }

      result.asInstanceOf[Array[T]]
    }
  }

  def serArrBufSize[T: TypeTag](arr: Any): Int = {
    serArrBufSize(typeOf[T], arr)
  }

  def serArrBufSize(typ: Type, arr: Any): Int = {
    var len = 4
    if (arr != null) {
      val temp = arr.asInstanceOf[Array[_]]
      if (temp.nonEmpty) {
        typ match {
          case bool if bool =:= typeOf[Boolean] =>
            len += boolSize * temp.length
          case byte if byte =:= typeOf[Byte] =>
            len += temp.length
          case char if char =:= typeOf[Char] =>
            len += charSize * temp.length
          case short if short =:= typeOf[Short] =>
            len += shortSize * temp.length
          case int if int =:= typeOf[Int] =>
            len += 4 * temp.length
          case long if long =:= typeOf[Long] =>
            len += 8 * temp.length
          case float if float =:= typeOf[Float] =>
            len += 4 * temp.length
          case double if double =:= typeOf[Double] =>
            len += 8 * temp.length
          case str if str =:= typeOf[String] =>
            temp.asInstanceOf[Array[String]].foreach { e =>
              len += e.getBytes().length + 4
            }
          case ser if ser <:< typeOf[Serialize] =>
            temp.asInstanceOf[Array[Serialize]].foreach { e =>
              len += e.bufferLen()
            }
          case t =>
            throw new Exception(s"type ${t.toString} cannot serialize")
        }
      }
    }

    len
  }

  // 2. for fast map
  def serFastMap(map: Any, byteBuf: ByteBuf): Unit = {
    if (map == null) {
      byteBuf.writeInt(0)
    } else {
      map match {
        case i2bo: Int2BooleanArrayMap =>
          byteBuf.writeInt(i2bo.size())
          val iter = i2bo.int2BooleanEntrySet().fastIterator()
          while (iter.hasNext) {
            val entry = iter.next()
            byteBuf.writeInt(entry.getIntKey).writeBoolean(entry.getBooleanValue)
          }
        case i2by: Int2ByteOpenHashMap =>
          byteBuf.writeInt(i2by.size())
          val iter = i2by.int2ByteEntrySet().fastIterator()
          while (iter.hasNext) {
            val entry = iter.next()
            byteBuf.writeInt(entry.getIntKey).writeByte(entry.getByteValue)
          }
        case i2c: Int2CharOpenHashMap =>
          byteBuf.writeInt(i2c.size())
          val iter = i2c.int2CharEntrySet().fastIterator()
          while (iter.hasNext) {
            val entry = iter.next()
            byteBuf.writeInt(entry.getIntKey).writeChar(entry.getCharValue)
          }
        case i2s: Int2ShortOpenHashMap =>
          byteBuf.writeInt(i2s.size())
          val iter = i2s.int2ShortEntrySet().fastIterator()
          while (iter.hasNext) {
            val entry = iter.next()
            byteBuf.writeInt(entry.getIntKey).writeShort(entry.getShortValue)
          }
        case i2i: Int2IntOpenHashMap =>
          byteBuf.writeInt(i2i.size())
          val iter = i2i.int2IntEntrySet().fastIterator()
          while (iter.hasNext) {
            val entry = iter.next()
            byteBuf.writeInt(entry.getIntKey).writeInt(entry.getIntValue)
          }
        case i2l: Int2LongOpenHashMap =>
          byteBuf.writeInt(i2l.size())
          val iter = i2l.int2LongEntrySet().fastIterator()
          while (iter.hasNext) {
            val entry = iter.next()
            byteBuf.writeInt(entry.getIntKey).writeLong(entry.getLongValue)
          }
        case i2f: Int2FloatOpenHashMap =>
          byteBuf.writeInt(i2f.size())
          val iter = i2f.int2FloatEntrySet().fastIterator()
          while (iter.hasNext) {
            val entry = iter.next()
            byteBuf.writeInt(entry.getIntKey).writeFloat(entry.getFloatValue)
          }
        case i2d: Int2DoubleOpenHashMap =>
          byteBuf.writeInt(i2d.size())
          val iter = i2d.int2DoubleEntrySet().fastIterator()
          while (iter.hasNext) {
            val entry = iter.next()
            byteBuf.writeInt(entry.getIntKey).writeDouble(entry.getDoubleValue)
          }
        case i2o: Int2ObjectOpenHashMap[_] =>
          byteBuf.writeInt(i2o.size())
          val iter = i2o.int2ObjectEntrySet().fastIterator()
          while (iter.hasNext) {
            val entry = iter.next()
            byteBuf.writeInt(entry.getIntKey)

            entry.getValue match {
              case s: Array[_] =>
                s.head match {
                  case _: Boolean =>
                    serArr(s.asInstanceOf[Array[Boolean]], byteBuf)
                  case _: Byte =>
                    serArr(s.asInstanceOf[Array[Byte]], byteBuf)
                  case _: Char =>
                    serArr(s.asInstanceOf[Array[Char]], byteBuf)
                  case _: Short =>
                    serArr(s.asInstanceOf[Array[Short]], byteBuf)
                  case _: Int =>
                    serArr(s.asInstanceOf[Array[Int]], byteBuf)
                  case _: Long =>
                    serArr(s.asInstanceOf[Array[Long]], byteBuf)
                  case _: Float =>
                    serArr(s.asInstanceOf[Array[Float]], byteBuf)
                  case _: Double =>
                    serArr(s.asInstanceOf[Array[Double]], byteBuf)
                  case _: String =>
                    serArr(s.asInstanceOf[Array[String]], byteBuf)
                  case _: Serialize =>
                    serArr(s.asInstanceOf[Array[Serialize]], byteBuf)
                  case _ =>
                    throw new Exception("cannot serialize value object!")
                }
              case s: Serialize =>
                s.serialize(byteBuf)
              case s: String =>
                val bytes = s.getBytes
                byteBuf.writeInt(bytes.length).writeBytes(bytes)
              case _ =>
                throw new Exception("cannot serialize value object!")
            }
          }
        case l2bo: Long2BooleanOpenHashMap =>
          byteBuf.writeInt(l2bo.size())
          val iter = l2bo.long2BooleanEntrySet().fastIterator()
          while (iter.hasNext) {
            val entry = iter.next()
            byteBuf.writeLong(entry.getLongKey).writeBoolean(entry.getBooleanValue)
          }
        case l2by: Long2ByteOpenHashMap =>
          byteBuf.writeInt(l2by.size())
          val iter = l2by.long2ByteEntrySet().fastIterator()
          while (iter.hasNext) {
            val entry = iter.next()
            byteBuf.writeLong(entry.getLongKey).writeByte(entry.getByteValue)
          }
        case l2c: Long2CharOpenHashMap =>
          byteBuf.writeInt(l2c.size())
          val iter = l2c.long2CharEntrySet().fastIterator()
          while (iter.hasNext) {
            val entry = iter.next()
            byteBuf.writeLong(entry.getLongKey).writeChar(entry.getCharValue)
          }
        case l2s: Long2ShortOpenHashMap =>
          byteBuf.writeInt(l2s.size())
          val iter = l2s.long2ShortEntrySet().fastIterator()
          while (iter.hasNext) {
            val entry = iter.next()
            byteBuf.writeLong(entry.getLongKey).writeShort(entry.getShortValue)
          }
        case l2i: Long2IntOpenHashMap =>
          byteBuf.writeInt(l2i.size())
          val iter = l2i.long2IntEntrySet().fastIterator()
          while (iter.hasNext) {
            val entry = iter.next()
            byteBuf.writeLong(entry.getLongKey).writeInt(entry.getIntValue)
          }
        case l2l: Long2LongOpenHashMap =>
          byteBuf.writeInt(l2l.size())
          val iter = l2l.long2LongEntrySet().fastIterator()
          while (iter.hasNext) {
            val entry = iter.next()
            byteBuf.writeLong(entry.getLongKey).writeLong(entry.getLongValue)
          }
        case l2f: Long2FloatOpenHashMap =>
          byteBuf.writeInt(l2f.size())
          val iter = l2f.long2FloatEntrySet().fastIterator()
          while (iter.hasNext) {
            val entry = iter.next()
            byteBuf.writeLong(entry.getLongKey).writeFloat(entry.getFloatValue)
          }
        case l2d: Long2DoubleOpenHashMap =>
          byteBuf.writeInt(l2d.size())
          val iter = l2d.long2DoubleEntrySet().fastIterator()
          while (iter.hasNext) {
            val entry = iter.next()
            byteBuf.writeLong(entry.getLongKey).writeDouble(entry.getDoubleValue)
          }
        case l2o: Long2ObjectOpenHashMap[_] =>
          byteBuf.writeInt(l2o.size())
          val iter = l2o.long2ObjectEntrySet().fastIterator()
          while (iter.hasNext) {
            val entry = iter.next()
            byteBuf.writeLong(entry.getLongKey)

            entry.getValue match {
              case s: Array[_] =>
                s.head match {
                  case _: Boolean =>
                    serArr(s.asInstanceOf[Array[Boolean]], byteBuf)
                  case _: Byte =>
                    serArr(s.asInstanceOf[Array[Byte]], byteBuf)
                  case _: Char =>
                    serArr(s.asInstanceOf[Array[Char]], byteBuf)
                  case _: Short =>
                    serArr(s.asInstanceOf[Array[Short]], byteBuf)
                  case _: Int =>
                    serArr(s.asInstanceOf[Array[Int]], byteBuf)
                  case _: Long =>
                    serArr(s.asInstanceOf[Array[Long]], byteBuf)
                  case _: Float =>
                    serArr(s.asInstanceOf[Array[Float]], byteBuf)
                  case _: Double =>
                    serArr(s.asInstanceOf[Array[Double]], byteBuf)
                  case _: String =>
                    serArr(s.asInstanceOf[Array[String]], byteBuf)
                  case _: Serialize =>
                    serArr(s.asInstanceOf[Array[Serialize]], byteBuf)
                  case _ =>
                    throw new Exception("cannot serialize value object!")
                }
              case s: Serialize =>
                s.serialize(byteBuf)
              case s: String =>
                val bytes = s.getBytes
                byteBuf.writeInt(bytes.length).writeBytes(bytes)
              case _ =>
                throw new Exception("cannot serialize value object!")
            }
          }
        case _ =>
          throw new Exception("cannot serialized fast map!")
      }
    }
  }

  def serFastMapBufSize(map: Any): Int = {
    var len = 4
    if (map != null) {
      map match {
        case i2bo: Int2BooleanArrayMap =>
          len += (4 + boolSize) * i2bo.size()
        case i2by: Int2ByteOpenHashMap =>
          len += 5 * i2by.size()
        case i2c: Int2CharOpenHashMap =>
          len += (4 + charSize) * i2c.size()
        case i2s: Int2ShortOpenHashMap =>
          len += (4 + shortSize) * i2s.size()
        case i2i: Int2IntOpenHashMap =>
          len += 8 * i2i.size()
        case i2l: Int2LongOpenHashMap =>
          len += 12 * i2l.size()
        case i2f: Int2FloatOpenHashMap =>
          len += 8 * i2f.size()
        case i2d: Int2DoubleOpenHashMap =>
          len += 12 * i2d.size()
        case i2o: Int2ObjectOpenHashMap[_] =>
          val iter = i2o.int2ObjectEntrySet().fastIterator()
          while (iter.hasNext) {
            val entry = iter.next()
            len += 4

            entry.getValue match {
              case s: Array[_] =>
                s.head match {
                  case _: Boolean =>
                    len += serArrBufSize(s.asInstanceOf[Array[Boolean]])
                  case _: Byte =>
                    len += serArrBufSize(s.asInstanceOf[Array[Byte]])
                  case _: Char =>
                    len += serArrBufSize(s.asInstanceOf[Array[Char]])
                  case _: Short =>
                    len += serArrBufSize(s.asInstanceOf[Array[Short]])
                  case _: Int =>
                    len += serArrBufSize(s.asInstanceOf[Array[Int]])
                  case _: Long =>
                    len += serArrBufSize(s.asInstanceOf[Array[Long]])
                  case _: Float =>
                    len += serArrBufSize(s.asInstanceOf[Array[Float]])
                  case _: Double =>
                    len += serArrBufSize(s.asInstanceOf[Array[Double]])
                  case _: String =>
                    len += serArrBufSize(s.asInstanceOf[Array[String]])
                  case _: Serialize =>
                    len += serArrBufSize(s.asInstanceOf[Array[Serialize]])
                  case _ =>
                    throw new Exception("cannot serialize value object!")
                }
              case s: Serialize =>
                len += s.bufferLen()
              case s: String =>
                len += 4 + s.getBytes.length
              case _ =>
                throw new Exception("cannot serialize value object!")
            }
          }
        case l2bo: Long2BooleanOpenHashMap =>
          len += (8 + boolSize) * l2bo.size()
        case l2by: Long2ByteOpenHashMap =>
          len += 9 * l2by.size()
        case l2c: Long2CharOpenHashMap =>
          len += (8 + charSize) * l2c.size()
        case l2s: Long2ShortOpenHashMap =>
          len += (8 + shortSize) * l2s.size()
        case l2i: Long2IntOpenHashMap =>
          len += 12 * l2i.size()
        case l2l: Long2LongOpenHashMap =>
          len += 16 * l2l.size()
        case l2f: Long2FloatOpenHashMap =>
          len += 12 * l2f.size()
        case l2d: Long2DoubleOpenHashMap =>
          len += 16 * l2d.size()
        case l2o: Long2ObjectOpenHashMap[_] =>
          val iter = l2o.long2ObjectEntrySet().fastIterator()
          while (iter.hasNext) {
            val entry = iter.next()
            len += 8

            entry.getValue match {
              case s: Array[_] =>
                s.head match {
                  case _: Boolean =>
                    len += serArrBufSize(s.asInstanceOf[Array[Boolean]])
                  case _: Byte =>
                    len += serArrBufSize(s.asInstanceOf[Array[Byte]])
                  case _: Char =>
                    len += serArrBufSize(s.asInstanceOf[Array[Char]])
                  case _: Short =>
                    len += serArrBufSize(s.asInstanceOf[Array[Short]])
                  case _: Int =>
                    len += serArrBufSize(s.asInstanceOf[Array[Int]])
                  case _: Long =>
                    len += serArrBufSize(s.asInstanceOf[Array[Long]])
                  case _: Float =>
                    len += serArrBufSize(s.asInstanceOf[Array[Float]])
                  case _: Double =>
                    len += serArrBufSize(s.asInstanceOf[Array[Double]])
                  case _: String =>
                    len += serArrBufSize(s.asInstanceOf[Array[String]])
                  case _: Serialize =>
                    len += serArrBufSize(s.asInstanceOf[Array[Serialize]])
                  case _ =>
                    throw new Exception("cannot serialize value object!")
                }
              case s: Serialize =>
                len += s.bufferLen()
              case s: String =>
                len += 4 + s.getBytes.length
              case _ =>
                throw new Exception("cannot serialize value object!")
            }
          }
        case _ =>
          throw new Exception("cannot serialized fast map!")
      }
    }

    len
  }

  def serFastMap[T: ClassTag](map: Any, keys: Array[T], start: Int, end: Int, byteBuf: ByteBuf): Unit = {
    if (end - start > 0) {
      byteBuf.writeInt(end - start)
      implicitly[ClassTag[T]].runtimeClass match {
        case t if t == classOf[Int] =>
          map match {
            case i2bo: Int2BooleanOpenHashMap =>
              (start until end).foreach { idx =>
                val key = keys(idx).asInstanceOf[Int]
                byteBuf.writeInt(key).writeBoolean(i2bo.get(key))
              }
            case i2by: Int2ByteOpenHashMap =>
              (start until end).foreach { idx =>
                val key = keys(idx).asInstanceOf[Int]
                byteBuf.writeInt(key).writeByte(i2by.get(key))
              }
            case i2c: Int2CharOpenHashMap =>
              (start until end).foreach { idx =>
                val key = keys(idx).asInstanceOf[Int]
                byteBuf.writeInt(key).writeChar(i2c.get(key))
              }
            case i2s: Int2ShortOpenHashMap =>
              (start until end).foreach { idx =>
                val key = keys(idx).asInstanceOf[Int]
                byteBuf.writeInt(key).writeShort(i2s.get(key))
              }
            case i2i: Int2IntOpenHashMap =>
              (start until end).foreach { idx =>
                val key = keys(idx).asInstanceOf[Int]
                byteBuf.writeInt(key).writeInt(i2i.get(key))
              }
            case i2l: Int2LongOpenHashMap =>
              (start until end).foreach { idx =>
                val key = keys(idx).asInstanceOf[Int]
                byteBuf.writeInt(key).writeLong(i2l.get(key))
              }
            case i2f: Int2FloatOpenHashMap =>
              (start until end).foreach { idx =>
                val key = keys(idx).asInstanceOf[Int]
                byteBuf.writeInt(key).writeFloat(i2f.get(key))
              }
            case i2d: Int2DoubleOpenHashMap =>
              (start until end).foreach { idx =>
                val key = keys(idx).asInstanceOf[Int]
                byteBuf.writeInt(key).writeDouble(i2d.get(key))
              }
            case i2o: Int2ObjectOpenHashMap[_] =>
              (start until end).foreach { idx =>
                val key = keys(idx).asInstanceOf[Int]
                byteBuf.writeInt(key)

                i2o.get(key) match {
                  case s: Array[_] =>
                    s.head match {
                      case _: Boolean =>
                        serArr(s.asInstanceOf[Array[Boolean]], byteBuf)
                      case _: Byte =>
                        serArr(s.asInstanceOf[Array[Byte]], byteBuf)
                      case _: Char =>
                        serArr(s.asInstanceOf[Array[Char]], byteBuf)
                      case _: Short =>
                        serArr(s.asInstanceOf[Array[Short]], byteBuf)
                      case _: Int =>
                        serArr(s.asInstanceOf[Array[Int]], byteBuf)
                      case _: Long =>
                        serArr(s.asInstanceOf[Array[Long]], byteBuf)
                      case _: Float =>
                        serArr(s.asInstanceOf[Array[Float]], byteBuf)
                      case _: Double =>
                        serArr(s.asInstanceOf[Array[Double]], byteBuf)
                      case _: String =>
                        serArr(s.asInstanceOf[Array[String]], byteBuf)
                      case _: Serialize =>
                        serArr(s.asInstanceOf[Array[Serialize]], byteBuf)
                      case _ =>
                        throw new Exception("cannot serialize value object!")
                    }
                  case v: Serialize =>
                    v.serialize(byteBuf)
                  case str: String =>
                    val bytes = str.getBytes
                    byteBuf.writeInt(bytes.length).writeBytes(str.getBytes)
                  case _ =>
                    throw new Exception("element type is not supported!")
                }
              }
            case _ =>
              throw new Exception("type is not supported!")
          }
        case t if t == classOf[Long] =>
          map match {
            case l2by: Long2BooleanOpenHashMap =>
              (start until end).foreach { idx =>
                val key = keys(idx).asInstanceOf[Long]
                byteBuf.writeLong(key).writeBoolean(l2by.get(key))
              }
            case l2bo: Long2ByteOpenHashMap =>
              (start until end).foreach { idx =>
                val key = keys(idx).asInstanceOf[Long]
                byteBuf.writeLong(key).writeByte(l2bo.get(key))
              }
            case l2c: Long2CharOpenHashMap =>
              (start until end).foreach { idx =>
                val key = keys(idx).asInstanceOf[Long]
                byteBuf.writeLong(key).writeChar(l2c.get(key))
              }
            case l2s: Long2ShortOpenHashMap =>
              (start until end).foreach { idx =>
                val key = keys(idx).asInstanceOf[Long]
                byteBuf.writeLong(key).writeShort(l2s.get(key))
              }
            case l2i: Long2IntOpenHashMap =>
              (start until end).foreach { idx =>
                val key = keys(idx).asInstanceOf[Long]
                byteBuf.writeLong(key).writeInt(l2i.get(key))
              }
            case l2l: Long2LongOpenHashMap =>
              (start until end).foreach { idx =>
                val key = keys(idx).asInstanceOf[Long]
                byteBuf.writeLong(key).writeLong(l2l.get(key))
              }
            case l2f: Long2FloatOpenHashMap =>
              (start until end).foreach { idx =>
                val key = keys(idx).asInstanceOf[Long]
                byteBuf.writeLong(key).writeFloat(l2f.get(key))
              }
            case l2d: Long2DoubleOpenHashMap =>
              (start until end).foreach { idx =>
                val key = keys(idx).asInstanceOf[Long]
                byteBuf.writeLong(key).writeDouble(l2d.get(key))
              }
            case l2o: Long2ObjectOpenHashMap[_] =>
              (start until end).foreach { idx =>
                val key = keys(idx).asInstanceOf[Long]
                byteBuf.writeLong(key)

                l2o.get(key) match {
                  case s: Array[_] =>
                    s.head match {
                      case _: Boolean =>
                        serArr(s.asInstanceOf[Array[Boolean]], byteBuf)
                      case _: Byte =>
                        serArr(s.asInstanceOf[Array[Byte]], byteBuf)
                      case _: Char =>
                        serArr(s.asInstanceOf[Array[Char]], byteBuf)
                      case _: Short =>
                        serArr(s.asInstanceOf[Array[Short]], byteBuf)
                      case _: Int =>
                        serArr(s.asInstanceOf[Array[Int]], byteBuf)
                      case _: Long =>
                        serArr(s.asInstanceOf[Array[Long]], byteBuf)
                      case _: Float =>
                        serArr(s.asInstanceOf[Array[Float]], byteBuf)
                      case _: Double =>
                        serArr(s.asInstanceOf[Array[Double]], byteBuf)
                      case _: String =>
                        serArr(s.asInstanceOf[Array[String]], byteBuf)
                      case _: Serialize =>
                        serArr(s.asInstanceOf[Array[Serialize]], byteBuf)
                      case _ =>
                        throw new Exception("cannot serialize value object!")
                    }
                  case v: Serialize =>
                    v.serialize(byteBuf)
                  case str: String =>
                    val bytes = str.getBytes
                    byteBuf.writeInt(bytes.length).writeBytes(str.getBytes)
                  case _ =>
                    throw new Exception("element type is not supported!")
                }
              }
            case _ =>
              throw new Exception("type is not supported!")
          }
      }
    } else if (end - start == 0) {
      byteBuf.writeInt(0)
    } else {
      throw new Exception("end < start, please check!")
    }
  }

  def serFastMapBufSize[T: ClassTag](map: Any, keys: Array[T], start: Int, end: Int): Int = {
    var len = 4
    if (end - start > 0) {
      implicitly[ClassTag[T]].runtimeClass match {
        case t if t == classOf[Int] =>
          map match {
            case _: Int2BooleanOpenHashMap =>
              len += (4 + boolSize) * (end - start)
            case _: Int2ByteOpenHashMap =>
              len += 5 * (end - start)
            case _: Int2CharOpenHashMap =>
              len += (4 + charSize) * (end - start)
            case _: Int2ShortOpenHashMap =>
              len += (4 + shortSize) * (end - start)
            case _: Int2IntOpenHashMap =>
              len += 8 * (end - start)
            case _: Int2LongOpenHashMap =>
              len += 12 * (end - start)
            case _: Int2FloatOpenHashMap =>
              len += 8 * (end - start)
            case _: Int2DoubleOpenHashMap =>
              len += 12 * (end - start)
            case i2o: Int2ObjectOpenHashMap[_] =>
              (start until end).foreach { idx =>
                val key = keys(idx).asInstanceOf[Int]
                len += 4

                i2o.get(key) match {
                  case s: Array[_] =>
                    s.head match {
                      case _: Boolean =>
                        len += serArrBufSize(s.asInstanceOf[Array[Boolean]])
                      case _: Byte =>
                        len += serArrBufSize(s.asInstanceOf[Array[Byte]])
                      case _: Char =>
                        len += serArrBufSize(s.asInstanceOf[Array[Char]])
                      case _: Short =>
                        len += serArrBufSize(s.asInstanceOf[Array[Short]])
                      case _: Int =>
                        len += serArrBufSize(s.asInstanceOf[Array[Int]])
                      case _: Long =>
                        len += serArrBufSize(s.asInstanceOf[Array[Long]])
                      case _: Float =>
                        len += serArrBufSize(s.asInstanceOf[Array[Float]])
                      case _: Double =>
                        len += serArrBufSize(s.asInstanceOf[Array[Double]])
                      case _: String =>
                        len += serArrBufSize(s.asInstanceOf[Array[String]])
                      case _: Serialize =>
                        len += serArrBufSize(s.asInstanceOf[Array[Serialize]])
                      case _ =>
                        throw new Exception("cannot serialize value object!")
                    }
                  case v: Serialize =>
                    len += v.bufferLen()
                  case str: String =>
                    len += 4 + str.getBytes.length
                  case _ =>
                    throw new Exception("element type is not supported!")
                }
              }
            case _ =>
              throw new Exception("type is not supported!")
          }
        case t if t == classOf[Long] =>
          map match {
            case _: Long2BooleanOpenHashMap =>
              len += (8 + boolSize) * (end - start)
            case _: Long2ByteOpenHashMap =>
              len += 9 * (end - start)
            case _: Long2CharOpenHashMap =>
              len += (8 + charSize) * (end - start)
            case _: Long2ShortOpenHashMap =>
              len += (8 + shortSize) * (end - start)
            case _: Long2IntOpenHashMap =>
              len += 12 * (end - start)
            case _: Long2LongOpenHashMap =>
              len += 16 * (end - start)
            case _: Long2FloatOpenHashMap =>
              len += 12 * (end - start)
            case _: Long2DoubleOpenHashMap =>
              len += 18 * (end - start)
            case l2o: Long2ObjectOpenHashMap[_] =>
              (start until end).foreach { idx =>
                val key = keys(idx).asInstanceOf[Long]
                len += 8

                l2o.get(key) match {
                  case s: Array[_] =>
                    s.head match {
                      case _: Boolean =>
                        len += serArrBufSize(s.asInstanceOf[Array[Boolean]])
                      case _: Byte =>
                        len += serArrBufSize(s.asInstanceOf[Array[Byte]])
                      case _: Char =>
                        len += serArrBufSize(s.asInstanceOf[Array[Char]])
                      case _: Short =>
                        len += serArrBufSize(s.asInstanceOf[Array[Short]])
                      case _: Int =>
                        len += serArrBufSize(s.asInstanceOf[Array[Int]])
                      case _: Long =>
                        len += serArrBufSize(s.asInstanceOf[Array[Long]])
                      case _: Float =>
                        len += serArrBufSize(s.asInstanceOf[Array[Float]])
                      case _: Double =>
                        len += serArrBufSize(s.asInstanceOf[Array[Double]])
                      case _: String =>
                        len += serArrBufSize(s.asInstanceOf[Array[String]])
                      case _: Serialize =>
                        len += serArrBufSize(s.asInstanceOf[Array[Serialize]])
                      case _ =>
                        throw new Exception("cannot serialize value object!")
                    }
                  case v: Serialize =>
                    len + v.bufferLen()
                  case str: String =>
                    len += 4 + str.getBytes.length
                  case _ =>
                    throw new Exception("element type is not supported!")
                }
              }
            case _ =>
              throw new Exception("type is not supported!")
          }
      }
    }

    len
  }

  def fastMapFromBuffer[T: TypeTag](byteBuf: ByteBuf): T = {
    fastMapFromBuffer(typeOf[T], byteBuf).asInstanceOf[T]
  }

  def fastMapFromBuffer(tpy: Type, byteBuf: ByteBuf): Any = {
    val size = byteBuf.readInt()
    tpy match {
      case tp if tp =:= typeOf[Int2BooleanOpenHashMap] =>
        val res = new Int2BooleanOpenHashMap(size)
        (0 until size).foreach { _ =>
          val key = byteBuf.readInt()
          val value = byteBuf.readBoolean()
          res.put(key, value)
        }
        res
      case tp if tp =:= typeOf[Int2ByteOpenHashMap] =>
        val res = new Int2ByteOpenHashMap(size)
        (0 until size).foreach { _ =>
          val key = byteBuf.readInt()
          val value = byteBuf.readByte()
          res.put(key, value)
        }
        res
      case tp if tp =:= typeOf[Int2CharOpenHashMap] =>
        val res = new Int2CharOpenHashMap(size)
        (0 until size).foreach { _ =>
          val key = byteBuf.readInt()
          val value = byteBuf.readChar()
          res.put(key, value)
        }
        res
      case tp if tp =:= typeOf[Int2ShortOpenHashMap] =>
        val res = new Int2ShortOpenHashMap(size)
        (0 until size).foreach { _ =>
          val key = byteBuf.readInt()
          val value = byteBuf.readShort()
          res.put(key, value)
        }
        res
      case tp if tp =:= typeOf[Int2IntOpenHashMap] =>
        val res = new Int2IntOpenHashMap(size)
        (0 until size).foreach { _ =>
          val key = byteBuf.readInt()
          val value = byteBuf.readInt()
          res.put(key, value)
        }
        res
      case tp if tp =:= typeOf[Int2LongOpenHashMap] =>
        val res = new Int2LongOpenHashMap(size)
        (0 until size).foreach { _ =>
          val key = byteBuf.readInt()
          val value = byteBuf.readLong()
          res.put(key, value)
        }
        res
      case tp if tp =:= typeOf[Int2FloatOpenHashMap] =>
        val res = new Int2FloatOpenHashMap(size)
        (0 until size).foreach { _ =>
          val key = byteBuf.readInt()
          val value = byteBuf.readFloat()
          res.put(key, value)
        }
        res
      case tp if tp =:= typeOf[Int2DoubleOpenHashMap] =>
        val res = new Int2DoubleOpenHashMap(size)
        (0 until size).foreach { _ =>
          val key = byteBuf.readInt()
          val value = byteBuf.readDouble()
          res.put(key, value)
        }
        res
      case tp if tp =:= typeOf[Int2ObjectOpenHashMap[String]] =>
        val res = new Int2ObjectOpenHashMap[String](size)
        (0 until size).foreach { _ =>
          val key = byteBuf.readInt()
          val len = byteBuf.readInt()
          val dst = new Array[Byte](len)
          byteBuf.readBytes(dst, 0, len)
          res.put(key, new String(dst))
        }
        res
      case tp if RObject.canSerializedIntKeyFastMap(tp) =>
        val outRf: RObject = RObject(tp)
        outRf.create(size)

        val inRf: RObject = RObject(tp.typeArgs.head)
        (0 until size).foreach { _ =>
          val key = byteBuf.readInt()
          inRf.create()
          inRf.instance.asInstanceOf[Serialize].deserialize(byteBuf)
          outRf.invoke("put", key, inRf.instance, tp.typeArgs.head)
        }

        outRf.instance

      case tp if tp =:= typeOf[Int2ObjectOpenHashMap[Array[Boolean]]] =>
        val res = new Int2ObjectOpenHashMap[Array[Boolean]](size)

        (0 until size).foreach { _ =>
          res.put(byteBuf.readInt(), arrFromBuffer[Boolean](byteBuf))
        }
        res
      case tp if tp =:= typeOf[Int2ObjectOpenHashMap[Array[Byte]]] =>
        val res = new Int2ObjectOpenHashMap[Array[Byte]](size)

        (0 until size).foreach { _ =>
          res.put(byteBuf.readInt(), arrFromBuffer[Byte](byteBuf))
        }
        res
      case tp if tp =:= typeOf[Int2ObjectOpenHashMap[Array[Char]]] =>
        val res = new Int2ObjectOpenHashMap[Array[Char]](size)

        (0 until size).foreach { _ =>
          res.put(byteBuf.readInt(), arrFromBuffer[Char](byteBuf))
        }
        res
      case tp if tp =:= typeOf[Int2ObjectOpenHashMap[Array[Short]]] =>
        val res = new Int2ObjectOpenHashMap[Array[Short]](size)

        (0 until size).foreach { _ =>
          res.put(byteBuf.readInt(), arrFromBuffer[Short](byteBuf))
        }
        res
      case tp if tp =:= typeOf[Int2ObjectOpenHashMap[Array[Int]]] =>
        val res = new Int2ObjectOpenHashMap[Array[Int]](size)

        (0 until size).foreach { _ =>
          res.put(byteBuf.readInt(), arrFromBuffer[Int](byteBuf))
        }
        res
      case tp if tp =:= typeOf[Int2ObjectOpenHashMap[Array[Long]]] =>
        val res = new Int2ObjectOpenHashMap[Array[Long]](size)

        (0 until size).foreach { _ =>
          res.put(byteBuf.readInt(), arrFromBuffer[Long](byteBuf))
        }
        res
      case tp if tp =:= typeOf[Int2ObjectOpenHashMap[Array[Float]]] =>
        val res = new Int2ObjectOpenHashMap[Array[Float]](size)

        (0 until size).foreach { _ =>
          res.put(byteBuf.readInt(), arrFromBuffer[Float](byteBuf))
        }
        res
      case tp if tp =:= typeOf[Int2ObjectOpenHashMap[Array[Double]]] =>
        val res = new Int2ObjectOpenHashMap[Array[Double]](size)

        (0 until size).foreach { _ =>
          res.put(byteBuf.readInt(), arrFromBuffer[Double](byteBuf))
        }
        res

      case tp if tp == typeOf[Long2BooleanOpenHashMap] =>
        val res = new Long2BooleanOpenHashMap(size)
        (0 until size).foreach { _ =>
          val key = byteBuf.readLong()
          val value = byteBuf.readBoolean()
          res.put(key, value)
        }
        res
      case tp if tp == typeOf[Long2ByteOpenHashMap] =>
        val res = new Long2ByteOpenHashMap(size)
        (0 until size).foreach { _ =>
          val key = byteBuf.readLong()
          val value = byteBuf.readByte()
          res.put(key, value)
        }
        res
      case tp if tp == typeOf[Long2CharOpenHashMap] =>
        val res = new Long2CharOpenHashMap(size)
        (0 until size).foreach { _ =>
          val key = byteBuf.readLong()
          val value = byteBuf.readChar()
          res.put(key, value)
        }
        res
      case tp if tp == typeOf[Long2ShortOpenHashMap] =>
        val res = new Long2ShortOpenHashMap(size)
        (0 until size).foreach { _ =>
          val key = byteBuf.readLong()
          val value = byteBuf.readShort()
          res.put(key, value)
        }
        res
      case tp if tp == typeOf[Long2IntOpenHashMap] =>
        val res = new Long2IntOpenHashMap(size)
        (0 until size).foreach { _ =>
          val key = byteBuf.readLong()
          val value = byteBuf.readInt()
          res.put(key, value)
        }
        res
      case tp if tp == typeOf[Long2LongOpenHashMap] =>
        val res = new Long2LongOpenHashMap(size)
        (0 until size).foreach { _ =>
          val key = byteBuf.readLong()
          val value = byteBuf.readLong()
          res.put(key, value)
        }
        res
      case tp if tp == typeOf[Long2FloatOpenHashMap] =>
        val res = new Long2FloatOpenHashMap(size)
        (0 until size).foreach { _ =>
          val key = byteBuf.readLong()
          val value = byteBuf.readFloat()
          res.put(key, value)
        }
        res
      case tp if tp == typeOf[Long2DoubleOpenHashMap] =>
        val res = new Long2DoubleOpenHashMap(size)
        (0 until size).foreach { _ =>
          val key = byteBuf.readLong()
          val value = byteBuf.readDouble()
          res.put(key, value)
        }
        res
      case tp if tp == typeOf[Long2ObjectOpenHashMap[String]] =>
        val res = new Long2ObjectOpenHashMap[String](size)
        (0 until size).foreach { _ =>
          val key = byteBuf.readLong()
          val len = byteBuf.readInt()
          val dst = new Array[Byte](len)
          byteBuf.readBytes(dst, 0, len)
          res.put(key, new String(dst))
        }
        res
      case tp if RObject.canSerializedLongKeyFastMap(tp) =>
        val outRf: RObject = RObject(tp)
        outRf.create(size)

        val inRf: RObject = RObject(tp.typeArgs.head)
        (0 until size).foreach { _ =>
          val key = byteBuf.readLong()
          inRf.create()
          inRf.instance.asInstanceOf[Serialize].deserialize(byteBuf)
          outRf.invoke("put", key, inRf.instance, tp.typeArgs.head)
        }

        outRf.instance

      case tp if tp =:= typeOf[Long2ObjectOpenHashMap[Array[Boolean]]] =>
        val res = new Long2ObjectOpenHashMap[Array[Boolean]](size)

        (0 until size).foreach { _ =>
          res.put(byteBuf.readLong(), arrFromBuffer[Boolean](byteBuf))
        }
        res
      case tp if tp =:= typeOf[Long2ObjectOpenHashMap[Array[Byte]]] =>
        val res = new Long2ObjectOpenHashMap[Array[Byte]](size)

        (0 until size).foreach { _ =>
          res.put(byteBuf.readLong(), arrFromBuffer[Byte](byteBuf))
        }
        res
      case tp if tp =:= typeOf[Long2ObjectOpenHashMap[Array[Char]]] =>
        val res = new Long2ObjectOpenHashMap[Array[Char]](size)

        (0 until size).foreach { _ =>
          res.put(byteBuf.readLong(), arrFromBuffer[Char](byteBuf))
        }
        res
      case tp if tp =:= typeOf[Long2ObjectOpenHashMap[Array[Short]]] =>
        val res = new Long2ObjectOpenHashMap[Array[Short]](size)

        (0 until size).foreach { _ =>
          res.put(byteBuf.readLong(), arrFromBuffer[Short](byteBuf))
        }
        res
      case tp if tp =:= typeOf[Long2ObjectOpenHashMap[Array[Int]]] =>
        val res = new Long2ObjectOpenHashMap[Array[Int]](size)

        (0 until size).foreach { _ =>
          res.put(byteBuf.readLong(), arrFromBuffer[Int](byteBuf))
        }
        res
      case tp if tp =:= typeOf[Long2ObjectOpenHashMap[Array[Long]]] =>
        val res = new Long2ObjectOpenHashMap[Array[Long]](size)

        (0 until size).foreach { _ =>
          res.put(byteBuf.readLong(), arrFromBuffer[Long](byteBuf))
        }
        res
      case tp if tp =:= typeOf[Long2ObjectOpenHashMap[Array[Float]]] =>
        val res = new Long2ObjectOpenHashMap[Array[Float]](size)

        (0 until size).foreach { _ =>
          res.put(byteBuf.readLong(), arrFromBuffer[Float](byteBuf))
        }
        res
      case tp if tp =:= typeOf[Long2ObjectOpenHashMap[Array[Double]]] =>
        val res = new Long2ObjectOpenHashMap[Array[Double]](size)

        (0 until size).foreach { _ =>
          res.put(byteBuf.readLong(), arrFromBuffer[Double](byteBuf))
        }
        res
      case tp =>
        throw new Exception(s"type ${tp.toString} cannot deserialize")
    }
  }

  // 3. for vector
  def serVector(vec: Vector, byteBuf: ByteBuf): Unit = {

  }

  def serVectorBufSize(vec: Vector): Int = {
    0
  }

  def vectorFromBuffer[T: ClassTag](byteBuf: ByteBuf): T = {
    null.asInstanceOf[T]
  }

}
