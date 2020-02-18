package com.tencent.angel.graph.utils

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import com.tencent.angel.common.Serialize
import com.tencent.angel.ml.math2.storage._
import com.tencent.angel.ml.math2.vector._
import io.netty.buffer.{ByteBuf, Unpooled}
import it.unimi.dsi.fastutil.ints._
import it.unimi.dsi.fastutil.longs._

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._


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

  // 0. for primitive
  def serPrimitive(value: Any, byteBuf: ByteBuf): Unit = {
    value match {
      case bo: Boolean =>
        byteBuf.writeBoolean(bo)
      case c: Char =>
        byteBuf.writeChar(c)
      case by: Byte =>
        byteBuf.writeByte(by)
      case s: Short =>
        byteBuf.writeShort(s)
      case i: Int =>
        byteBuf.writeInt(i)
      case l: Long =>
        byteBuf.writeLong(l)
      case f: Float =>
        byteBuf.writeFloat(f)
      case d: Double =>
        byteBuf.writeDouble(d)
      case str: String =>
        val tmp = str.getBytes
        byteBuf.writeInt(tmp.length).writeBytes(tmp)
      case _ =>
        throw new Exception("the input type is not Primitive !")
    }
  }

  def primitiveFromBuffer(tpe: Type, byteBuf: ByteBuf): Any = {
    tpe match {
      case t if t =:= typeOf[Boolean] =>
        byteBuf.readBoolean()
      case t if t =:= typeOf[Byte] =>
        byteBuf.readByte()
      case t if t =:= typeOf[Char] =>
        byteBuf.readChar()
      case t if t =:= typeOf[Short] =>
        byteBuf.readShort()
      case t if t =:= typeOf[Int] =>
        byteBuf.readInt()
      case t if t =:= typeOf[Long] =>
        byteBuf.readLong()
      case t if t =:= typeOf[Float] =>
        byteBuf.readFloat()
      case t if t =:= typeOf[Double] =>
        byteBuf.readDouble()
      case t if t =:= typeOf[String] =>
        val len = byteBuf.readInt()
        val tmp = new Array[Byte](len)
        byteBuf.readBytes(tmp, 0, len)
        new String(tmp)
      case _ =>
        throw new Exception("the input type is not Primitive !")
    }
  }

  def primitiveFromBuffer[T: TypeTag](byteBuf: ByteBuf): T = {
    primitiveFromBuffer(typeOf[T], byteBuf).asInstanceOf[T]
  }

  def serPrimitiveBufSize(value: Any): Int = {
    value match {
      case _: Boolean => boolSize
      case _: Char => charSize
      case _: Byte => 1
      case _: Short => shortSize
      case _: Int => 4
      case _: Long => 8
      case _: Float => 4
      case _: Double => 8
      case str: String => str.getBytes.length + 4
      case _ =>
        throw new Exception("the input type is not Primitive !")
    }
  }

  // 1. for array
  def serArr(arr: Any, byteBuf: ByteBuf): Unit = {
    if (arr == null) {
      byteBuf.writeInt(0)
    } else {
      val temp = arr.asInstanceOf[Array[_]]
      if (temp.isEmpty) {
        byteBuf.writeInt(0)
      } else {
        byteBuf.writeInt(temp.length)
        temp.head match {
          case _: Boolean =>
            temp.asInstanceOf[Array[Boolean]].foreach(e => byteBuf.writeBoolean(e))
          case _: Byte =>
            temp.asInstanceOf[Array[Byte]].foreach(e => byteBuf.writeByte(e))
          case _: Char =>
            temp.asInstanceOf[Array[Char]].foreach(e => byteBuf.writeChar(e))
          case _: Short =>
            temp.asInstanceOf[Array[Short]].foreach(e => byteBuf.writeShort(e))
          case _: Int =>
            temp.asInstanceOf[Array[Int]].foreach(e => byteBuf.writeInt(e))
          case _: Long =>
            temp.asInstanceOf[Array[Long]].foreach(e => byteBuf.writeLong(e))
          case _: Float =>
            temp.asInstanceOf[Array[Float]].foreach(e => byteBuf.writeFloat(e))
          case _: Double =>
            temp.asInstanceOf[Array[Double]].foreach(e => byteBuf.writeDouble(e))
          case _: String =>
            temp.asInstanceOf[Array[String]].foreach { e =>
              val bytes = e.getBytes
              byteBuf.writeInt(bytes.length).writeBytes(bytes)
            }
          case t =>
            throw new Exception(s"type ${t.toString} cannot serialize")
        }
      }
    }
  }

  def serArr(arr: Any, start: Int, end: Int, byteBuf: ByteBuf): Unit = {
    if (arr == null || end - start <= 1) {
      byteBuf.writeInt(0)
    } else {
      byteBuf.writeInt(end - start)
      arr.asInstanceOf[Array[_]].head match {
        case _: Boolean =>
          val array = arr.asInstanceOf[Array[Boolean]]
          (start until end).foreach(idx => byteBuf.writeBoolean(array(idx)))
        case _: Byte =>
          val array = arr.asInstanceOf[Array[Byte]]
          (start until end).foreach(idx => byteBuf.writeByte(array(idx)))
        case _: Char =>
          val array = arr.asInstanceOf[Array[Char]]
          (start until end).foreach(idx => byteBuf.writeChar(array(idx)))
        case _: Short =>
          val array = arr.asInstanceOf[Array[Short]]
          (start until end).foreach(idx => byteBuf.writeShort(array(idx)))
        case _: Int =>
          val array = arr.asInstanceOf[Array[Int]]
          (start until end).foreach(idx => byteBuf.writeInt(array(idx)))
        case _: Long =>
          val array = arr.asInstanceOf[Array[Long]]
          (start until end).foreach(idx => byteBuf.writeLong(array(idx)))
        case _: Float =>
          val array = arr.asInstanceOf[Array[Float]]
          (start until end).foreach(idx => byteBuf.writeFloat(array(idx)))
        case _: Double =>
          val array = arr.asInstanceOf[Array[Double]]
          (start until end).foreach(idx => byteBuf.writeDouble(array(idx)))
        case _: String =>
          val array = arr.asInstanceOf[Array[String]]
          (start until end).foreach { idx =>
            val strBytes = array(idx).getBytes
            byteBuf.writeInt(strBytes.length).writeBytes(strBytes)
          }
        case t =>
          throw new Exception(s"type ${t.toString} cannot serialize")
      }
    }
  }

  def arrFromBuffer[T: TypeTag](byteBuf: ByteBuf): Array[T] = {
    arrFromBuffer(typeOf[T], byteBuf).asInstanceOf[Array[T]]
  }

  def arrFromBuffer(tpe: Type, byteBuf: ByteBuf): Any = {
    val size = byteBuf.readInt()

    if (size == 0) {
      null.asInstanceOf[Any]
    } else {
      tpe match {
        case bool if bool =:= typeOf[Boolean] =>
          Array.tabulate[Boolean](size)(_ => byteBuf.readBoolean())
        case byte if byte =:= typeOf[Byte] =>
          Array.tabulate[Byte](size)(_ => byteBuf.readByte())
        case char if char =:= typeOf[Char] =>
          Array.tabulate[Char](size)(_ => byteBuf.readChar())
        case short if short =:= typeOf[Short] =>
          Array.tabulate[Short](size)(_ => byteBuf.readShort())
        case int if int =:= typeOf[Int] =>
          Array.tabulate[Int](size)(_ => byteBuf.readInt())
        case long if long =:= typeOf[Long] =>
          Array.tabulate[Long](size)(_ => byteBuf.readLong())
        case float if float =:= typeOf[Float] =>
          Array.tabulate[Float](size)(_ => byteBuf.readFloat())
        case double if double =:= typeOf[Double] =>
          Array.tabulate[Double](size)(_ => byteBuf.readDouble())
        case double if double =:= typeOf[String] =>
          Array.tabulate[String](size) { _ =>
            val size = byteBuf.readInt()
            val dst = new Array[Byte](size)
            byteBuf.readBytes(dst, 0, size)
            new String(dst)
          }
        case t =>
          throw new Exception(s"type ${t.toString} cannot deserialize")
      }
    }
  }

  def serArrBufSize(arr: Any): Int = {
    var len = 4
    if (arr != null) {
      val temp = arr.asInstanceOf[Array[_]]
      if (temp.nonEmpty) {
        temp.head match {
          case _: Boolean =>
            len += boolSize * temp.length
          case _: Byte =>
            len += temp.length
          case _: Char =>
            len += charSize * temp.length
          case _: Short =>
            len += shortSize * temp.length
          case _: Int =>
            len += 4 * temp.length
          case _: Long =>
            len += 8 * temp.length
          case _: Float =>
            len += 4 * temp.length
          case _: Double =>
            len += 8 * temp.length
          case _: String =>
            temp.asInstanceOf[Array[String]].foreach { e =>
              len += e.getBytes().length + 4
            }
          case t =>
            throw new Exception(s"type ${t.toString} cannot serialize")
        }
      }
    }

    len
  }

  def serArrBufSize(arr: Any, start: Int, end: Int): Int = {
    var len = 4
    val length = end - start
    if (arr != null && end - start <= 1) {
      arr.asInstanceOf[Array[_]].head match {
        case _: Boolean =>
          len += length * boolSize
        case _: Byte =>
          len += length
        case _: Char =>
          len += length * charSize
        case _: Short =>
          len += length * shortSize
        case _: Int =>
          len += length * 4
        case _: Long =>
          len += length * 8
        case _: Float =>
          len += length * 4
        case _: Double =>
          len += length * 8
        case _: String =>
          val array = arr.asInstanceOf[Array[String]]
          (start until end).foreach { idx =>
            len += array(idx).getBytes.length + 4
          }
        case t =>
          throw new Exception(s"type ${t.toString} cannot serialize")
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
      case tp if GUtils.isSerIntKeyMap(tp) =>
        val outer = ReflectUtils.constructor(tp, typeOf[Int])(size)

        val put = ReflectUtils.method(outer, "put", typeOf[Int])
        (0 until size).foreach { _ =>
          val key = byteBuf.readInt()
          val inner = ReflectUtils.newInstance(tp.typeArgs.head).asInstanceOf[Serialize]
          inner.deserialize(byteBuf)
          put(key, inner)
        }

        outer
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
      case tp if GUtils.isSerLongKeyMap(tp) =>
        val outer = ReflectUtils.constructor(tp, typeOf[Int])(size)

        val put = ReflectUtils.method(outer, "put", typeOf[Long])
        (0 until size).foreach { _ =>
          val key = byteBuf.readLong()
          val inner = ReflectUtils.newInstance(tp.typeArgs.head).asInstanceOf[Serialize]
          inner.deserialize(byteBuf)
          put(key, inner)
        }

        outer
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
  def serVector(vec: Any, byteBuf: ByteBuf): Unit = {
    byteBuf.writeLong(vec.asInstanceOf[Vector].dim())

    vec match {
      case id: IntDummyVector =>
        byteBuf.writeInt(100)
        serArr(id.getIndices, byteBuf)
      case ld: LongDummyVector =>
        byteBuf.writeInt(200)
        serArr(ld.getIndices, byteBuf)
      case v: ComponentVector =>
        throw new Exception("vector type is not supported!")
      case v: Vector =>
        v.getStorage match {
          case s: IntIntSortedVectorStorage => // 111
            byteBuf.writeInt(111)
            serArr(s.getIndices, byteBuf)
            serArr(s.getValues, byteBuf)
          case s: IntIntSparseVectorStorage => // 112
            byteBuf.writeInt(112)
            val map = ReflectUtils.field(s, "map").get
              .asInstanceOf[Int2IntOpenHashMap]
            serFastMap(map, byteBuf)
          case s: IntIntDenseVectorStorage => // 113
            byteBuf.writeInt(113)
            serArr(s.getValues, byteBuf)

          case s: IntLongSortedVectorStorage => // 121
            byteBuf.writeInt(121)
            serArr(s.getIndices, byteBuf)
            serArr(s.getValues, byteBuf)
          case s: IntLongSparseVectorStorage => // 122
            byteBuf.writeInt(122)
            val map = ReflectUtils.field(s, "map").get
              .asInstanceOf[Int2LongOpenHashMap]
            serFastMap(map, byteBuf)
          case s: IntLongDenseVectorStorage => // 123
            byteBuf.writeInt(123)
            serArr(s.getValues, byteBuf)

          case s: IntFloatSortedVectorStorage => // 131
            byteBuf.writeInt(131)
            serArr(s.getIndices, byteBuf)
            serArr(s.getValues, byteBuf)
          case s: IntFloatSparseVectorStorage => // 132
            byteBuf.writeInt(132)
            val map = ReflectUtils.field(s, "map").get
              .asInstanceOf[Int2FloatOpenHashMap]
            serFastMap(map, byteBuf)
          case s: IntFloatDenseVectorStorage => // 133
            byteBuf.writeInt(133)
            serArr(s.getValues, byteBuf)

          case s: IntDoubleSortedVectorStorage => // 141
            byteBuf.writeInt(141)
            serArr(s.getIndices, byteBuf)
            serArr(s.getValues, byteBuf)
          case s: IntDoubleSparseVectorStorage => // 142
            byteBuf.writeInt(142)
            val map = ReflectUtils.field(s, "map").get
              .asInstanceOf[Int2DoubleOpenHashMap]
            serFastMap(map, byteBuf)
          case s: IntDoubleDenseVectorStorage => // 143
            byteBuf.writeInt(143)
            serArr(s.getValues, byteBuf)

          case s: LongIntSortedVectorStorage => // 211
            byteBuf.writeInt(211)
            serArr(s.getIndices, byteBuf)
            serArr(s.getValues, byteBuf)
          case s: LongIntSparseVectorStorage => // 212
            byteBuf.writeInt(212)
            val map = ReflectUtils.field(s, "map").get
              .asInstanceOf[Long2IntOpenHashMap]
            serFastMap(map, byteBuf)

          case s: LongLongSortedVectorStorage => // 221
            byteBuf.writeInt(221)
            serArr(s.getIndices, byteBuf)
            serArr(s.getValues, byteBuf)
          case s: LongLongSparseVectorStorage => // 222
            byteBuf.writeInt(222)
            val map = ReflectUtils.field(s, "map").get
              .asInstanceOf[Long2LongOpenHashMap]
            serFastMap(map, byteBuf)

          case s: LongFloatSortedVectorStorage => // 231
            byteBuf.writeInt(231)
            serArr(s.getIndices, byteBuf)
            serArr(s.getValues, byteBuf)
          case s: LongFloatSparseVectorStorage => // 232
            byteBuf.writeInt(232)
            val map = ReflectUtils.field(s, "map").get
              .asInstanceOf[Long2FloatOpenHashMap]
            serFastMap(map, byteBuf)

          case s: LongDoubleSortedVectorStorage => // 241
            byteBuf.writeInt(241)
            serArr(s.getIndices, byteBuf)
            serArr(s.getValues, byteBuf)
          case s: LongDoubleSparseVectorStorage => // 242
            byteBuf.writeInt(242)
            val map = ReflectUtils.field(s, "map").get
              .asInstanceOf[Long2DoubleOpenHashMap]
            serFastMap(map, byteBuf)
          case _ =>
            throw new Exception("vector type is not supported!")
        }
    }
  }

  def vectorFromBuffer(tpe: Type, byteBuf: ByteBuf): Any = {
    assert(tpe <:< typeOf[Vector])
    val dim = byteBuf.readLong()

    byteBuf.readInt() match {
      case 100 => // IntDummyVector
        val indices = arrFromBuffer[Int](byteBuf)
        new IntDummyVector(dim.toInt, indices)
      case 200 => // LongDummyVector
        val indices = arrFromBuffer[Long](byteBuf)
        new LongDummyVector(dim, indices)

      case 111 => // IntIntSortedVectorStorage
        val indices = arrFromBuffer[Int](byteBuf)
        val values = arrFromBuffer[Int](byteBuf)
        val storage = new IntIntSortedVectorStorage(dim.toInt, indices, values)
        new IntIntVector(dim.toInt, storage)
      case 112 => // IntIntSparseVectorStorage
        val map = fastMapFromBuffer[Int2IntOpenHashMap](byteBuf)
        val storage = new IntIntSparseVectorStorage(dim.toInt, map)
        new IntIntVector(dim.toInt, storage)
      case 113 => // IntIntDenseVectorStorage
        val values = arrFromBuffer[Int](byteBuf)
        val storage = new IntIntDenseVectorStorage(values)
        new IntIntVector(dim.toInt, storage)

      case 121 => // IntLongSortedVectorStorage
        val indices = arrFromBuffer[Int](byteBuf)
        val values = arrFromBuffer[Long](byteBuf)
        val storage = new IntLongSortedVectorStorage(dim.toInt, indices, values)
        new IntLongVector(dim.toInt, storage)
      case 122 => // IntLongSparseVectorStorage
        val map = fastMapFromBuffer[Int2LongOpenHashMap](byteBuf)
        val storage = new IntLongSparseVectorStorage(dim.toInt, map)
        new IntLongVector(dim.toInt, storage)
      case 123 => // IntLongDenseVectorStorage
        val values = arrFromBuffer[Long](byteBuf)
        val storage = new IntLongDenseVectorStorage(values)
        new IntLongVector(dim.toInt, storage)

      case 131 => // IntFloatSortedVectorStorage
        val indices = arrFromBuffer[Int](byteBuf)
        val values = arrFromBuffer[Float](byteBuf)
        val storage = new IntFloatSortedVectorStorage(dim.toInt, indices, values)
        new IntFloatVector(dim.toInt, storage)
      case 132 => // IntFloatSparseVectorStorage
        val map = fastMapFromBuffer[Int2FloatOpenHashMap](byteBuf)
        val storage = new IntFloatSparseVectorStorage(dim.toInt, map)
        new IntFloatVector(dim.toInt, storage)
      case 133 => // IntFloatDenseVectorStorage
        val values = arrFromBuffer[Float](byteBuf)
        val storage = new IntFloatDenseVectorStorage(values)
        new IntFloatVector(dim.toInt, storage)

      case 141 => // IntDoubleSortedVectorStorage
        val indices = arrFromBuffer[Int](byteBuf)
        val values = arrFromBuffer[Double](byteBuf)
        val storage = new IntDoubleSortedVectorStorage(dim.toInt, indices, values)
        new IntDoubleVector(dim.toInt, storage)
      case 142 => // IntDoubleSparseVectorStorage
        val map = fastMapFromBuffer[Int2DoubleOpenHashMap](byteBuf)
        val storage = new IntDoubleSparseVectorStorage(dim.toInt, map)
        new IntDoubleVector(dim.toInt, storage)
      case 143 => // IntDoubleDenseVectorStorage
        val values = arrFromBuffer[Double](byteBuf)
        val storage = new IntDoubleDenseVectorStorage(values)
        new IntDoubleVector(dim.toInt, storage)

      case 211 => // LongIntSortedVectorStorage
        val indices = arrFromBuffer[Long](byteBuf)
        val values = arrFromBuffer[Int](byteBuf)
        val storage = new LongIntSortedVectorStorage(dim.toInt, indices, values)
        new LongIntVector(dim, storage)
      case 212 => // LongIntSparseVectorStorage
        val map = fastMapFromBuffer[Long2IntOpenHashMap](byteBuf)
        val storage = new LongIntSparseVectorStorage(dim.toInt, map)
        new LongIntVector(dim, storage)

      case 221 => // LongLongSortedVectorStorage
        val indices = arrFromBuffer[Long](byteBuf)
        val values = arrFromBuffer[Long](byteBuf)
        val storage = new LongLongSortedVectorStorage(dim.toInt, indices, values)
        new LongLongVector(dim, storage)
      case 222 => // LongLongSparseVectorStorage
        val map = fastMapFromBuffer[Long2LongOpenHashMap](byteBuf)
        val storage = new LongLongSparseVectorStorage(dim.toInt, map)
        new LongLongVector(dim, storage)

      case 231 => // LongFloatSortedVectorStorage
        val indices = arrFromBuffer[Long](byteBuf)
        val values = arrFromBuffer[Float](byteBuf)
        val storage = new LongFloatSortedVectorStorage(dim.toInt, indices, values)
        new LongFloatVector(dim, storage)
      case 232 => // LongFloatSparseVectorStorage
        val map = fastMapFromBuffer[Long2FloatOpenHashMap](byteBuf)
        val storage = new LongFloatSparseVectorStorage(dim.toInt, map)
        new LongFloatVector(dim, storage)

      case 241 => // LongDoubleSortedVectorStorage
        val indices = arrFromBuffer[Long](byteBuf)
        val values = arrFromBuffer[Double](byteBuf)
        val storage = new LongDoubleSortedVectorStorage(dim.toInt, indices, values)
        new LongDoubleVector(dim, storage)
      case 242 => // LongDoubleSparseVectorStorage
        val map = fastMapFromBuffer[Long2DoubleOpenHashMap](byteBuf)
        val storage = new LongDoubleSparseVectorStorage(dim.toInt, map)
        new LongDoubleVector(dim, storage)
      case _ =>
        throw new Exception("vector type is not supported!")
    }
  }

  def vectorFromBuffer[T: TypeTag](byteBuf: ByteBuf): T = {
    vectorFromBuffer(typeOf[T], byteBuf).asInstanceOf[T]
  }

  def serVectorBufSize(vec: Any): Int = {
    val len = vec match {
      case id: IntDummyVector =>
        serArrBufSize(id.getIndices)
      case ld: LongDummyVector =>
        serArrBufSize(ld.getIndices)
      case v: ComponentVector =>
        throw new Exception("vector type is not supported!")
      case v: Vector =>
        v.getStorage match {
          case s: IntIntSortedVectorStorage => // 111
            serArrBufSize(s.getIndices) * 2
          case s: IntIntSparseVectorStorage => // 112
            val map = ReflectUtils.field(s, "map").get
              .asInstanceOf[Int2IntOpenHashMap]
            serFastMapBufSize(map)
          case s: IntIntDenseVectorStorage => // 113
            serArrBufSize(s.getValues)

          case s: IntLongSortedVectorStorage => // 121
            serArrBufSize(s.getIndices) + serArrBufSize(s.getValues)
          case s: IntLongSparseVectorStorage => // 122
            val map = ReflectUtils.field(s, "map").get
              .asInstanceOf[Int2LongOpenHashMap]
            serFastMapBufSize(map)
          case s: IntLongDenseVectorStorage => // 123
            serArrBufSize(s.getValues)

          case s: IntFloatSortedVectorStorage => // 131
            serArrBufSize(s.getIndices) + serArrBufSize(s.getValues)
          case s: IntFloatSparseVectorStorage => // 132
            val map = ReflectUtils.field(s, "map").get
              .asInstanceOf[Int2FloatOpenHashMap]
            serFastMapBufSize(map)
          case s: IntFloatDenseVectorStorage => // 133
            serArrBufSize(s.getValues)

          case s: IntDoubleSortedVectorStorage => // 141
            serArrBufSize(s.getIndices) + serArrBufSize(s.getValues)
          case s: IntDoubleSparseVectorStorage => // 142
            val map = ReflectUtils.field(s, "map").get
              .asInstanceOf[Int2DoubleOpenHashMap]
            serFastMapBufSize(map)
          case s: IntDoubleDenseVectorStorage => // 143
            serArrBufSize(s.getValues)

          case s: LongIntSortedVectorStorage => // 211
            serArrBufSize(s.getIndices) + serArrBufSize(s.getValues)
          case s: LongIntSparseVectorStorage => // 212
            val map = ReflectUtils.field(s, "map").get
              .asInstanceOf[Long2IntOpenHashMap]
            serFastMapBufSize(map)

          case s: LongLongSortedVectorStorage => // 221
            serArrBufSize(s.getIndices) + serArrBufSize(s.getValues)
          case s: LongLongSparseVectorStorage => // 222
            val map = ReflectUtils.field(s, "map").get
              .asInstanceOf[Long2LongOpenHashMap]
            serFastMapBufSize(map)

          case s: LongFloatSortedVectorStorage => // 231
            serArrBufSize(s.getIndices) + serArrBufSize(s.getValues)
          case s: LongFloatSparseVectorStorage => // 232
            val map = ReflectUtils.field(s, "map").get
              .asInstanceOf[Long2FloatOpenHashMap]
            serFastMapBufSize(map)

          case s: LongDoubleSortedVectorStorage => // 241
            serArrBufSize(s.getIndices) + serArrBufSize(s.getValues)
          case s: LongDoubleSparseVectorStorage => // 242
            val map = ReflectUtils.field(s, "map").get
              .asInstanceOf[Long2DoubleOpenHashMap]
            serFastMapBufSize(map)
          case _ =>
            throw new Exception("vector type is not supported!")
        }
    }

    len + 12
  }

  // 4. for object
  def serialize(obj: Any, fields: List[TermSymbol], byteBuf: ByteBuf): Unit = {
    val inst = obj match {
      case instMirror: InstanceMirror => instMirror
      case _ => ReflectUtils.instMirror(obj)
    }

    fields.foreach { field =>
      field.typeSignature match {
        case tpe if GUtils.isPrimitive(tpe) =>
          SerDe.serPrimitive(ReflectUtils.field(inst, field).get, byteBuf)
        case tpe if tpe <:< typeOf[Serialize] =>
          ReflectUtils.field(inst, field).get.asInstanceOf[Serialize].serialize(byteBuf)
        case tpe if GUtils.isPrimitiveArray(tpe) =>
          SerDe.serArr(ReflectUtils.field(inst, field).get, byteBuf)
        case tpe if GUtils.isFastMap(tpe) =>
          SerDe.serFastMap(ReflectUtils.field(inst, field).get, byteBuf)
        case tpe if GUtils.isVector(tpe) =>
          SerDe.serVector(ReflectUtils.field(inst, field).get.asInstanceOf[Vector], byteBuf)
        case tpe =>
          throw new Exception(s"cannot serialize field ${tpe.toString}")
      }
    }
  }

  def serialize(obj: Any, byteBuf: ByteBuf): Unit = {
    val inst = obj match {
      case instMirror: InstanceMirror => instMirror
      case _ => ReflectUtils.instMirror(obj)
    }
    val fields = ReflectUtils.getFields(inst.symbol.typeSignature)

    serialize(inst, fields, byteBuf)
  }

  def deserialize(obj: Any, fields: List[TermSymbol], byteBuf: ByteBuf): Unit = {
    val inst = obj match {
      case instMirror: InstanceMirror => instMirror
      case _ => ReflectUtils.instMirror(obj)
    }

    fields.foreach { field =>
      field.typeSignature match {
        case tpe if GUtils.isPrimitive(tpe) =>
          ReflectUtils.field(inst, field)
            .set(SerDe.primitiveFromBuffer(tpe, byteBuf))
        case tpe if tpe <:< typeOf[Serialize] =>
          val node = ReflectUtils.newInstance(tpe).asInstanceOf[Serialize]
          node.deserialize(byteBuf)
          ReflectUtils.field(inst, field).set(node)
        case tpe if GUtils.isPrimitiveArray(tpe) =>
          ReflectUtils.field(inst, field)
            .set(SerDe.arrFromBuffer(tpe.typeArgs.head, byteBuf))
        case tpe if GUtils.isFastMap(tpe) =>
          ReflectUtils.field(inst, field)
            .set(SerDe.fastMapFromBuffer(tpe, byteBuf))
        case tpe if GUtils.isVector(tpe) =>
          ReflectUtils.field(inst, field)
            .set(SerDe.vectorFromBuffer(tpe, byteBuf))
        case tpe =>
          throw new Exception(s"cannot serialize field ${tpe.toString}")
      }
    }
  }

  def deserialize(obj: Any, byteBuf: ByteBuf): Unit = {
    val inst = obj match {
      case instMirror: InstanceMirror => instMirror
      case _ => ReflectUtils.instMirror(obj)
    }
    val fields = ReflectUtils.getFields(inst.symbol.typeSignature)

    deserialize(inst, fields, byteBuf)
  }

  def bufferLen(obj: Any, fields: List[TermSymbol]): Int = {
    val inst = obj match {
      case instMirror: InstanceMirror => instMirror
      case _ => ReflectUtils.instMirror(obj)
    }

    var len = 0

    fields.foreach { field =>
      field.typeSignature match {
        case tpe if GUtils.isPrimitive(tpe) =>
          len += SerDe.serPrimitiveBufSize(ReflectUtils.field(inst, field).get)
        case tpe if tpe <:< typeOf[Serialize] =>
          len += ReflectUtils.field(inst, field).get.asInstanceOf[Serialize].bufferLen()
        case tpe if GUtils.isPrimitiveArray(tpe) =>
          len += SerDe.serArrBufSize(ReflectUtils.field(inst, field).get)
        case tpe if GUtils.isFastMap(tpe) =>
          len += SerDe.serFastMapBufSize(ReflectUtils.field(inst, field).get)
        case tpe if GUtils.isVector(tpe) =>
          len += SerDe.serVectorBufSize(ReflectUtils.field(inst, field).get.asInstanceOf[Vector])
        case tpe =>
          throw new Exception(s"cannot serialize field ${tpe.toString}")
      }
    }

    len
  }

  def bufferLen(obj: Any): Int = {
    val inst = obj match {
      case instMirror: InstanceMirror => instMirror
      case _ => ReflectUtils.instMirror(obj)
    }
    val fields = ReflectUtils.getFields(inst.symbol.typeSignature)

    bufferLen(inst, fields)
  }

  // 5. for java Serialize
  def javaSerialize(obj: Any, byteBuf: ByteBuf): Unit = {
    val baos = new ByteArrayOutputStream(2048)
    val outObj = new ObjectOutputStream(baos)

    outObj.writeObject(obj)

    outObj.flush()
    outObj.close()
    val dataObj = baos.toByteArray
    byteBuf.writeInt(dataObj.length).writeBytes(dataObj)

    baos.close()
  }

  def javaSerBufferSize(obj: Any): Int = {
    val baos = new ByteArrayOutputStream(2048)
    val outObj = new ObjectOutputStream(baos)

    outObj.writeObject(obj)

    outObj.flush()
    outObj.close()

    val size = baos.size()
    baos.close()

    size + 4
  }

  def javaSer2Bytes(obj: Any): Array[Byte] = {
    val baos = new ByteArrayOutputStream(2048)
    val outObj = new ObjectOutputStream(baos)

    outObj.writeObject(obj)

    outObj.flush()
    outObj.close()
    val dataObj = baos.toByteArray
    baos.close()

    dataObj
  }

  def javaDeserialize[T](byteBuf: ByteBuf): T = {
    val size = byteBuf.readInt()

    if (size <= 0) {
      throw new Exception("no data to deserialize!")
    }

    val dataObj = new Array[Byte](size)
    byteBuf.readBytes(dataObj)
    val bais = new ByteArrayInputStream(dataObj)
    val inObj = new ObjectInputStream(bais)

    val obj = inObj.readObject()

    inObj.close()
    if (bais != null) {
      bais.close()
    }

    obj.asInstanceOf[T]
  }

}
