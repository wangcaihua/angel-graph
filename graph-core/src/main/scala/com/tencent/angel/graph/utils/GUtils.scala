package com.tencent.angel.graph.utils

import java.io.{FileInputStream, FileOutputStream, InputStream, OutputStream}
import java.nio.channels.FileChannel

import com.tencent.angel.graph.VertexId
import com.tencent.angel.graph.core.data.GData
import com.tencent.angel.ml.math2.vector.Vector
import it.unimi.dsi.fastutil.ints._
import it.unimi.dsi.fastutil.longs._

import scala.reflect.runtime.universe._


object GUtils {
  def isPrimitive(tpe: Type): Boolean = tpe match {
    case t if t =:= typeOf[Boolean] => true
    case t if t =:= typeOf[Char] => true
    case t if t =:= typeOf[Byte] => true
    case t if t =:= typeOf[Short] => true
    case t if t =:= typeOf[Int] => true
    case t if t =:= typeOf[Long] => true
    case t if t =:= typeOf[Float] => true
    case t if t =:= typeOf[Double] => true
    case t if t =:= typeOf[String] => true
    case _ => false
  }

  def isPrimitiveArray(tpe: Type): Boolean = tpe match {
    case t if t.weak_<:<(typeOf[Array[_]]) && isPrimitive(tpe.typeArgs.head) => true
    case _ => false
  }

  def isIntKeyFastMap(tpe: Type): Boolean = tpe match {
    case t if t =:= typeOf[Int2BooleanOpenHashMap] => true
    case t if t =:= typeOf[Int2CharOpenHashMap] => true
    case t if t =:= typeOf[Int2ByteOpenHashMap] => true
    case t if t =:= typeOf[Int2ShortOpenHashMap] => true
    case t if t =:= typeOf[Int2IntOpenHashMap] => true
    case t if t =:= typeOf[Int2LongOpenHashMap] => true
    case t if t =:= typeOf[Int2FloatOpenHashMap] => true
    case t if t =:= typeOf[Int2DoubleOpenHashMap] => true
    case t if t.weak_<:<(typeOf[Int2ObjectOpenHashMap[_]]) => true
    case _ => false
  }

  def isLongKeyFastMap(tpe: Type): Boolean = tpe match {
    case t if t =:= typeOf[Long2BooleanOpenHashMap] => true
    case t if t =:= typeOf[Long2CharOpenHashMap] => true
    case t if t =:= typeOf[Long2ByteOpenHashMap] => true
    case t if t =:= typeOf[Long2ShortOpenHashMap] => true
    case t if t =:= typeOf[Long2IntOpenHashMap] => true
    case t if t =:= typeOf[Long2LongOpenHashMap] => true
    case t if t =:= typeOf[Long2FloatOpenHashMap] => true
    case t if t =:= typeOf[Long2DoubleOpenHashMap] => true
    case t if t.weak_<:<(typeOf[Long2ObjectOpenHashMap[_]]) => true
    case _ => false
  }

  def getFastMapKeys(value: Any): Array[VertexId] = {
    typeOf[VertexId] match {
      case t if t =:= typeOf[Int] =>
        val (size, iterator) = value match {
          case v: Int2BooleanOpenHashMap => (v.size(), v.keySet().iterator())
          case v: Int2CharOpenHashMap => (v.size(), v.keySet().iterator())
          case v: Int2ByteOpenHashMap => (v.size(), v.keySet().iterator())
          case v: Int2ShortOpenHashMap => (v.size(), v.keySet().iterator())
          case v: Int2IntOpenHashMap => (v.size(), v.keySet().iterator())
          case v: Int2LongOpenHashMap => (v.size(), v.keySet().iterator())
          case v: Int2FloatOpenHashMap => (v.size(), v.keySet().iterator())
          case v: Int2DoubleOpenHashMap => (v.size(), v.keySet().iterator())
          case v: Int2ObjectOpenHashMap[_] => (v.size(), v.keySet().iterator())
          case _ =>
            throw new Exception("error type!")
        }
        Array.tabulate[VertexId](size)(_ => iterator.nextInt().asInstanceOf[VertexId])
      case t if t =:= typeOf[Long] =>
        val (size, iterator) = value match {
          case v: Long2BooleanOpenHashMap => (v.size(), v.keySet().iterator())
          case v: Long2CharOpenHashMap => (v.size(), v.keySet().iterator())
          case v: Long2ByteOpenHashMap => (v.size(), v.keySet().iterator())
          case v: Long2ShortOpenHashMap => (v.size(), v.keySet().iterator())
          case v: Long2IntOpenHashMap => (v.size(), v.keySet().iterator())
          case v: Long2LongOpenHashMap => (v.size(), v.keySet().iterator())
          case v: Long2FloatOpenHashMap => (v.size(), v.keySet().iterator())
          case v: Long2DoubleOpenHashMap => (v.size(), v.keySet().iterator())
          case v: Long2ObjectOpenHashMap[_] => (v.size(), v.keySet().iterator())
          case _ =>
            throw new Exception("error type!")
        }
        Array.tabulate[VertexId](size)(_ => iterator.nextLong().asInstanceOf[VertexId])
    }
  }

  def isFastMap(tpe: Type): Boolean = isIntKeyFastMap(tpe) || isLongKeyFastMap(tpe)

  def isVector(tpe: Type): Boolean = tpe.weak_<:<(typeOf[Vector])

  def paramCheck(tpe: Type): Boolean = {
    tpe match {
      case t if GUtils.isPrimitive(t) => true
      case t if GUtils.isPrimitiveArray(t) => true
      case t if GUtils.isFastMap(t) => true
      case t => t.weak_<:<(typeOf[GData]) && t.typeSymbol.asClass.isCaseClass
    }
  }

  def copyStream(in: InputStream, out: OutputStream,
                 closeStreams: Boolean = false, transferToEnabled: Boolean = false): Long = {
    tryWithSafeFinally {
      if (in.isInstanceOf[FileInputStream] && out.isInstanceOf[FileOutputStream]
        && transferToEnabled) {
        // When both streams are File stream, use transferTo to improve copy performance.
        val inChannel = in.asInstanceOf[FileInputStream].getChannel()
        val outChannel = out.asInstanceOf[FileOutputStream].getChannel()
        val size = inChannel.size()
        copyFileStreamNIO(inChannel, outChannel, 0, size)
        size
      } else {
        var count = 0L
        val buf = new Array[Byte](8192)
        var n = 0
        while (n != -1) {
          n = in.read(buf)
          if (n != -1) {
            out.write(buf, 0, n)
            count += n
          }
        }
        count
      }
    } {
      if (closeStreams) {
        try {
          in.close()
        } finally {
          out.close()
        }
      }
    }
  }

  private def copyFileStreamNIO(input: FileChannel, output: FileChannel,
                                startPosition: Long, bytesToCopy: Long): Unit = {
    val initialPos = output.position()
    var count = 0L
    while (count < bytesToCopy) {
      count += input.transferTo(count + startPosition, bytesToCopy - count, output)
    }
    assert(count == bytesToCopy,
      s"request to copy $bytesToCopy bytes, but actually copied $count bytes.")

    val finalPos = output.position()
    val expectedPos = initialPos + bytesToCopy
    assert(finalPos == expectedPos,
      s"""
         |Current position $finalPos do not equal to expected position $expectedPos
         |after transferTo, please check your kernel version to see if it is 2.6.32,
         |this is a kernel bug which will lead to unexpected behavior when using transferTo.
         |You can set spark.file.transferTo = false to disable this NIO feature.
           """.stripMargin)
  }

  private def tryWithSafeFinally[T](block: => T)(finallyBlock: => Unit): T = {
    var originalThrowable: Throwable = null
    try {
      block
    } catch {
      case t: Throwable =>
        originalThrowable = t
        throw originalThrowable
    } finally {
      try {
        finallyBlock
      } catch {
        case t: Throwable if originalThrowable != null && originalThrowable != t =>
          originalThrowable.addSuppressed(t)
          throw originalThrowable
      }
    }
  }
}