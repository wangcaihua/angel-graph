package com.tencent.angel.graph.utils

import scala.{specialized => spec}

object HashCommon {
  private val INT_PHI: Int = 0x9E3779B9
  private val LONG_PHI: Long = 0x9E3779B97F4A7C15L

  val DEFAULT_INITIAL_SIZE: Int = 16
  val DEFAULT_LOAD_FACTOR: Float = .75f
  val FAST_LOAD_FACTOR: Float = .5f
  val VERY_FAST_LOAD_FACTOR: Float = .25f

  sealed class Hasher[@spec(Long, Int) T] extends Serializable {
    def apply(o: T): Int = o.hashCode()
  }

  class LongHasher extends Hasher[Long] {
    override def apply(x: Long): Int = {
      var h: Long = x * LONG_PHI
      h ^= h >>> 32
      (h ^ (h >>> 16)).toInt
    }
  }

  class IntHasher extends Hasher[Int] {
    override def apply(x: Int): Int = {
      val h: Int = x * INT_PHI
      h ^ (h >>> 16)
    }
  }

  def nextPowerOfTwo(x: Int): Int = {
    var tmp = x
    if (tmp == 0) return 1
    tmp -= 1
    tmp |= tmp >> 1
    tmp |= tmp >> 2
    tmp |= tmp >> 4
    tmp |= tmp >> 8
    tmp |= tmp >> 16
    tmp + 1
  }

  def nextPowerOfTwo(x: Long): Long = {
    var tmp = x
    if (tmp == 0) return 1
    tmp -= 1
    tmp |= tmp >> 1
    tmp |= tmp >> 2
    tmp |= tmp >> 4
    tmp |= tmp >> 8
    tmp |= tmp >> 16
    tmp |= tmp >> 32
    tmp + 1
  }

  def calMaxFill(n: Int, f: Float): Int = {
    /* We must guarantee that there is always at least
        * one free entry (even with pathological load factors). */
    Math.min(Math.ceil(n * f).toInt, n - 1)
  }

  def calMaxFill(n: Long, f: Float): Long = {
    /* We must guarantee that there is always at least
        * one free entry (even with pathological load factors). */
    Math.min(Math.ceil(n * f).toLong, n - 1)
  }

  def arraySize(expected: Int, f: Float): Int = {
    val s: Long = Math.max(2, nextPowerOfTwo(Math.ceil(expected / f).toLong))
    if (s > (1 << 30)) {
      throw new IllegalArgumentException("Too large (" + expected + " expected elements with load factor " + f + ")")
    }
    s.toInt
  }
}
