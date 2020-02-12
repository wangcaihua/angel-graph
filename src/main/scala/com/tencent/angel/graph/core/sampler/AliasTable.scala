package com.tencent.angel.graph.core.sampler


import com.tencent.angel.graph.{VertexId, WgtTpe}
import it.unimi.dsi.fastutil.ints.{IntArrayList, IntStack}

class AliasTable(neighs: Array[VertexId], weights: Array[WgtTpe]) extends SampleOne {
  private val alias: Array[Int] = neighs.indices.toArray
  private val proba: Array[WgtTpe] = new Array[WgtTpe](weights.length)

  assert(neighs != null && weights != null)
  assert(weights.length == neighs.length && weights.length > 0)

  @transient private val wSum: WgtTpe = weights.sum.asInstanceOf[WgtTpe]
  @transient private val smaller: IntStack = new IntArrayList()
  @transient private val larger: IntStack = new IntArrayList()

  weights.zipWithIndex.foreach { case (weight, idx) =>
    proba(idx) = (weight * weights.length / wSum).asInstanceOf[WgtTpe]

    if (proba(idx) < 1.0) {
      smaller.push(idx)
    } else {
      larger.push(idx)
    }
  }

  while (!smaller.isEmpty && !larger.isEmpty) {
    val s = smaller.popInt()
    val l = larger.popInt()

    alias(s) = l
    proba(l) -= (1.0 - proba(s)).asInstanceOf[WgtTpe]
    if (proba(l) < 1.0) {
      smaller.push(l)
    } else { // proba[l] >= 1.0
      larger.push(l)
    }
  }

  while (!smaller.isEmpty) {
    smaller.popInt()
  }

  while (!larger.isEmpty) {
    smaller.popInt()
  }

  override def sample(): VertexId = {
    val idx = Sampler.rand.nextInt(alias.length)

    if (Sampler.rand.nextFloat() <= proba(idx)) {
      neighs(idx)
    } else {
      neighs(alias(idx))
    }
  }
}
