package com.tencent.angel.graph.core.psf.common

import scala.reflect.runtime.universe.Type


case class PSFMCtx(private val tpe: Type, private val last: Any, private val curr: Any) {
  def getLast[T]: T = {
    last.asInstanceOf[T]
  }

  def getCurr[T]: T = {
    curr.asInstanceOf[T]
  }
}
