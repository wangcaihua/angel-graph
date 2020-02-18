package com.tencent.angel.graph.core.psf.common

import scala.reflect.runtime.universe._


case class PSFMCtx(private val tpe: Type, private val last: Any, private val curr: Any) {
  def getLast[T: TypeTag]: T = {
    assert(tpe =:= typeOf[T])
    last.asInstanceOf[T]
  }

  def getCurr[T: TypeTag]: T = {
    assert(tpe =:= typeOf[T])
    curr.asInstanceOf[T]
  }
}
