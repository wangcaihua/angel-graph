package com.tencent.angel.graph.core.psf.common

import com.tencent.angel.graph.core.data.GData
import scala.reflect.runtime.universe._

case class Singular[T: TypeTag](partition: Int, param: T) extends GData