package com.tencent.angel.graph.utils

import scala.reflect.runtime.universe._

trait ReflectTrait {
  protected val tp: Type
  protected lazy val classMirror: ClassMirror = RObject.getClassMirror(tp)
  protected lazy val fields: Array[Symbol] = RObject.getFields(tp)

  def getRObject(obj: Any): RObject = new RObject(classMirror, fields, tp, obj)

  def getType[T: TypeTag](obj: T): Type = typeOf[T]
}

