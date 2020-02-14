package org.apache.spark.compat

import org.apache.spark.util.ClosureCleaner

object CUtils {
  def clean[F <: AnyRef](func: F): F ={
    ClosureCleaner.clean(func, checkSerializable=true)
    func
  }
}
