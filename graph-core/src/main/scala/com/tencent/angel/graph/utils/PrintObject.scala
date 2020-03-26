package com.tencent.angel.graph.utils

import scala.collection.mutable

class PrintObject(obj: Any, level: Int, visited: mutable.HashSet[String]) {
  private val clz = obj.getClass

  val prefix: String = if (level == 0) "" else Array.tabulate[String](level) { _ => "\t" }.mkString("")

  def apply(): Unit = {
    if (level <= 5) {
      clz.getFields.foreach { field =>
        if (level == 0) {
          val pstr = s"$prefix ${field.get(obj)} -> ${field.getName}: ${field.getType.getName}"

//          if (pstr.toLowerCase().contains("spark")) {
//            println(pstr)
//          }
          println(pstr)
          new PrintObject(field.get(obj), level + 1, visited)()
          //visited.add(field.getType.getName)
        } else if (!PrintObject.stopType.contains(field.getType.getName) && !visited.contains(field.getType.getName)) {
          val pstr = s"$prefix ${field.getName}: ${field.getType.getName}"
          println(pstr)
          new PrintObject(field.get(obj), level + 1, visited)()
        }
      }

      /*
      clz.getDeclaredMethods.foreach { method =>
        if (!PrintObject.stopMethod.contains(method.getName)) {
          val isAllPrimitiveParam = method.getParameterTypes.forall(p => PrintObject.stopType.contains(p.getName))
          val isPrimitiveReturn = PrintObject.stopType.contains(method.getReturnType.getName)
          if (!isAllPrimitiveParam || !isPrimitiveReturn) {
            val pstr = s"$prefix ${method.getName}(${
              method.getParameterTypes.map(p => p.getName).mkString(",")
            }):${method.getReturnType.getName}"

            if (pstr.toLowerCase().contains("spark")) {
              println(pstr)
            }
            // println(pstr)

            method.getParameterTypes.foreach { p =>
              if (!visited.contains(p.getName) && !PrintObject.stopType.contains(p.getName)) {
                new PrintObject(p, level + 1, visited)()
                visited.add(p.getName)
              }
            }

            val returnType = method.getReturnType
            if (!visited.contains(returnType.getName) && !PrintObject.stopType.contains(returnType.getName)) {
              new PrintObject(method.getReturnType, level + 1, visited)()
              visited.add(returnType.getName)
            }
          }
        }
      }
      */
    }

  }

}

object PrintObject {
  val stopMethod: Set[String] = classOf[Object].getDeclaredMethods.map(m => m.getName).toSet
  val stopType: Set[String] = List(classOf[Boolean], classOf[Byte], classOf[Short], classOf[Char],
    classOf[Int], classOf[Long], classOf[Float], classOf[Double], classOf[String],
    classOf[java.lang.Boolean], classOf[java.lang.Byte], classOf[java.lang.Short], classOf[java.lang.Character],
    classOf[java.lang.Integer], classOf[java.lang.Long], classOf[java.lang.Float], classOf[java.lang.Double], classOf[java.lang.String],
    classOf[Array[Boolean]], classOf[Array[Byte]], classOf[Array[Short]], classOf[Array[Char]],
    classOf[Array[Int]], classOf[Array[Long]], classOf[Array[Float]], classOf[Array[Double]],
    classOf[Array[String]], classOf[java.lang.Void]
  ).map(t => t.getName).toSet ++ Set("void")

  def apply(obj: Any): Unit = {
    val visited = mutable.HashSet[String]()
    stopType.foreach(x => visited.add(x))

    val printObject = new PrintObject(obj, 0, visited)
    printObject()
  }
}
