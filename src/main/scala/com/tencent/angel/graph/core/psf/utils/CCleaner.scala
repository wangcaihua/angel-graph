package com.tencent.angel.graph.core.psf.utils

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import org.apache.xbean.asm6.{ClassReader, ClassVisitor, MethodVisitor}
import com.tencent.angel.graph._
import com.tencent.angel.graph.utils.GUtils
import org.objectweb.asm.Type

import scala.language.existentials
import scala.collection.mutable.{Map, Set, Stack}

object CCleaner {
  def getClassReader(cls: Class[_]): ClassReader = {
    // Copy data over, before delegating to ClassReader - else we can run out of open file handles.
    val className = cls.getName.replaceFirst("^.*\\.", "") + ".class"
    val resourceStream = cls.getResourceAsStream(className)
    if (resourceStream == null) {
      null
    } else {
      val baos = new ByteArrayOutputStream(128)
      GUtils.copyStream(resourceStream, baos, closeStreams = true)
      new ClassReader(new ByteArrayInputStream(baos.toByteArray))
    }
  }

  private def isClosure(cls: Class[_]): Boolean = cls.getName.contains("$anonfun$")

  private def getOuterClassesAndObjects(obj: AnyRef): (List[Class[_]], List[AnyRef]) = {
    for (f <- obj.getClass.getDeclaredFields if f.getName == "$outer") {
      f.setAccessible(true)
      val outer = f.get(obj)
      // The outer pointer may be null if we have cleaned this closure before
      if (outer != null) {
        if (isClosure(f.getType)) {
          val recurRet = getOuterClassesAndObjects(outer)
          return (f.getType :: recurRet._1, outer :: recurRet._2)
        } else {
          return (f.getType :: Nil, outer :: Nil)
        }
      }
    }

    (Nil, Nil)
  }

  private def getInnerClosureClasses(obj: AnyRef): List[Class[_]] = {
    val seen = Set[Class[_]](obj.getClass)
    val stack = Stack[Class[_]](obj.getClass)
    while (!stack.isEmpty) {
      val cr = CCleaner.getClassReader(stack.pop())
      if (cr != null) {
        val set = Set.empty[Class[_]]
        cr.accept(new InnerClosureFinder(set), 0)
        for (cls <- set -- seen) {
          seen += cls
          stack.push(cls)
        }
      }
    }
    (seen - obj.getClass).toList
  }

  private def initAccessedFields(accessedFields: Map[Class[_], Set[String]],
                                 outerClasses: Seq[Class[_]]): Unit = {
    for (cls <- outerClasses) {
      var currentClass = cls
      assert(currentClass != null, "The outer class can't be null.")

      while (currentClass != null) {
        accessedFields(currentClass) = Set.empty[String]
        currentClass = currentClass.getSuperclass()
      }
    }
  }

  private def setAccessedFields(outerClass: Class[_],
                                clone: AnyRef,
                                obj: AnyRef,
                                accessedFields: Map[Class[_], Set[String]]): Unit = {
    for (fieldName <- accessedFields(outerClass)) {
      val field = outerClass.getDeclaredField(fieldName)
      field.setAccessible(true)
      val value = field.get(obj)
      field.set(clone, value)
    }
  }

  private def cloneAndSetFields(parent: AnyRef,
                                obj: AnyRef,
                                outerClass: Class[_],
                                accessedFields: Map[Class[_], Set[String]]): AnyRef = {
    val clone = instantiateClass(outerClass, parent)

    var currentClass = outerClass
    assert(currentClass != null, "The outer class can't be null.")

    while (currentClass != null) {
      setAccessedFields(currentClass, clone, obj, accessedFields)
      currentClass = currentClass.getSuperclass()
    }

    clone
  }

  def clean[F <: AnyRef](func: F): F = {
    clean(func, cleanTransitively = true, accessedFields = Map.empty)
    func
  }

  private def clean(func: AnyRef,
                    cleanTransitively: Boolean,
                    accessedFields: Map[Class[_], Set[String]]): Unit = {

    if (!isClosure(func.getClass) || func == null) {
      return
    }

    // A list of classes that represents closures enclosed in the given one
    val innerClasses = getInnerClosureClasses(func)

    // A list of enclosing objects and their respective classes, from innermost to outermost
    // An outer object at a given index is of type outer class at the same index
    val (outerClasses, outerObjects) = getOuterClassesAndObjects(func)

    // For logging purposes only
    // val declaredFields = func.getClass.getDeclaredFields
    // val declaredMethods = func.getClass.getDeclaredMethods

    // Fail fast if we detect return statements in closures
    CCleaner.getClassReader(func.getClass).accept(new ReturnStatementFinder(), 0)

    // If accessed fields is not populated yet, we assume that
    // the closure we are trying to clean is the starting one
    if (accessedFields.isEmpty) {
      initAccessedFields(accessedFields, outerClasses)
      for (cls <- func.getClass :: innerClasses) {
        CCleaner.getClassReader(cls).accept(new FieldAccessFinder(accessedFields, cleanTransitively), 0)
      }
    }

    var outerPairs: List[(Class[_], AnyRef)] = (outerClasses zip outerObjects).reverse
    var parent: AnyRef = null
    if (outerPairs.nonEmpty) {
      val (outermostClass, outermostObject) = outerPairs.head
      if (isClosure(outermostClass)) {
        println(s" + outermost object is a closure, so we clone it: ${outerPairs.head}")
      } else if (outermostClass.getName.startsWith("$line")) {
        println(s" + outermost object is a REPL line object, so we clone it: ${outerPairs.head}")
      } else {
        parent = outermostObject // e.g. SparkContext
        outerPairs = outerPairs.tail
      }
    } else {
      println(" + there are no enclosing objects!")
    }


    for ((cls, obj) <- outerPairs) {
      val clone = cloneAndSetFields(parent, obj, cls, accessedFields)
      if (cleanTransitively && isClosure(clone.getClass)) {
        clean(clone, cleanTransitively, accessedFields)
      }
      parent = clone
    }

    // Update the parent pointer ($outer) of this closure
    if (parent != null) {
      val field = func.getClass.getDeclaredField("$outer")
      field.setAccessible(true)
      if (accessedFields.contains(func.getClass) &&
        !accessedFields(func.getClass).contains("$outer")) {
        field.set(func, null)
      } else {
        // Update this closure's parent pointer to point to our enclosing object,
        // which could either be a cloned closure or the original user object
        field.set(func, parent)
      }
    }
  }

  private def instantiateClass(cls: Class[_], enclosingObject: AnyRef): AnyRef = {
    // Use reflection to instantiate object without calling constructor
    val rf = sun.reflect.ReflectionFactory.getReflectionFactory()
    val parentCtor = classOf[java.lang.Object].getDeclaredConstructor()
    val newCtor = rf.newConstructorForSerialization(cls, parentCtor)
    val obj = newCtor.newInstance().asInstanceOf[AnyRef]
    if (enclosingObject != null) {
      val field = cls.getDeclaredField("$outer")
      field.setAccessible(true)
      field.set(obj, enclosingObject)
    }
    obj
  }
}

private case class MethodIdentifier[T](cls: Class[T], name: String, desc: String)

private class FieldAccessFinder(fields: Map[Class[_], Set[String]],
                                findTransitively: Boolean,
                                specificMethod: Option[MethodIdentifier[_]] = None,
                                visitedMethods: Set[MethodIdentifier[_]] = Set.empty)
  extends ClassVisitor(ASM5) {

  override def visitMethod(access: Int,
                           name: String,
                           desc: String,
                           sig: String,
                           exceptions: Array[String]): MethodVisitor = {

    if (specificMethod.isDefined &&
      (specificMethod.get.name != name || specificMethod.get.desc != desc)) {
      return null
    }

    new MethodVisitor(ASM5) {
      override def visitFieldInsn(op: Int, owner: String, name: String, desc: String) {
        if (op == GETFIELD) {
          for (cl <- fields.keys if cl.getName == owner.replace('/', '.')) {
            fields(cl) += name
          }
        }
      }

      override def visitMethodInsn(op: Int, owner: String, name: String, desc: String, itf: Boolean) {
        for (cl <- fields.keys if cl.getName == owner.replace('/', '.')) {
          if (op == INVOKEVIRTUAL && owner.endsWith("$iwC") && !name.endsWith("$outer")) {
            fields(cl) += name
          }
          if (findTransitively) {
            val m = MethodIdentifier(cl, name, desc)
            if (!visitedMethods.contains(m)) {
              visitedMethods += m
              var currentClass = cl
              assert(currentClass != null, "The outer class can't be null.")

              while (currentClass != null) {
                CCleaner.getClassReader(currentClass).accept(
                  new FieldAccessFinder(fields, findTransitively, Some(m), visitedMethods), 0)
                currentClass = currentClass.getSuperclass
              }
            }
          }
        }
      }
    }
  }
}

private class InnerClosureFinder(output: Set[Class[_]]) extends ClassVisitor(ASM5) {
  var myName: String = null

  // TODO: Recursively find inner closures that we indirectly reference, e.g.
  //   val closure1 = () = { () => 1 }
  //   val closure2 = () => { (1 to 5).map(closure1) }
  // The second closure technically has two inner closures, but this finder only finds one

  override def visit(version: Int, access: Int, name: String, sig: String,
                     superName: String, interfaces: Array[String]) {
    myName = name
  }

  override def visitMethod(access: Int, name: String, desc: String,
                           sig: String, exceptions: Array[String]): MethodVisitor = {
    new MethodVisitor(ASM5) {
      override def visitMethodInsn(
                                    op: Int, owner: String, name: String, desc: String, itf: Boolean) {
        val argTypes = Type.getArgumentTypes(desc)
        if (op == INVOKESPECIAL && name == "<init>" && argTypes.length > 0
          && argTypes(0).toString.startsWith("L") // is it an object?
          && argTypes(0).getInternalName == myName) {
          // scalastyle:off classforname
          output += Class.forName(
            owner.replace('/', '.'),
            false,
            Thread.currentThread.getContextClassLoader)
          // scalastyle:on classforname
        }
      }
    }
  }
}

private class ReturnStatementFinder extends ClassVisitor(ASM5) {
  override def visitMethod(access: Int, name: String, desc: String,
                           sig: String, exceptions: Array[String]): MethodVisitor = {
    // $anonfun$ covers Java 8 lambdas
    if (name.contains("apply") || name.contains("$anonfun$")) {
      new MethodVisitor(ASM5) {
        override def visitTypeInsn(op: Int, tp: String) {
          if (op == NEW && tp.contains("scala/runtime/NonLocalReturnControl")) {
            throw new Exception("NonLocalReturnControl")
          }
        }
      }
    } else {
      new MethodVisitor(ASM5) {}
    }
  }
}
