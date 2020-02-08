package com.tencent.angel.graph.utils

import com.tencent.angel.common.Serialize
import io.netty.buffer.ByteBuf
import it.unimi.dsi.fastutil.ints._
import it.unimi.dsi.fastutil.longs._

import scala.reflect.runtime.universe._


class RObject(var classMirror: ClassMirror, var fields: Array[Symbol], tp: Type, var instance: Any) {
  private var instMirror: InstanceMirror = {
    if (instance != null) {
      RObject.mirror.reflect(instance)
    } else {
      null.asInstanceOf[InstanceMirror]
    }
  }

  def this(typ: Type) = {
    this(null, null, typ, null)
    this.classMirror = RObject.mirror.reflectClass(tp.typeSymbol.asClass)
    this.fields = tp.members.filter { s =>
      if (s.isTerm && !s.isMethod) {
        val str = s.toString
        if (str.startsWith("value")) {
          true
        } else if (str.startsWith("variable")) {
          true
        } else {
          false
        }
      } else {
        false
      }
    }.toArray
  }

  def this(typ: Type, inst: Any) = {
    this(null, null, typ, inst)
    this.classMirror = RObject.mirror.reflectClass(tp.typeSymbol.asClass)
    this.fields = tp.members.filter { s =>
      if (s.isTerm && !s.isMethod) {
        val str = s.toString
        if (str.startsWith("value")) {
          true
        } else if (str.startsWith("variable")) {
          true
        } else {
          false
        }
      } else {
        false
      }
    }.toArray
  }

  private def emptyConstructor(): MethodSymbol = {
    try {
      tp.decl(termNames.CONSTRUCTOR).asMethod
    } catch {
      case _: ScalaReflectionException =>
        tp.decl(termNames.CONSTRUCTOR).asTerm.alternatives
          .find(s => s.asMethod.paramLists.map(_.map(_.typeSignature)) == List(List()))
          .get.asMethod
    }
  }

  private def oneArgConstructor[T: TypeTag](): MethodSymbol = {
    oneArgConstructor(typeOf[T])
  }

  private def oneArgConstructor(tpy: Type): MethodSymbol = {
    try {
      tp.decl(termNames.CONSTRUCTOR).asMethod
    } catch {
      case _: ScalaReflectionException =>
        val alternatives = tp.decl(termNames.CONSTRUCTOR).asTerm.alternatives
        try {
          alternatives.find(s => s.asMethod.paramLists.map(_.map(_.typeSignature)) == List(List(tpy)))
            .get.asMethod
        } catch {
          case _: Exception =>
            alternatives.head.asMethod
        }
    }
  }

  private def twoArgConstructor[T1: TypeTag, T2: TypeTag](): MethodSymbol = {
    twoArgConstructor(typeOf[T1], typeOf[T2])
  }

  private def twoArgConstructor(tpy1: Type, tpy2: Type): MethodSymbol = {
    try {
      tp.decl(termNames.CONSTRUCTOR).asMethod
    } catch {
      case _: ScalaReflectionException =>
        val alternatives = tp.decl(termNames.CONSTRUCTOR).asTerm.alternatives
        try {
          alternatives.find(s => s.asMethod.paramLists.map(_.map(_.typeSignature)) == List(List(tpy1, tpy2)))
            .get.asMethod
        } catch {
          case _: Exception =>
            alternatives.head.asMethod
        }
    }
  }

  private def getEmptyMethod(name: String): MethodSymbol = {
    try {
      tp.member(TermName(name)).asMethod
    } catch {
      case _: ScalaReflectionException =>
        tp.member(TermName(name)).asTerm.alternatives
          .find(s => s.asMethod.paramLists.map(_.map(_.typeSignature)) == List(List()))
          .get.asMethod
    }
  }

  private def getOneArgMethod[T: TypeTag](name: String): MethodSymbol = {
    getOneArgMethod(name, typeOf[T])
  }

  private def getOneArgMethod(name: String, tpy: Type): MethodSymbol = {
    try {
      tp.member(TermName(name)).asMethod
    } catch {
      case _: ScalaReflectionException =>
        val alternatives = tp.member(TermName(name)).asTerm.alternatives
        try {
          tp.member(TermName(name)).asTerm.alternatives
            .find(s => s.asMethod.paramLists.map(_.map(_.typeSignature)) == List(List(tpy)))
            .get.asMethod
        } catch {
          case _: Exception =>
            alternatives.head.asMethod
        }
    }
  }

  private def getTwoArgMethod[T1: TypeTag, T2: TypeTag](name: String): MethodSymbol = {
    getTwoArgMethod(name, typeOf[T1], typeOf[T2])
  }

  private def getTwoArgMethod(name: String, tpy1: Type, tpy2: Type): MethodSymbol = {
    try {
      tp.member(TermName(name)).asMethod
    } catch {
      case _: ScalaReflectionException =>
        val alternatives = tp.member(TermName(name)).asTerm.alternatives
        try {
          tp.member(TermName(name)).asTerm.alternatives
            .find(s => s.asMethod.paramLists.map(_.map(_.typeSignature)) == List(List(tpy1, tpy2)))
            .get.asMethod
        } catch {
          case _: Exception =>
            alternatives.head.asMethod
        }
    }
  }

  def create(): Unit = {
    val constMethod = emptyConstructor()
    val constructor = classMirror.reflectConstructor(constMethod)
    instance = constructor()
    instMirror = RObject.mirror.reflect(instance)
  }

  def create[T: TypeTag](value: T): Unit = {
    val constructor = classMirror.reflectConstructor(oneArgConstructor[T]())
    instance = constructor(value)
    instMirror = RObject.mirror.reflect(instance)
  }

  def create[T1: TypeTag, T2: TypeTag](value1: T1, value2: T2): Unit = {
    val constructor = classMirror.reflectConstructor(twoArgConstructor[T1, T2]())
    instance = constructor(value1, value2)
    instMirror = RObject.mirror.reflect(instance)
  }

  def invoke(name: String): Any = {
    if (instMirror == null) {
      create()
    }

    val method = getEmptyMethod(name)
    val methodMirror = instMirror.reflectMethod(method)

    methodMirror()
  }

  def invoke[T: TypeTag](name: String, value: T): Any = {
    if (instMirror == null) {
      create()
    }

    val method = getOneArgMethod[T](name)
    val methodMirror = instMirror.reflectMethod(method)

    methodMirror(value)
  }

  def invoke(name: String, value: Any, tp: Type): Any = {
    if (instMirror == null) {
      create()
    }

    val method = getOneArgMethod(name, tp)
    val methodMirror = instMirror.reflectMethod(method)

    methodMirror(value)
  }

  def invoke[T1: TypeTag, T2: TypeTag](name: String, value1: T1, value2: T2): Any = {
    if (instMirror == null) {
      create()
    }

    val method = getTwoArgMethod[T1, T2](name)
    val methodMirror = instMirror.reflectMethod(method)

    methodMirror(value1, value2)
  }

  def invoke[T2: TypeTag](name: String, value1: Any, tpy: Type, value2: T2): Any = {
    if (instMirror == null) {
      create()
    }

    val method = getTwoArgMethod(name, tpy, typeOf[T2])
    val methodMirror = instMirror.reflectMethod(method)

    methodMirror(value1, value2)
  }

  def invoke[T1: TypeTag](name: String, value1: T1, value2: Any, tpy: Type): Any = {
    if (instMirror == null) {
      create()
    }

    val method = getTwoArgMethod(name, typeOf[T1], tpy)
    val methodMirror = instMirror.reflectMethod(method)

    methodMirror(value1, value2)
  }

  def invoke(name: String, value1: Any, tpy1: Type, value2: Any, tpy2: Type): Any = {
    if (instMirror == null) {
      create()
    }

    val method = getTwoArgMethod(name, tpy1, tpy2)
    val methodMirror = instMirror.reflectMethod(method)

    methodMirror(value1, value2)
  }

  def get(name: String): Any = {
    if (instMirror == null) {
      create()
    }

    val fieldMirror = instMirror.reflectField(tp.decl(TermName(name)).asTerm)
    fieldMirror.get
  }

  private def get(field: Symbol): Any = {
    val fieldMirror = instMirror.reflectField(field.asTerm)
    fieldMirror.get
  }

  def set(name: String, value: Any): this.type = {
    if (instMirror == null) {
      create()
    }

    val term = tp.decl(TermName(name)).asTerm
    val fieldMirror = instMirror.reflectField(term)
    fieldMirror.set(value)

    this
  }

  private def set(field: Symbol, value: Any): this.type = {
    if (instMirror == null) {
      create()
    }

    val fieldMirror = instMirror.reflectField(field.asTerm)
    fieldMirror.set(value)

    this
  }

  def serialize(byteBuf: ByteBuf): Unit = {
    fields.foreach {
      case ft if ft.typeSignature =:= typeOf[Boolean] =>
        byteBuf.writeBoolean(get(ft).asInstanceOf[Boolean])
      case ft if ft.typeSignature =:= typeOf[Byte] =>
        byteBuf.writeByte(get(ft).asInstanceOf[Byte])
      case ft if ft.typeSignature =:= typeOf[Char] =>
        byteBuf.writeChar(get(ft).asInstanceOf[Char])
      case ft if ft.typeSignature =:= typeOf[Short] =>
        byteBuf.writeShort(get(ft).asInstanceOf[Short])
      case ft if ft.typeSignature =:= typeOf[Int] =>
        byteBuf.writeInt(get(ft).asInstanceOf[Int])
      case ft if ft.typeSignature =:= typeOf[Long] =>
        byteBuf.writeLong(get(ft).asInstanceOf[Long])
      case ft if ft.typeSignature =:= typeOf[Float] =>
        byteBuf.writeFloat(get(ft).asInstanceOf[Float])
      case ft if ft.typeSignature =:= typeOf[Double] =>
        byteBuf.writeDouble(get(ft).asInstanceOf[Double])
      case ft if ft.typeSignature =:= typeOf[String] =>
        val data = get(ft).asInstanceOf[String]
        byteBuf.writeInt(data.length)
        byteBuf.writeBytes(data.getBytes)
      case ft if ft.typeSignature <:< typeOf[Serialize] =>
        get(ft).asInstanceOf[Serialize].serialize(byteBuf)

      case ft if ft.typeSignature =:= typeOf[Array[Boolean]] =>
        SerDe.serArr(ft.typeSignature.typeArgs.head, get(ft), byteBuf)
      case ft if ft.typeSignature =:= typeOf[Array[Byte]] =>
        SerDe.serArr(ft.typeSignature.typeArgs.head, get(ft), byteBuf)
      case ft if ft.typeSignature =:= typeOf[Array[Char]] =>
        SerDe.serArr(ft.typeSignature.typeArgs.head, get(ft), byteBuf)
      case ft if ft.typeSignature =:= typeOf[Array[Short]] =>
        SerDe.serArr(ft.typeSignature.typeArgs.head, get(ft), byteBuf)
      case ft if ft.typeSignature =:= typeOf[Array[Int]] =>
        SerDe.serArr(ft.typeSignature.typeArgs.head, get(ft), byteBuf)
      case ft if ft.typeSignature =:= typeOf[Array[Long]] =>
        SerDe.serArr(ft.typeSignature.typeArgs.head, get(ft), byteBuf)
      case ft if ft.typeSignature =:= typeOf[Array[Float]] =>
        SerDe.serArr(ft.typeSignature.typeArgs.head, get(ft), byteBuf)
      case ft if ft.typeSignature =:= typeOf[Array[Double]] =>
        SerDe.serArr(ft.typeSignature.typeArgs.head, get(ft), byteBuf)
      case ft if ft.typeSignature =:= typeOf[Array[String]] =>
        SerDe.serArr(ft.typeSignature.typeArgs.head, get(ft), byteBuf)
      case ft if RObject.canSerializedArray(ft) =>
        SerDe.serArr(ft.typeSignature, get(ft), byteBuf)

      case ft if ft.typeSignature =:= typeOf[Int2BooleanOpenHashMap] =>
        SerDe.serFastMap(get(ft), byteBuf)
      case ft if ft.typeSignature =:= typeOf[Int2ByteOpenHashMap] =>
        SerDe.serFastMap(get(ft), byteBuf)
      case ft if ft.typeSignature =:= typeOf[Int2CharOpenHashMap] =>
        SerDe.serFastMap(get(ft), byteBuf)
      case ft if ft.typeSignature =:= typeOf[Int2ShortOpenHashMap] =>
        SerDe.serFastMap(get(ft), byteBuf)
      case ft if ft.typeSignature =:= typeOf[Int2IntOpenHashMap] =>
        SerDe.serFastMap(get(ft), byteBuf)
      case ft if ft.typeSignature =:= typeOf[Int2LongOpenHashMap] =>
        SerDe.serFastMap(get(ft), byteBuf)
      case ft if ft.typeSignature =:= typeOf[Int2FloatOpenHashMap] =>
        SerDe.serFastMap(get(ft), byteBuf)
      case ft if ft.typeSignature =:= typeOf[Int2DoubleOpenHashMap] =>
        SerDe.serFastMap(get(ft), byteBuf)
      case ft if RObject.canSerializedIntKeyFastMap(ft) =>
        SerDe.serFastMap(get(ft), byteBuf)

      case ft if ft.typeSignature =:= typeOf[Int2ObjectOpenHashMap[Array[Boolean]]] =>
        SerDe.serFastMap(get(ft), byteBuf)
      case ft if ft.typeSignature =:= typeOf[Int2ObjectOpenHashMap[Array[Byte]]] =>
        SerDe.serFastMap(get(ft), byteBuf)
      case ft if ft.typeSignature =:= typeOf[Int2ObjectOpenHashMap[Array[Char]]] =>
        SerDe.serFastMap(get(ft), byteBuf)
      case ft if ft.typeSignature =:= typeOf[Int2ObjectOpenHashMap[Array[Short]]] =>
        SerDe.serFastMap(get(ft), byteBuf)
      case ft if ft.typeSignature =:= typeOf[Int2ObjectOpenHashMap[Array[Int]]] =>
        SerDe.serFastMap(get(ft), byteBuf)
      case ft if ft.typeSignature =:= typeOf[Int2ObjectOpenHashMap[Array[Long]]] =>
        SerDe.serFastMap(get(ft), byteBuf)
      case ft if ft.typeSignature =:= typeOf[Int2ObjectOpenHashMap[Array[Float]]] =>
        SerDe.serFastMap(get(ft), byteBuf)
      case ft if ft.typeSignature =:= typeOf[Int2ObjectOpenHashMap[Array[Double]]] =>
        SerDe.serFastMap(get(ft), byteBuf)

      case ft if ft.typeSignature =:= typeOf[Long2BooleanOpenHashMap] =>
        SerDe.serFastMap(get(ft), byteBuf)
      case ft if ft.typeSignature =:= typeOf[Long2ByteOpenHashMap] =>
        SerDe.serFastMap(get(ft), byteBuf)
      case ft if ft.typeSignature =:= typeOf[Long2CharOpenHashMap] =>
        SerDe.serFastMap(get(ft), byteBuf)
      case ft if ft.typeSignature =:= typeOf[Long2ShortOpenHashMap] =>
        SerDe.serFastMap(get(ft), byteBuf)
      case ft if ft.typeSignature =:= typeOf[Long2IntOpenHashMap] =>
        SerDe.serFastMap(get(ft), byteBuf)
      case ft if ft.typeSignature =:= typeOf[Long2LongOpenHashMap] =>
        SerDe.serFastMap(get(ft), byteBuf)
      case ft if ft.typeSignature =:= typeOf[Long2FloatOpenHashMap] =>
        SerDe.serFastMap(get(ft), byteBuf)
      case ft if ft.typeSignature =:= typeOf[Long2DoubleOpenHashMap] =>
        SerDe.serFastMap(get(ft), byteBuf)
      case ft if RObject.canSerializedLongKeyFastMap(ft) =>
        SerDe.serFastMap(get(ft), byteBuf)

      case ft if ft.typeSignature =:= typeOf[Long2ObjectOpenHashMap[Array[Boolean]]] =>
        SerDe.serFastMap(get(ft), byteBuf)
      case ft if ft.typeSignature =:= typeOf[Long2ObjectOpenHashMap[Array[Byte]]] =>
        SerDe.serFastMap(get(ft), byteBuf)
      case ft if ft.typeSignature =:= typeOf[Long2ObjectOpenHashMap[Array[Char]]] =>
        SerDe.serFastMap(get(ft), byteBuf)
      case ft if ft.typeSignature =:= typeOf[Long2ObjectOpenHashMap[Array[Short]]] =>
        SerDe.serFastMap(get(ft), byteBuf)
      case ft if ft.typeSignature =:= typeOf[Long2ObjectOpenHashMap[Array[Int]]] =>
        SerDe.serFastMap(get(ft), byteBuf)
      case ft if ft.typeSignature =:= typeOf[Long2ObjectOpenHashMap[Array[Long]]] =>
        SerDe.serFastMap(get(ft), byteBuf)
      case ft if ft.typeSignature =:= typeOf[Long2ObjectOpenHashMap[Array[Float]]] =>
        SerDe.serFastMap(get(ft), byteBuf)
      case ft if ft.typeSignature =:= typeOf[Long2ObjectOpenHashMap[Array[Double]]] =>
        SerDe.serFastMap(get(ft), byteBuf)

      case ft =>
        throw new Exception(s"cannot serialize type ${ft.toString}")

    }
  }

  def deserialize(byteBuf: ByteBuf): Unit = {
    fields.foreach {
      case ft if ft.typeSignature =:= typeOf[Boolean] =>
        set(ft, byteBuf.readBoolean())
      case ft if ft.typeSignature =:= typeOf[Byte] =>
        set(ft, byteBuf.readByte())
      case ft if ft.typeSignature =:= typeOf[Char] =>
        set(ft, byteBuf.readChar())
      case ft if ft.typeSignature =:= typeOf[Short] =>
        set(ft, byteBuf.readShort())
      case ft if ft.typeSignature =:= typeOf[Int] =>
        set(ft, byteBuf.readInt())
      case ft if ft.typeSignature =:= typeOf[Long] =>
        set(ft, byteBuf.readLong())
      case ft if ft.typeSignature =:= typeOf[Float] =>
        set(ft, byteBuf.readFloat())
      case ft if ft.typeSignature =:= typeOf[Double] =>
        set(ft, byteBuf.readDouble())
      case ft if ft.typeSignature =:= typeOf[String] =>
        val len = byteBuf.readInt()
        val dst = new Array[Byte](len)
        byteBuf.readBytes(dst)
        set(ft, new String(dst))
      case ft if ft.typeSignature <:< typeOf[Serialize] =>
        val temp = new RObject(ft.typeSignature)
        temp.create()
        temp.invoke("deserialize", byteBuf)
        set(ft, temp.instance)

      case ft if ft.typeSignature =:= typeOf[Array[Boolean]] =>
        set(ft, SerDe.arrFromBuffer[Boolean](byteBuf))
      case ft if ft.typeSignature =:= typeOf[Array[Byte]] =>
        set(ft, SerDe.arrFromBuffer[Byte](byteBuf))
      case ft if ft.typeSignature =:= typeOf[Array[Char]] =>
        set(ft, SerDe.arrFromBuffer[Char](byteBuf))
      case ft if ft.typeSignature =:= typeOf[Array[Short]] =>
        set(ft, SerDe.arrFromBuffer[Short](byteBuf))
      case ft if ft.typeSignature =:= typeOf[Array[Int]] =>
        set(ft, SerDe.arrFromBuffer[Int](byteBuf))
      case ft if ft.typeSignature =:= typeOf[Array[Long]] =>
        set(ft, SerDe.arrFromBuffer[Long](byteBuf))
      case ft if ft.typeSignature =:= typeOf[Array[Float]] =>
        set(ft, SerDe.arrFromBuffer[Float](byteBuf))
      case ft if ft.typeSignature =:= typeOf[Array[Double]] =>
        set(ft, SerDe.arrFromBuffer[Double](byteBuf))
      case ft if ft.typeSignature =:= typeOf[Array[String]] =>
        set(ft, SerDe.arrFromBuffer[String](byteBuf))
      case ft if RObject.canSerializedArray(ft) =>
        // bug
        set(ft, SerDe.arrFromBuffer[Serialize](byteBuf))

      case ft if ft.typeSignature =:= typeOf[Int2BooleanOpenHashMap] =>
        set(ft, SerDe.fastMapFromBuffer[Int2BooleanOpenHashMap](byteBuf))
      case ft if ft.typeSignature =:= typeOf[Int2ByteOpenHashMap] =>
        set(ft, SerDe.fastMapFromBuffer[Int2ByteOpenHashMap](byteBuf))
      case ft if ft.typeSignature =:= typeOf[Int2CharOpenHashMap] =>
        set(ft, SerDe.fastMapFromBuffer[Int2CharOpenHashMap](byteBuf))
      case ft if ft.typeSignature =:= typeOf[Int2ShortOpenHashMap] =>
        set(ft, SerDe.fastMapFromBuffer[Int2ShortOpenHashMap](byteBuf))
      case ft if ft.typeSignature =:= typeOf[Int2IntOpenHashMap] =>
        set(ft, SerDe.fastMapFromBuffer[Int2IntOpenHashMap](byteBuf))
      case ft if ft.typeSignature =:= typeOf[Int2LongOpenHashMap] =>
        set(ft, SerDe.fastMapFromBuffer[Int2LongOpenHashMap](byteBuf))
      case ft if ft.typeSignature =:= typeOf[Int2FloatOpenHashMap] =>
        set(ft, SerDe.fastMapFromBuffer[Int2FloatOpenHashMap](byteBuf))
      case ft if ft.typeSignature =:= typeOf[Int2DoubleOpenHashMap] =>
        set(ft, SerDe.fastMapFromBuffer[Int2DoubleOpenHashMap](byteBuf))
      case ft if RObject.canSerializedIntKeyFastMap(ft) =>
        set(ft, SerDe.fastMapFromBuffer(ft.typeSignature, byteBuf))

      case ft if ft.typeSignature =:= typeOf[Int2ObjectOpenHashMap[Array[Boolean]]] =>
        set(ft, SerDe.fastMapFromBuffer[Int2ObjectOpenHashMap[Array[Boolean]]](byteBuf))
      case ft if ft.typeSignature =:= typeOf[Int2ObjectOpenHashMap[Array[Byte]]] =>
        set(ft, SerDe.fastMapFromBuffer[Int2ObjectOpenHashMap[Array[Byte]]](byteBuf))
      case ft if ft.typeSignature =:= typeOf[Int2ObjectOpenHashMap[Array[Char]]] =>
        set(ft, SerDe.fastMapFromBuffer[Int2ObjectOpenHashMap[Array[Char]]](byteBuf))
      case ft if ft.typeSignature =:= typeOf[Int2ObjectOpenHashMap[Array[Short]]] =>
        set(ft, SerDe.fastMapFromBuffer[Int2ObjectOpenHashMap[Array[Short]]](byteBuf))
      case ft if ft.typeSignature =:= typeOf[Int2ObjectOpenHashMap[Array[Int]]] =>
        set(ft, SerDe.fastMapFromBuffer[Int2ObjectOpenHashMap[Array[Int]]](byteBuf))
      case ft if ft.typeSignature =:= typeOf[Int2ObjectOpenHashMap[Array[Long]]] =>
        set(ft, SerDe.fastMapFromBuffer[Int2ObjectOpenHashMap[Array[Long]]](byteBuf))
      case ft if ft.typeSignature =:= typeOf[Int2ObjectOpenHashMap[Array[Float]]] =>
        set(ft, SerDe.fastMapFromBuffer[Int2ObjectOpenHashMap[Array[Float]]](byteBuf))
      case ft if ft.typeSignature =:= typeOf[Int2ObjectOpenHashMap[Array[Double]]] =>
        set(ft, SerDe.fastMapFromBuffer[Int2ObjectOpenHashMap[Array[Double]]](byteBuf))

      case ft if ft.typeSignature =:= typeOf[Long2BooleanOpenHashMap] =>
        set(ft, SerDe.fastMapFromBuffer[Long2BooleanOpenHashMap](byteBuf))
      case ft if ft.typeSignature =:= typeOf[Long2ByteOpenHashMap] =>
        set(ft, SerDe.fastMapFromBuffer[Long2ByteOpenHashMap](byteBuf))
      case ft if ft.typeSignature =:= typeOf[Long2CharOpenHashMap] =>
        set(ft, SerDe.fastMapFromBuffer[Long2CharOpenHashMap](byteBuf))
      case ft if ft.typeSignature =:= typeOf[Long2ShortOpenHashMap] =>
        set(ft, SerDe.fastMapFromBuffer[Long2ShortOpenHashMap](byteBuf))
      case ft if ft.typeSignature =:= typeOf[Long2IntOpenHashMap] =>
        set(ft, SerDe.fastMapFromBuffer[Long2IntOpenHashMap](byteBuf))
      case ft if ft.typeSignature =:= typeOf[Long2LongOpenHashMap] =>
        set(ft, SerDe.fastMapFromBuffer[Long2LongOpenHashMap](byteBuf))
      case ft if ft.typeSignature =:= typeOf[Long2FloatOpenHashMap] =>
        set(ft, SerDe.fastMapFromBuffer[Long2FloatOpenHashMap](byteBuf))
      case ft if ft.typeSignature =:= typeOf[Long2DoubleOpenHashMap] =>
        set(ft, SerDe.fastMapFromBuffer[Long2DoubleOpenHashMap](byteBuf))
      case ft if RObject.canSerializedLongKeyFastMap(ft) =>
        set(ft, SerDe.fastMapFromBuffer(ft.typeSignature, byteBuf))

      case ft if ft.typeSignature =:= typeOf[Long2ObjectOpenHashMap[Array[Boolean]]] =>
        set(ft, SerDe.fastMapFromBuffer[Long2ObjectOpenHashMap[Array[Boolean]]](byteBuf))
      case ft if ft.typeSignature =:= typeOf[Long2ObjectOpenHashMap[Array[Byte]]] =>
        set(ft, SerDe.fastMapFromBuffer[Long2ObjectOpenHashMap[Array[Byte]]](byteBuf))
      case ft if ft.typeSignature =:= typeOf[Long2ObjectOpenHashMap[Array[Char]]] =>
        set(ft, SerDe.fastMapFromBuffer[Long2ObjectOpenHashMap[Array[Char]]](byteBuf))
      case ft if ft.typeSignature =:= typeOf[Long2ObjectOpenHashMap[Array[Short]]] =>
        set(ft, SerDe.fastMapFromBuffer[Long2ObjectOpenHashMap[Array[Short]]](byteBuf))
      case ft if ft.typeSignature =:= typeOf[Long2ObjectOpenHashMap[Array[Int]]] =>
        set(ft, SerDe.fastMapFromBuffer[Long2ObjectOpenHashMap[Array[Int]]](byteBuf))
      case ft if ft.typeSignature =:= typeOf[Long2ObjectOpenHashMap[Array[Long]]] =>
        set(ft, SerDe.fastMapFromBuffer[Long2ObjectOpenHashMap[Array[Long]]](byteBuf))
      case ft if ft.typeSignature =:= typeOf[Long2ObjectOpenHashMap[Array[Float]]] =>
        set(ft, SerDe.fastMapFromBuffer[Long2ObjectOpenHashMap[Array[Float]]](byteBuf))
      case ft if ft.typeSignature =:= typeOf[Long2ObjectOpenHashMap[Array[Double]]] =>
        set(ft, SerDe.fastMapFromBuffer[Long2ObjectOpenHashMap[Array[Double]]](byteBuf))

      case ft =>
        throw new Exception(s"cannot serialize type ${ft.toString}")
    }
  }

  def bufferLen(): Int = {
    var len = 0
    fields.foreach {
      case ft if ft.typeSignature =:= typeOf[Boolean] =>
        len += SerDe.boolSize
      case ft if ft.typeSignature =:= typeOf[Byte] =>
        len += 1
      case ft if ft.typeSignature =:= typeOf[Char] =>
        len += SerDe.charSize
      case ft if ft.typeSignature =:= typeOf[Short] =>
        len += SerDe.shortSize
      case ft if ft.typeSignature =:= typeOf[Int] =>
        len += 4
      case ft if ft.typeSignature =:= typeOf[Long] =>
        len += 8
      case ft if ft.typeSignature =:= typeOf[Float] =>
        len += 4
      case ft if ft.typeSignature =:= typeOf[Double] =>
        len += 8
      case ft if ft.typeSignature =:= typeOf[String] =>
        val data = get(ft).asInstanceOf[String]
        len += data.getBytes.length + 4
      case ft if ft.typeSignature <:< typeOf[Serialize] =>
        len += get(ft).asInstanceOf[Serialize].bufferLen()

      case ft if ft.typeSignature =:= typeOf[Array[Boolean]] =>
        len += SerDe.serArrBufSize(ft.typeSignature.typeArgs.head, get(ft))
      case ft if ft.typeSignature =:= typeOf[Array[Byte]] =>
        len += SerDe.serArrBufSize(ft.typeSignature.typeArgs.head, get(ft))
      case ft if ft.typeSignature =:= typeOf[Array[Char]] =>
        len += SerDe.serArrBufSize(ft.typeSignature.typeArgs.head, get(ft))
      case ft if ft.typeSignature =:= typeOf[Array[Short]] =>
        len += SerDe.serArrBufSize(ft.typeSignature.typeArgs.head, get(ft))
      case ft if ft.typeSignature =:= typeOf[Array[Int]] =>
        len += SerDe.serArrBufSize(ft.typeSignature.typeArgs.head, get(ft))
      case ft if ft.typeSignature =:= typeOf[Array[Long]] =>
        len += SerDe.serArrBufSize(ft.typeSignature.typeArgs.head, get(ft))
      case ft if ft.typeSignature =:= typeOf[Array[Float]] =>
        len += SerDe.serArrBufSize(ft.typeSignature.typeArgs.head, get(ft))
      case ft if ft.typeSignature =:= typeOf[Array[Double]] =>
        len += SerDe.serArrBufSize(ft.typeSignature.typeArgs.head, get(ft))
      case ft if ft.typeSignature =:= typeOf[Array[String]] =>
        len += SerDe.serArrBufSize(ft.typeSignature.typeArgs.head, get(ft))
      case ft if RObject.canSerializedArray(ft) =>
        len += SerDe.serArrBufSize(ft.typeSignature.typeArgs.head, get(ft))

      case ft if ft.typeSignature =:= typeOf[Int2BooleanOpenHashMap] =>
        len += SerDe.serFastMapBufSize(get(ft))
      case ft if ft.typeSignature =:= typeOf[Int2ByteOpenHashMap] =>
        len += SerDe.serFastMapBufSize(get(ft))
      case ft if ft.typeSignature =:= typeOf[Int2CharOpenHashMap] =>
        len += SerDe.serFastMapBufSize(get(ft))
      case ft if ft.typeSignature =:= typeOf[Int2ShortOpenHashMap] =>
        len += SerDe.serFastMapBufSize(get(ft))
      case ft if ft.typeSignature =:= typeOf[Int2IntOpenHashMap] =>
        len += SerDe.serFastMapBufSize(get(ft))
      case ft if ft.typeSignature =:= typeOf[Int2LongOpenHashMap] =>
        len += SerDe.serFastMapBufSize(get(ft))
      case ft if ft.typeSignature =:= typeOf[Int2FloatOpenHashMap] =>
        len += SerDe.serFastMapBufSize(get(ft))
      case ft if ft.typeSignature =:= typeOf[Int2DoubleOpenHashMap] =>
        len += SerDe.serFastMapBufSize(get(ft))
      case ft if RObject.canSerializedIntKeyFastMap(ft) =>
        len += SerDe.serFastMapBufSize(get(ft))

      case ft if ft.typeSignature =:= typeOf[Int2ObjectOpenHashMap[Array[Boolean]]] =>
        len += SerDe.serFastMapBufSize(get(ft))
      case ft if ft.typeSignature =:= typeOf[Int2ObjectOpenHashMap[Array[Byte]]] =>
        len += SerDe.serFastMapBufSize(get(ft))
      case ft if ft.typeSignature =:= typeOf[Int2ObjectOpenHashMap[Array[Char]]] =>
        len += SerDe.serFastMapBufSize(get(ft))
      case ft if ft.typeSignature =:= typeOf[Int2ObjectOpenHashMap[Array[Short]]] =>
        len += SerDe.serFastMapBufSize(get(ft))
      case ft if ft.typeSignature =:= typeOf[Int2ObjectOpenHashMap[Array[Int]]] =>
        len += SerDe.serFastMapBufSize(get(ft))
      case ft if ft.typeSignature =:= typeOf[Int2ObjectOpenHashMap[Array[Long]]] =>
        len += SerDe.serFastMapBufSize(get(ft))
      case ft if ft.typeSignature =:= typeOf[Int2ObjectOpenHashMap[Array[Float]]] =>
        len += SerDe.serFastMapBufSize(get(ft))
      case ft if ft.typeSignature =:= typeOf[Int2ObjectOpenHashMap[Array[Double]]] =>
        len += SerDe.serFastMapBufSize(get(ft))

      case ft if ft.typeSignature =:= typeOf[Long2BooleanOpenHashMap] =>
        len += SerDe.serFastMapBufSize(get(ft))
      case ft if ft.typeSignature =:= typeOf[Long2ByteOpenHashMap] =>
        len += SerDe.serFastMapBufSize(get(ft))
      case ft if ft.typeSignature =:= typeOf[Long2CharOpenHashMap] =>
        len += SerDe.serFastMapBufSize(get(ft))
      case ft if ft.typeSignature =:= typeOf[Long2ShortOpenHashMap] =>
        len += SerDe.serFastMapBufSize(get(ft))
      case ft if ft.typeSignature =:= typeOf[Long2IntOpenHashMap] =>
        len += SerDe.serFastMapBufSize(get(ft))
      case ft if ft.typeSignature =:= typeOf[Long2LongOpenHashMap] =>
        len += SerDe.serFastMapBufSize(get(ft))
      case ft if ft.typeSignature =:= typeOf[Long2FloatOpenHashMap] =>
        len += SerDe.serFastMapBufSize(get(ft))
      case ft if ft.typeSignature =:= typeOf[Long2DoubleOpenHashMap] =>
        len += SerDe.serFastMapBufSize(get(ft))
      case ft if RObject.canSerializedLongKeyFastMap(ft) =>
        len += SerDe.serFastMapBufSize(get(ft))

      case ft if ft.typeSignature =:= typeOf[Long2ObjectOpenHashMap[Array[Boolean]]] =>
        len += SerDe.serFastMapBufSize(get(ft))
      case ft if ft.typeSignature =:= typeOf[Long2ObjectOpenHashMap[Array[Byte]]] =>
        len += SerDe.serFastMapBufSize(get(ft))
      case ft if ft.typeSignature =:= typeOf[Long2ObjectOpenHashMap[Array[Char]]] =>
        len += SerDe.serFastMapBufSize(get(ft))
      case ft if ft.typeSignature =:= typeOf[Long2ObjectOpenHashMap[Array[Short]]] =>
        len += SerDe.serFastMapBufSize(get(ft))
      case ft if ft.typeSignature =:= typeOf[Long2ObjectOpenHashMap[Array[Int]]] =>
        len += SerDe.serFastMapBufSize(get(ft))
      case ft if ft.typeSignature =:= typeOf[Long2ObjectOpenHashMap[Array[Long]]] =>
        len += SerDe.serFastMapBufSize(get(ft))
      case ft if ft.typeSignature =:= typeOf[Long2ObjectOpenHashMap[Array[Float]]] =>
        len += SerDe.serFastMapBufSize(get(ft))
      case ft if ft.typeSignature =:= typeOf[Long2ObjectOpenHashMap[Array[Double]]] =>
        len += SerDe.serFastMapBufSize(get(ft))

      case ft =>
        throw new Exception(s"cannot calculate buf length of type ${ft.toString}")
    }

    len
  }
}

object RObject {
  val mirror: Mirror = runtimeMirror(getClass.getClassLoader)

  def getClassMirror(tp: Type): ClassMirror = {
    mirror.reflectClass(tp.typeSymbol.asClass)
  }

  def getFields(tp: Type): Array[Symbol] = tp.members.filter { s =>
    if (s.isTerm && !s.isMethod) {
      val str = s.toString
      if (str.startsWith("value")) {
        true
      } else if (str.startsWith("variable")) {
        true
      } else {
        false
      }
    } else {
      false
    }
  }.toArray

  def apply[T: TypeTag](obj: T): RObject = new RObject(typeOf[T], obj)

  def apply[T: TypeTag](): RObject = new RObject(typeOf[T])

  def apply(tp: Type): RObject = {
    new RObject(tp)
  }

  def canSerializedIntKeyFastMap(tpy: Symbol): Boolean = {
    tpy.typeSignature.weak_<:<(typeOf[Int2ObjectOpenHashMap[_]]) &&
      tpy.typeSignature.typeArgs.head <:< typeOf[Serialize]
  }

  def canSerializedIntKeyFastMap(tpy: Type): Boolean = {
    tpy.weak_<:<(typeOf[Int2ObjectOpenHashMap[_]]) &&
      tpy.typeArgs.head <:< typeOf[Serialize]
  }

  def canSerializedLongKeyFastMap(tpy: Symbol): Boolean = {
    tpy.typeSignature.weak_<:<(typeOf[Long2ObjectOpenHashMap[_]]) &&
      tpy.typeSignature.typeArgs.head <:< typeOf[Serialize]
  }

  def canSerializedLongKeyFastMap(tpy: Type): Boolean = {
    tpy.weak_<:<(typeOf[Long2ObjectOpenHashMap[_]]) &&
      tpy.typeArgs.head <:< typeOf[Serialize]
  }

  def canSerializedArray(tpy: Symbol): Boolean = {
    tpy.typeSignature.weak_<:<(typeOf[Array[_]]) && tpy.typeSignature.typeArgs.head <:< typeOf[Serialize]
  }

  def canSerializedArray(tpy: Type): Boolean = {
    tpy.weak_<:<(typeOf[Array[_]]) && tpy.typeArgs.head <:< typeOf[Serialize]
  }

}
