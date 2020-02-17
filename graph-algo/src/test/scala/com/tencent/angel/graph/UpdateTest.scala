  package com.tencent.angel.graph

  import java.io.{ByteArrayInputStream, ObjectInputStream}
  import java.util.Random

  import com.tencent.angel.graph.core.data.NodeN
  import com.tencent.angel.graph.core.psf.update.{GPartitionUpdateParam, GUpdateParam, UpdateOp}
  import com.tencent.angel.ml.matrix.RowType
  import com.tencent.angel.ps.PSContext
  import com.tencent.angel.ps.storage.vector.ServerRow
  import com.tencent.angel.spark.models.PSMatrix
  import io.netty.buffer.{ByteBuf, Unpooled}
  import it.unimi.dsi.fastutil.longs.Long2FloatOpenHashMap

  import scala.reflect.runtime.universe._

  case class Param(i: Int, l: Long, f: Float, d: Double,
                   ia: Array[Int], la: Array[Long], fa: Array[Float], da: Array[Double]) {
    def this() = this(0, 0L, 0.0f, 0.0, null, null, null, null)
  }

  class UpdateTest extends PSFunSuite with SharedPSContext {
    val directBuf: ByteBuf = Unpooled.buffer(204800)

    private val upFuncId = UpdateOp(
      (psContext: PSContext, mId: Int, pId: Int, row: ServerRow, tpe: Type, partParam: Any) => {
        println("hello world!")
        println(s"$mId, $pId")
      })
    private val rand = new Random()

    val (minId, maxId) = (0, 10000000L)
    var mat: PSMatrix = _

    override def beforeAll(): Unit = {
      super.beforeAll()
      mat = createMatrix("param", 1, minId, maxId,
        RowType.T_ANY_LONGKEY_SPARSE, classOf[NodeN])
    }

    test("UpdateOp") {
      val op1 = UpdateOp.getOp(upFuncId)

      op1(null, 1, 2, null, null, null)

      val seredOp = UpdateOp.get(upFuncId)
      val bais = new ByteArrayInputStream(seredOp)
      val inObj = new ObjectInputStream(bais)

      val op2 = inObj.readObject().asInstanceOf[UpdateOp]

      inObj.close()

      println()
      op2(null, 2, 3, null, null, null)
    }

    test("update param: primitive") {
      val params1 = GUpdateParam(mat.id, 123.8, upFuncId)

      val splits = params1.split()

      (0 until splits.size()).foreach { idx =>
        splits.get(idx).serialize(directBuf)
      }

      val dsplit = (0 until splits.size()).map { _ =>
        val pp = new GPartitionUpdateParam()
        pp.deserialize(directBuf)
        val operation = pp.operation.asInstanceOf[UpdateOp]

        operation(null, 2, 3, null, null, null)
        pp
      }

      println("OK")
    }

    test("update param: array") {
      val array = Array.tabulate[VertexId](1000) { _ => rand.nextInt(maxId.toInt).toLong }

      val params1 = GUpdateParam(mat.id, array, upFuncId)

      val splits = params1.split()

      (0 until splits.size()).foreach { idx =>
        splits.get(idx).serialize(directBuf)
      }

      val dsplit = (0 until splits.size()).map { _ =>
        val pp = new GPartitionUpdateParam()
        pp.deserialize(directBuf)
        val operation = pp.operation.asInstanceOf[UpdateOp]

        operation(null, 2, 3, null, null, null)
        pp
      }

      println("OK")
    }

    test("update param: map") {
      val map = new Long2FloatOpenHashMap(1000)
      (0 until 1000).foreach { _ =>
        var key = rand.nextInt(maxId.toInt).toLong
        if (map.containsKey(key)) {
          key = rand.nextInt(maxId.toInt).toLong
          while (map.containsKey(key)) {
            key = rand.nextInt(maxId.toInt).toLong
          }
        }
        map.put(key, rand.nextFloat())
      }

      val params1 = GUpdateParam(mat.id, map, upFuncId)

      val splits = params1.split()

      (0 until splits.size()).foreach { idx =>
        splits.get(idx).serialize(directBuf)
      }

      val dsplit = (0 until splits.size()).map { _ =>
        val pp = new GPartitionUpdateParam()
        pp.deserialize(directBuf)
        val operation = pp.operation.asInstanceOf[UpdateOp]

        operation(null, 2, 3, null, null, null)
        pp
      }

      println("OK")
    }

    test("update param: object") {
      val obj = Param(1, 2L, 3.4f, 5.6,
        Array(1, 2, 3), Array(4L, 5L, 6L, 7L),
        Array(8.9f, 0.1f, 2.3f, 4.5f, 6.7f, 8.9f),
        Array(0.1, 2.3, 4.5, 6.7)
      )

      val params1 = GUpdateParam(mat.id, obj, upFuncId)

      val splits = params1.split()

      (0 until splits.size()).foreach { idx =>
        splits.get(idx).serialize(directBuf)
      }

      val dsplit = (0 until splits.size()).map { _ =>
        val pp = new GPartitionUpdateParam()
        pp.deserialize(directBuf)
        val operation = pp.operation.asInstanceOf[UpdateOp]

        operation(null, 2, 3, null, null, null)
        pp
      }

      println("OK")
    }
  }
