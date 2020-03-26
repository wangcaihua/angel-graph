package com.tencent.angel.graph.algo.svdpp

import com.github.fommil.netlib.BLAS.{getInstance => blas}
import com.tencent.angel.graph.VertexId
import com.tencent.angel.graph.framework._
import com.tencent.angel.graph.utils.FastHashMap
import org.apache.spark.rdd.RDD

import scala.util.Random

object SVDPlusPlus {

  def apply(g: Graph[SVDPPVD, Double], conf: Conf): Graph[SVDPPVD, Double] = {

    require(conf.maxIters > 0, s"Maximum of iterations must be greater than 0," +
      s" but got ${conf.maxIters}")
    require(conf.maxVal > conf.minVal, s"MaxVal must be greater than MinVal," +
      s" but got {maxVal: ${conf.maxVal}, minVal: ${conf.minVal}}")

    // Generate default vertex attribute
    def vprog(vd: SVDPPVD, rank: Int, v3: Double, v4: Double): SVDPPVD = {
      // TODO: use a fixed random seed
      val v1 = Array.fill(rank)(Random.nextDouble())
      val v2 = Array.fill(rank)(Random.nextDouble())
      SVDPPVD(v1, v2, v3, v4)
    }

    // calculate global rating mean
    val u = g.edges.flatMap { edgePartition => edgePartition.iterator.map(_.attr) }.mean()

    //Calculate initial bias and norm
    val degreeSlotName = "degree"
    val valueSlotName = "values"
    g.calDegree(degreeSlotName)
    g.calValues(valueSlotName)

    //init default vertex attribute
    var graph = g.updateVertexAttr(degreeSlotName, valueSlotName, vprog, conf.rank, u)

    def trainF(conf: Conf, u: Double): RDD[(VertexId, (Array[Double], Array[Double], Double))] = {

      graph.edges.mapPartitions { iter =>
        val edgePartition = iter.next()

        val update = new FastHashMap[VertexId, (Array[Double], Array[Double], Double)]()

        def mergeF(id: VertexId, values: (Array[Double], Array[Double], Double)): Unit = {
          update.putMerge(id, values,
            (g1, g2) => {
              val out1 = g1._1.clone()
              blas.daxpy(out1.length, 1.0, g2._1, 1, out1, 1)
              val out2 = g2._2.clone()
              blas.daxpy(out2.length, 1.0, g2._2, 1, out2, 1)
              (out1, out2, g1._3 + g2._3)
            })
        }

        edgePartition.iterator.foreach { edge =>
          val (usr, itm) = (edgePartition.vertexAttrFromId(edge.srcId), edgePartition.vertexAttrFromId(edge.dstId))

          val (p, q) = (usr.v1, itm.v1)
          val rank = p.length
          var pred = u + usr.v3 + itm.v3 + blas.ddot(rank, q, 1, usr.v2, 1)
          pred = math.max(pred, conf.minVal)
          pred = math.min(pred, conf.maxVal)
          val err = edge.attr - pred
          // updateP = (err * q - conf.gamma7 * p) * conf.gamma2
          val updateP = q.clone()
          blas.dscal(rank, err * conf.gamma2, updateP, 1)
          blas.daxpy(rank, -conf.gamma7 * conf.gamma2, p, 1, updateP, 1)
          // updateQ = (err * usr._2 - conf.gamma7 * q) * conf.gamma2
          val updateQ = usr.v2.clone()
          blas.dscal(rank, err * conf.gamma2, updateQ, 1)
          blas.daxpy(rank, -conf.gamma7 * conf.gamma2, q, 1, updateQ, 1)
          // updateY = (err * usr._4 * q - conf.gamma7 * itm._2) * conf.gamma2
          val updateY = q.clone()
          blas.dscal(rank, err * usr.v4 * conf.gamma2, updateY, 1)
          blas.daxpy(rank, -conf.gamma7 * conf.gamma2, itm.v2, 1, updateY, 1)

          mergeF(edge.srcId, (updateP, updateY, (err - conf.gamma6 * usr.v3) * conf.gamma1))
          mergeF(edge.dstId, (updateQ, updateY, (err - conf.gamma6 * itm.v3) * conf.gamma1))

        }
        update.iterator
      }
    }

    // calculate error on training set
    def testF(conf: Conf, u: Double): RDD[(VertexId, Double)] = {

      graph.edges.mapPartitions { iter =>
        val edgePartition = iter.next()
        val errs = new FastHashMap[VertexId, Double]()

        def mergeF(id: VertexId, v: Double): Unit = {
          errs.putMerge(id, v, (g1, g2) => g1 + g2)
        }

        edgePartition.iterator.foreach { edge =>
          val (usr, itm) = (edgePartition.vertexAttrFromId(edge.srcId), edgePartition.vertexAttrFromId(edge.dstId))

          val (p, q) = (usr.v1, itm.v1)
          val rank = p.length
          var pred = u + usr.v3 + itm.v3 + blas.ddot(rank, q, 1, usr.v2, 1)
          pred = math.max(pred, conf.minVal)
          pred = math.min(pred, conf.maxVal)
          val err = (edge.attr - pred) * (edge.attr - pred)
          mergeF(edge.dstId, err)
        }
        errs.iterator
      }.reduceByKey(_ + _).repartition(graph.edges.getNumPartitions)
    }

    for (i <- 0 until conf.maxIters) {
      // Phase 1, calculate pu + |N(u)|^(-0.5)*sum(y) for user nodes
      val sumY = graph.edges.mapPartitions { iter =>
        val edgePartition = iter.next()
        edgePartition.updateVertexAttrs()

        val attrs = new FastHashMap[VertexId, Array[Double]]()
        edgePartition.iterator.foreach { edge =>
          val (src, dst) = (edge.srcId, edge.dstId)
          if (attrs.containsKey(src)) {
            val out = attrs.get(src).clone()
            blas.daxpy(out.length, 1.0, edgePartition.vertexAttrFromId(dst).v2, 1, out, 1)
            attrs(src) = out
          } else {
            attrs(src) = edgePartition.vertexAttrFromId(dst).v2
          }
        }

        // update local vertex attr, avoid to pull again in trainF
        attrs.foreach { case (vid, v2) =>
          val vd = edgePartition.vertexAttrFromId(vid)
          val out = vd.v1.clone()
          blas.daxpy(out.length, vd.v4, v2, 1, out, 1)
          edgePartition.setVertexAttrFromId(vid, SVDPPVD(vd.v1, out, vd.v3, vd.v4))
        }
        attrs.iterator
      }

      // update ps vertex attr
      graph = graph.outerJoinVertices(sumY) {
        (vd: SVDPPVD,
         msg: Array[Double]) =>
          val out = vd.v1.clone()
          blas.daxpy(out.length, vd.v4, msg, 1, out, 1)
          SVDPPVD(vd.v1, out, vd.v3, vd.v4)
      }
      sumY.unpersist()

      // Phase 2, update p for user nodes and q, y for item nodes
      val update = trainF(conf, u).reduceByKey {
        case (g1, g2) => {
          val out1 = g1._1.clone()
          blas.daxpy(out1.length, 1.0, g2._1, 1, out1, 1)
          (out1, g2._2, g1._3 + g2._3)
        }
      }

      def mergeFunc(vd: SVDPPVD,
                    msg: (Array[Double], Array[Double], Double)): SVDPPVD = {
        val out1 = vd.v1.clone()
        blas.daxpy(out1.length, 1.0, msg._1, 1, out1, 1)
        val out2 = vd.v2.clone()
        blas.daxpy(out2.length, 1.0, msg._2, 1, out2, 1)
        SVDPPVD(out1, out2, vd.v3 + msg._3, vd.v4)
      }

      graph = graph.outerJoinVertices[(Array[Double], Array[Double], Double)](update)(mergeFunc)
      update.unpersist()
    }


    val test = testF(conf, u)
    graph = graph.outerJoinVertices[Double](test) {
      (vd: SVDPPVD, msg: Double) =>
        SVDPPVD(vd.v1, vd.v2, vd.v3, msg)
    }
    test.unpersist()

    graph
  }

  private def materialize(g: Graph[_, _]): Unit = {
    g.numVertices
    g.numEdges

  }

  /** Configuration parameters for SVDPlusPlus. */
  class Conf(
              var rank: Int,
              var maxIters: Int,
              var minVal: Double,
              var maxVal: Double,
              var gamma1: Double,
              var gamma2: Double,
              var gamma6: Double,
              var gamma7: Double)
    extends Serializable

}
