package com.tencent.angel.graph.framework


import com.tencent.angel.graph.VertexId
import com.tencent.angel.spark.models.PSMatrix
import org.apache.spark.{Dependency, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class MirrorRDD[VD: ClassTag](sparkContext: SparkContext, psMatrix: PSMatrix)
  extends RDD[(VertexId, VD)](sparkContext, deps=Seq[Dependency[_]]()){

  override def compute(split: Partition, context: TaskContext): Iterator[(VertexId, VD)] = ???

  override protected def getPartitions: Array[Partition] = ???
}
