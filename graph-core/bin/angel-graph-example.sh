#!/usr/bin/env bash

source ./angel-graph-env.sh

$SPARK_HOME/bin/spark-submit \
    --master yarn-cluster \
	--conf spark.ps.jars=${ANGEL_GRAPH_ANGEL_JARS} \
	--conf spark.ps.instances=2 \
	--conf spark.ps.cores=2 \
	--conf spark.ps.memory=6g \
	--jars ${ANGEL_GRAPH_SPARK_JARS}\
	--name "pagerank on angel-graph" \
	--driver-memory 10g \
	--num-executors 2 \
	--executor-cores 2 \
	--executor-memory 6g \
	--class com.tencent.angel.graph.examples.cluster.PageRankClusterExample \
	./../lib/graph-examples-${ANGEL_GRAPH_VERSION}.jar \
	input:<input_path> \
	maxIter:<max iter> \
	batchSize:<batch size> \
	resetProb:<resetProb>