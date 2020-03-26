#!/bin/bash

# Before run Angel Graph application, you must follow the steps:
# 1. confirm Hadoop and Spark have ready in your environment
# 2. unzip angel-graph-<version>-bin.zip to local directory
# 3. upload angel-graph-<version>-bin directory to HDFS
# 4. set the following variables, SPARK_HOME, ANGEL_GRAPH_HOME, ANGEL_GRAPH_HDFS_HOME, ANGEL_GRAPH_VERSION, ANGEL_VERSION


export JAVA_HOME=
export HADOOP_HOME=
export SPARK_HOME=
export ANGEL_GRAPH_HOME=
export ANGEL_GRAPH_HDFS_HOME=
export ANGEL_GRAPH_VERSION=1.0-SNAPSHOT
export ANGEL_VERSION=2.4.0

scala_jar=scala-library-2.11.8.jar
angel_ps_external_jar=fastutil-8.2.2.jar,htrace-core-2.05.jar,sizeof-0.3.0.jar,kryo-shaded-4.0.0.jar,minlog-1.3.0.jar,memory-0.8.1.jar,commons-pool-1.6.jar,netty-all-4.1.39.Final.jar,hll-1.6.0.jar
angel_ps_jar=angel-ps-core-${ANGEL_VERSION}.jar,angel-ps-mllib-${ANGEL_VERSION}.jar,angel-ps-psf-${ANGEL_VERSION}.jar

angel_graph_jar=graph-core-${ANGEL_GRAPH_VERSION}.jar,graph-algo-${ANGEL_GRAPH_VERSION}.jar
angel_graph_external_jar=fastutil-8.2.2.jar,htrace-core-2.05.jar,sizeof-0.3.0.jar,kryo-shaded-4.0.0.jar,minlog-1.3.0.jar,memory-0.8.1.jar,commons-pool-1.6.jar,netty-all-4.1.39.Final.jar,hll-1.6.0.jar,jniloader-1.1.jar,native_system-java-1.1.jar,arpack_combined_all-0.1.jar,core-1.1.2.jar,netlib-native_ref-linux-armhf-1.1-natives.jar,netlib-native_ref-linux-i686-1.1-natives.jar,netlib-native_ref-linux-x86_64-1.1-natives.jar,netlib-native_system-linux-armhf-1.1-natives.jar,netlib-native_system-linux-i686-1.1-natives.jar,netlib-native_system-linux-x86_64-1.1-natives.jar,jettison-1.4.0.jar,json4s-native_2.11-3.5.3.jar

dist_jar=${angel_ps_external_jar},${angel_ps_jar},${scala_jar},${angel_graph_jar}
local_jar=${angel_graph_external_jar},${angel_ps_jar},${angel_graph_jar}

unset ANGEL_GRAPH_ANGEL_JARS
unset ANGEL_GRAPH_SPARK_JARS

for f in `echo ${dist_jar} | awk -F, '{for(i=1; i<=NF; i++){ print $i}}'`; do
	jar=${ANGEL_GRAPH_HDFS_HOME}/lib/${f}
    if [[ "$ANGEL_GRAPH_ANGEL_JARS" ]]; then
        ANGEL_GRAPH_ANGEL_JARS=${ANGEL_GRAPH_ANGEL_JARS},$jar
    else
        ANGEL_GRAPH_ANGEL_JARS=${jar}
    fi
done
echo ANGEL_GRAPH_ANGEL_JARS: ${ANGEL_GRAPH_ANGEL_JARS}
export ANGEL_GRAPH_ANGEL_JARS


for f in `echo ${local_jar} | awk -F, '{for(i=1; i<=NF; i++){ print $i}}'`; do
	jar=${ANGEL_GRAPH_HDFS_HOME}/lib/${f}
    if [[ "$ANGEL_GRAPH_SPARK_JARS" ]]; then
        ANGEL_GRAPH_SPARK_JARS=${ANGEL_GRAPH_SPARK_JARS},$jar
    else
        ANGEL_GRAPH_SPARK_JARS=${jar}
    fi
done
echo ANGEL_GRAPH_SPARK_JARS: ${ANGEL_GRAPH_SPARK_JARS}
export ANGEL_GRAPH_SPARK_JARS