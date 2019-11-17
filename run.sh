#!/bin/bash

if [[ $# -ge 2 ]]; then

  sbt clean && sbt package

  ${HADOOP_HOME}/bin/hdfs dfs -rm -r -f $1 /spark_log
  
  ${HADOOP_HOME}/bin/hdfs dfs -mkdir /spark_log
  
  cp target/scala-2.11/*.jar wikirank.jar 

  $SPARK_HOME/bin/spark-submit  \
    --class Main \
    --master spark://dover:34567 \
    --executor-cores 2 \
    --num-executors 4 \
    --executor-memory 2g \
    --driver-memory 3g \
    --supervise wikirank.jar $1 $2

else 
  echo "Usage: ./run.sh <hdfs_output_path> --profile=[0|1|2]"
fi
