#!/bin/bash

current_dir=`pwd`

cd
. ~/run.sh
run_hdfs=`jps | grep -i namenode`
if [ ${#run_hdfs} == 0 ]; then
    start_hdfs
else
    echo 'HDFS daemons running.'
fi    

run_spark=`jps | grep -i master`
if [ ${#run_spark} == 0 ]; then
    start_spark
else
    echo 'Spark daemons running.'
fi

cd ~/GraphX/graphx/
sbt package

cd
cache_clean

~/software/spark-2.0.0-bin-hadoop2.6/bin/spark-submit ~/GraphX/graphx/target/scala-2.11/graphx-pagerank_2.11-1.0.jar 'hdfs:///user/ubuntu/dataset.txt' 20

trap SIGHUP SIGKILL SIGTERM SIGINT

cd $current_dir
