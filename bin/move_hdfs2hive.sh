#!/bin/bash
base_dir=$(cd `dirname ../../`; pwd)
echo "work directory:"${base_dir}
# Process data
run_cmd="--master yarn-client \
--driver-memory 4g \
--executor-memory 4g \
--num-executors 4 \
--executor-cores 4 \
--class com.imooc.helloWorld.hdfs2hive ${base_dir}/scala_project-1.0-SNAPSHOT.jar"
echo "over"

main(){
 echo ${run_cmd}
/usr/hdp/current/spark2-client/bin/spark-submit ${run_cmd}
 # eval nohup /usr/hdp/current/spark2-client/bin/spark-submit ${run_cmd} > /dev/null 2>&1 &
 }
main $@