#!/bin/bash
base_dir=$(cd `dirname ../../`; pwd)
echo "work directory:"${base_dir}
# Process data
run_cmd="--master yarn-client \
--driver-memory 8g \
--executor-memory 16g \
--num-executors 8 \
--executor-cores 8 \
--class com.imooc.helloWorld.shopping_sim ${base_dir}/scala_project-1.0-SNAPSHOT.jar"
#echo ${run_cmd}
#/usr/hdp/current/spark2-client/bin/spark-submit ${run_cmd}
echo "over"


main(){
 echo ${run_cmd}
/usr/hdp/current/spark2-client/bin/spark-submit ${run_cmd}
 # eval nohup /usr/hdp/current/spark2-client/bin/spark-submit ${run_cmd} > /dev/null 2>&1 &
 }
main $@