package com.imooc.helloWorld
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}

object hello {
  def main(args: Array[String]): Unit = {
    // 初始化 spark 相关环境
    val conf = new SparkConf()
    conf.setAppName("getUserData")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.rdd.compress", "true")
    conf.set("spark.speculation.interval", "10000ms")
    conf.set("spark.sql.tungsten.enabled", "true")

    conf.set("spark.network.timeout", "10000000")
    conf.set("spark.sql.shuffle.partitions", "720")
    conf.set("spark.Kryoserializer.buffer.max", "1024m")
    conf.set("spark.sql.warehouse.dir", "hdfs://coocaadata/apps/hive/warehouse")
    conf.set("spark.sql.broadcastTimeout","5400")
    conf.set("spark.default.parallelism", "720")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    // 加载数据
    val embDF = spark.read.parquet("/user/bigdata/embedding/eval/jiayuepeng/se_vid_32dim_o/day=2020-06-29/se_embedding_part.parquet")
    val df = embDF.drop("vid")

    // 格式转换，统一输出集中到一列 vector 类型
    val arr = Array("0","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31")
    val assembler = new VectorAssembler().setInputCols(arr).setOutputCol("features")
    val ssembled_df = assembler.transform(df)

    // 模型初始化 & 训练
    val kmeans = new KMeans().setK(8).setMaxIter(500).setSeed(1L).setFeaturesCol("features")
    val model = kmeans.fit(ssembled_df)

    // 预测相关数据，输出类别，对应获取模型评分
    val predictions = model.transform(ssembled_df)
    val cost = model.computeCost(ssembled_df)

    // 模型持久化 & 加载
    model.save("/user/bigdata/spark_data/model/kmeans/model")
    val sameModel = KMeansModel.load("/user/bigdata/spark_data/model/kmeans/model") // todo 加载不成功，待完成

    // 数据持久化
    ssembled_df.write.parquet("/user/bigdata/spark_data/model/kmeans/data/ret.parquet")

    spark.stop()

  }
}

// export HDP_VERSION=2.7.3.2.5.0.0-1245
/// /usr/hdp/current/spark2-client/bin/spark-shell