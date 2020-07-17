package com.imooc.helloWorld
import org.apache.spark.sql.{SaveMode, SparkSession}

object hdfs2hive {
  def main(args: Array[String]): Unit = {
    println("############ main 1 ##############")
    // 初始化 spark 相关环境
    val spark: SparkSession = SparkSession.builder()
      .config("spark.default.parallelism", "600")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.rdd.compress", "true")
      .config("spark.sql.crossJoin.enabled", "true")
      .config("spark.speculation.interval", "10000ms")
      .config("spark.sql.tungsten.enabled", "true")
      .config("spark.sql.shuffle.partitions", "600")
      .config("hive.metastore.uris", "thrift://xl.namenode2.coocaa.com:9083")
      .config("spark.sql.warehouse.dir", "hdfs://coocaadata/apps/hive/warehouse")
      .appName(s"${this.getClass.getSimpleName} guanyue".filter(!_.equals('$')))
      .enableHiveSupport()
      .getOrCreate()

    val df = spark.read.csv("/user/bigdata/embedding/eval/jiayuepeng/test/shop_rec_csv/rec_shop.csv")

    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    df.write.mode(SaveMode.Overwrite).saveAsTable("test.product_sim")

    spark.stop()
    println("##########################")
    println("write over")
  }
}
