package com.imooc.helloWorld
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DoubleType, StringType}

// export HDP_VERSION=2.7.3.2.5.0.0-1245
// /usr/hdp/current/spark2-client/bin/spark-shell

object changePaquetType {
  def main(args: Array[String]): Unit = {

    // 初始化 spark 相关环境
    val conf = new SparkConf()
    conf.setAppName("getUserData")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.rdd.compress", "true")
    conf.set("spark.speculation.interval", "10000ms")
    conf.set("spark.sql.tungsten.enabled", "true")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    // 加载数据
    val embDF = spark.read.parquet("/user/bigdata/embedding/eval/jiayuepeng/se_vid_32dim_o/day=2020-06-29/se_embedding_part.parquet")
    // 合并不同列的数据为一列，同时筛选出所需要dd的列的数据
    val concat_embDF = embDF.select(
      concat(
        col("0"),lit(","),
        col("1"),lit(","),
        col("2"),lit(","),
        col("3"),lit(","),
        col("4"),lit(","),
        col("5"),lit(","),
        col("6"),lit(","),
        col("7"),lit(","),
        col("8"),lit(","),
        col("9"),lit(","),
        col("10"),lit(","),
        col("11"),lit(","),
        col("12"),lit(","),
        col("13"),lit(","),
        col("14"),lit(","),
        col("15"),lit(","),
        col("16"),lit(","),
        col("17"),lit(","),
        col("18"),lit(","),
        col("19"),lit(","),
        col("20"),lit(","),
        col("21"),lit(","),
        col("22"),lit(","),
        col("23"),lit(","),
        col("24"),lit(","),
        col("25"),lit(","),
        col("26"),lit(","),
        col("27"),lit(","),
        col("28"),lit(","),
        col("29"),lit(","),
        col("30"),lit(","),
        col("31")
      ).as("concat_emb"),col("vid"),col("category")
    )
    // 处理旧的列的数据，生成新的一列数据 & 旧数据列的删除
    val emb_df = concat_embDF.withColumn("embedding",split(col("concat_emb"),",").cast(ArrayType(DoubleType)))

    val ret = emb_df.drop("concat_emb")
    ret.printSchema()

    ret.write.parquet("/user/bigdata/embedding/eval/jiayuepeng/se_vid_32dim/day=2020-06-22/")

  }
}
