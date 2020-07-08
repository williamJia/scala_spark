package com.imooc.helloWorld

import java.text.SimpleDateFormat
import java.util.Calendar

import com.imooc.helloWorld.shopping_data.get_yesterday
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object shopping_sim {

  def get_yesterday(): String ={
    val ymdFormat=new SimpleDateFormat("yyyy-MM-dd")
    val day:Calendar = Calendar.getInstance()
    day.add(Calendar.DATE,-1)
    val logDate = ymdFormat.format(day.getTime)
    logDate
  }

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

    val yesterday = get_yesterday()
    val df = spark.read.parquet(s"/user/bigdata/embedding/eval/jiayuepeng/parquet/shopping/${yesterday}")



  }
}
