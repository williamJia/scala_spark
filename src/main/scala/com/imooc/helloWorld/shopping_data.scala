package com.imooc.helloWorld
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._

object shopping_data {

  def get_yesterday(): String ={
    val ymdFormat=new SimpleDateFormat("yyyy-MM-dd")
    val day:Calendar = Calendar.getInstance()
    day.add(Calendar.DATE,-1)
    val logDate = ymdFormat.format(day.getTime)
    logDate
  }

  def main(args: Array[String]): Unit = {
    println("im in scala")
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
    spark.sql("set spark.sql.adaptive.enabled=true")
    val df = spark.sql(
      s"""
        |select goods_id as goods_id,dt as dt,
        |    -- onehot --
        |    category_id as category_id,store_id as store_id,goods_classify,book_amount as book_amount,
        |    -- number --
        |    goods_weight as goods_weight,market_price as market_price,shop_price as shop_price,integral as integral,sell_number as sell_number,
        |    -- null to -1 --
        |    is_real as is_real,is_alone_sale as is_alone_sale,is_shipping as is_shipping,is_delete as is_delete,is_best as is_best,is_new as is_new,is_hot as is_hot,sell_top as sell_top,is_promote as is_promote,start_sale as start_sale,is_wap as is_wap,isshow as isshow,is_real_subscribe as is_real_subscribe
        |       FROM coocaa_rds.rds_goods_dim where dt="${yesterday}"
      """.stripMargin)

    println(" ########### df ############### ")
    df.show(5)
    val df_fill_null = df.na.fill(-1)

    println(" ########### df_null ############### ")
    df.show(5)
    // 合并不同列的数据为一列，同时筛选出所需要的列的数据
    val ret = df_fill_null.select(
      col("goods_id"),concat(
        col("goods_weight"),lit(","),
        col("market_price"),lit(","),
        col("shop_price"),lit(","),
        col("integral"),lit(","),
        col("sell_number"),lit(","),
        col("is_real"),lit(","),
        col("is_alone_sale"),lit(","),
        col("is_shipping"),lit(","),
        col("is_delete"),lit(","),
        col("is_best"),lit(","),
        col("is_new"),lit(","),
        col("is_hot"),lit(","),
        col("sell_top"),lit(","),
        col("is_promote"),lit(","),
        col("start_sale"),lit(","),
        col("is_wap"),lit(","),
        col("isshow"),lit(","),
        col("is_real_subscribe")
      ).as("features"),col("dt")
    )
    // 处理旧的列的数据，生成新的一列数据 & 旧数据列的删除
//    val emb_df = concatDF.withColumn("features",split(col("concat_emb"),","))
//    val ret = emb_df.drop("concat_emb")
    ret.printSchema()
    println(" ########### ret ############### ")
    ret.show(5)
    ret.write.csv(s"/user/bigdata/embedding/eval/jiayuepeng/shopping/${yesterday}")

    spark.stop()

  }
}
