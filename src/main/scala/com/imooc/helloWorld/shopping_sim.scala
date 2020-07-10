package com.imooc.helloWorld

import java.text.SimpleDateFormat
import java.util.Calendar

import breeze.linalg.DenseVector
import breeze.numerics.{pow, sqrt}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, DoubleType}
import org.apache.spark.sql.expressions.Window

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object shopping_sim {

  def get_yesterday(): String ={
    val ymdFormat=new SimpleDateFormat("yyyy-MM-dd")
    val day:Calendar = Calendar.getInstance()
    day.add(Calendar.DATE,-1)
    val logDate = ymdFormat.format(day.getTime)
    logDate
  }

  def cos(p1: Array[Double], p2: Array[Double]) = {

    val v1 = new DenseVector(p1)
    val v2 = new DenseVector(p2)

    val a = sqrt(p1.map(pow(_, 2)).sum)
    val b = sqrt(p2.map(pow(_, 2)).sum)

    val ab =  v1.t * v2
    ab / (a * b)
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
    import spark.implicits._

    val yesterday = get_yesterday()
    val df = spark.read.parquet(s"/user/bigdata/embedding/eval/jiayuepeng/test/shopping/${yesterday}02")

    // 格式转换，统一输出集中到一列 vector 类型
//    val arr = Array("goods_weight","market_price","shop_price","integral","sell_number","is_real","is_alone_sale","is_shipping","is_delete","is_best","is_new","is_hot","sell_top","is_promote","start_sale","is_wap","isshow","is_real_subscribe")
//    val assembler = new VectorAssembler().setInputCols(arr).setOutputCol("features")
//    val ssembled_df = assembler.transform(df)

    // 数据整合到一列，遍历每条数据的此列，两两计算相似度
    val concatDF = df.select(
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
      ).as("concat_emb"),col("dt")
    )
    // 处理旧的列的数据，生成新的一列数据 & 旧数据列的删除
    val emb_df = concatDF.withColumn("features",split(col("concat_emb"),",").cast(ArrayType(DoubleType)))
    val arr_df = emb_df.drop("concat_emb")// .limit(10)

    val arr_df_copy = arr_df.as("arr_df_copy")
    val array = arr_df.collect
    val array_c = arr_df_copy.collect

    // 计算相似度
    var ret_arr = ArrayBuffer[List[String]]()
    for(i <- 0 to array.length-1){
      val p1:Array[Double] = array(i)(2).asInstanceOf[mutable.WrappedArray[Double]].toArray
      for(j <- 0 to array_c.length-1){
        var row_arr = ArrayBuffer[String]()
        val p2:Array[Double] = array_c(j)(2).asInstanceOf[mutable.WrappedArray[Double]].toArray
        val value = cos(p1,p2)
        row_arr += array(i)(0).toString
        row_arr += array_c(j)(0).toString
        row_arr += value.toString
        ret_arr += row_arr.toList
      }
    }

    // 格式化，分列，分组，排序，取TopN
    val ret_df = ret_arr.toSeq.asInstanceOf[Seq[Seq[String]]]toDF()
    val ret_col_df = ret_df.select( $"value" +: (0 until 3).map(i => $"value"(i).alias(s"col$i")): _* ).drop("value")
    val w = Window.partitionBy($"col0").orderBy($"col2".desc)
    val dfTop3 = ret_col_df.withColumn("rn", row_number().over(w)).where($"rn" <= 3).drop("rn")
    // 合并相同id的数据，逗号间隔
    dfTop3.createOrReplaceTempView("test")
    val df_concat = spark.sql("select col0 as id,concat_ws(',',collect_set(col1)) as other_ids from test group by col0")
    println("#############")
    df_concat.show(10)
    df_concat.printSchema()
    println(df_concat.count())
    df_concat.write.parquet(s"/user/bigdata/embedding/eval/jiayuepeng/test/shop_sim/recal/${yesterday}")
  }
}
