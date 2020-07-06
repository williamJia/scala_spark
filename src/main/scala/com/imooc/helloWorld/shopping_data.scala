package com.imooc.helloWorld
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
//import org.apache.spark.ml.{Pipeline, PipelineStage}
//import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
//import scala.collection.mutable.ListBuffer

object shopping_data {
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

    val df = spark.sql(
      """
        |select goods_id as goods_id,
        |    -- onehot --
        |    category_id as category_id,store_id as store_id,goods_classify,book_amount as book_amount,
        |    -- number --
        |    goods_weight as goods_weight,market_price as market_price,shop_price as shop_price,integral as integral,sell_number as sell_number,
        |    -- null to -1 --
        |    is_real as is_real,is_alone_sale as is_alone_sale,is_shipping as is_shipping,is_delete as is_delete,is_best as is_best,is_new as is_new,is_hot as is_hot,sell_top as sell_top,is_promote as is_promote,start_sale as start_sale,is_wap as is_wap,isshow as isshow,is_real_subscribe as is_real_subscribe
        |        |  FROM coocaa_rds.rds_goods_dim where dt="2020-07-02"
        |    limit 100
      """.stripMargin)

//    val df_fill_null = df.na.fill(-1)
//
//    /** onehot 编码处理*/
//    val categoricalColumns = Array("category_id", "store_id", "goods_classify", "book_amount") // 要进行OneHotEncoder编码的字段
//    val stagesArray = new ListBuffer[PipelineStage]() // 采用Pileline方式处理机器学习流程
//    for (cate <- categoricalColumns) {
//      val indexer = new StringIndexer().setInputCol(cate).setOutputCol(s"${cate}Index") // 使用StringIndexer 建立类别索引
//      val encoder = new OneHotEncoder().setInputCol(indexer.getOutputCol).setOutputCol(s"${cate}classVec") // 使用OneHotEncoder将分类变量转换为二进制稀疏向量
//      stagesArray.append(indexer,encoder)
//    }
//
//    /** 合并所有特征为单个向量 */
//    val numericCols = Array("goods_weight", "market_price", "shop_price","integral", "sell_number")
//    val boolenCols = Array("is_real", "is_alone_sale", "is_shipping","is_delete", "is_best", "is_new","is_hot","sell_top","is_promote","start_sale","is_wap","isshow","is_real_subscribe")
//    val assemblerInputs = categoricalColumns.map(_ + "classVec") ++ numericCols ++ boolenCols
//    // 使用VectorAssembler将所有特征转换为一个向量
//    val assembler = new VectorAssembler().setInputCols(assemblerInputs).setOutputCol("features")
//    stagesArray.append(assembler)
//
//    // 以Pipeline的形式运行各个PipelineStage，训练并生成新的处理后的数据
//    val pipeline = new Pipeline()
//    pipeline.setStages(stagesArray.toArray)
//    // fit() 根据需要计算特征统计信息
//    val pipelineModel = pipeline.fit(df_fill_null)
//    // transform() 真实转换特征
//    val dataset = pipelineModel.transform(df_fill_null)
//    dataset.show(false)
//    println(dataset.count())

    spark.stop()

  }
}
