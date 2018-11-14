package com.atguigu.product

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object printTable extends App{
  val sparkConf = new SparkConf().setAppName("AreaTop3Product").setMaster("local[*]")
  // 创建Spark客户端
  val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
  //val sc = spark.sparkContext
  spark.sql("use spark")

  spark.sql("select * from user_visit_action").show
  spark.sql("select * from user_info").show
  spark.sql("select * from product_info").show

}
