package com.atguigu.product

import java.util.UUID

import com.atguigu.commons.conf.ConfigurationManager
import com.atguigu.commons.constant.Constants
import com.atguigu.commons.utils.ParamUtils
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

object AreaTop3ProductApp {

  def main(args: Array[String]): Unit = {

    // 获取统计任务参数【为了方便，直接从配置文件中获取，企业中会从一个调度平台获取】
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParam = JSONObject.fromObject(jsonStr)

    // 任务的执行ID，用户唯一标示运行后的结果，用在MySQL数据库中
    val taskUUID = UUID.randomUUID().toString

    // 构建Spark上下文
    val sparkConf = new SparkConf().setAppName("AreaTop3Product").setMaster("local[*]")
    // 创建Spark客户端
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    //val sc = spark.sparkContext
    spark.sql("use spark")

    // 查询用户指定日期范围内的点击行为数据（city_id，在哪个城市发生的点击行为）
    val cityId2clickActionRDD = getCityid2ClickActionRDDByDate(spark, taskParam)
    // (2,[2,8]) (cityId, (cityId,productId))

    // 查询城市信息
    val cityId2cityInfoRDD = getCityid2CityInfoRDD(spark)
    //  (2,[2,南京,华东]) (city_id , 城市信息)

    // 生成点击商品基础信息临时表
    // 将点击行为cityId2clickActionRDD和城市信息cityId2cityInfoRDD进行Join关联
    generateTempClickProductBasicTable(spark, cityId2clickActionRDD, cityId2cityInfoRDD)
    // tmp_click_product_basic
    //  "city_id", "city_name", "area", "product_id"
    //  (2,南京,华东,8)

    // UDF：concat_long_string()，将两个字段拼接起来，用指定的分隔符 :
    spark.udf.register("concat_long_string",
      (v1: Long, v2: String, split: String) => v1.toString + split + v2)
    // UD AF：group_concat_distinct()，将一个分组中的多个字段值，用逗号拼接起来，同时进行去重
    spark.udf.register("group_concat_distinct", new GroupConcatDistinctUDAF())
    // 生成各区域各商品点击次数的临时表
    // 对tmp_click_product_basic表中的数据进行count聚合统计，得到点击次数
    generateTempAreaPrdocutClickCountTable(spark)
    // tmp_area_product_click_count
    //"AREA", "product_id", "click_count", "city_infos"
    //(华中,40,13,(5:武汉,6:长沙))

    spark.udf.register("get_json_object", (json: String, field: String) => {
      val jsonObject = JSONObject.fromObject(json)
      jsonObject.getString(field)
    })
    // 生成包含完整商品信息的各区域各商品点击次数的临时表
    // 关联tmp_area_product_click_count表与product_info表，在tmp_area_product_click_count基础上引入商品的详细信息
    //  UDF get_json_object
    //  tmp_area_fullprod_click_count
    generateTempAreaFullProductClickCountTable(spark)
    /*
    +----+----------+-----------+----------+------------+--------------+
    |area|product_id|click_count|city_infos|product_name|product_status|
    +----+----------+-----------+----------+------------+--------------+
    |  西北|      43|         11|     7:西安|   product43|          Self|
    |  西南|      70|         11|     8:成都|   product70|   Third Party|   */

    // 需求一：使用开窗函数获取各个区域内点击次数排名前3的热门商品
    val areaTop3ProductRDD: DataFrame = getAreaTop3(taskUUID, spark)
    /*val areaTop3ProductRDD = getAreaTop3ProductRDD(taskUUID, spark)

    // 将数据转换为DF，并保存到MySQL数据库
    import spark.implicits._
    val areaTop3ProductDF = areaTop3ProductRDD.rdd.map(row =>
      AreaTop3Product(
      taskUUID,
      row.getAs[String]("area"),
      row.getAs[String]("area_level"),
      row.getAs[Long]("product_id"),
      row.getAs[String]("city_infos"),
      row.getAs[Long]("click_count"),
      row.getAs[String]("product_name"),
      row.getAs[String]("product_status"))
    ).toDS

    areaTop3ProductDF.write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "area_top3_product")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()*/

    spark.close()
  }

  def getAreaTop3(taskUUID: String, spark: SparkSession) = {
    /*各个区域内 点击次数排名前3 的热门商品
+----+----------+-----------+----------+------------+--------------+
|area|product_id|click_count|city_infos|product_name|product_status|
+----+----------+-----------+----------+------------+--------------+
|  西北|      43|         11|     7:西安|   product43|          Self|
|  西南|      70|         11|     8:成都|   product70|   Third Party|   */
    //  tmp_area_fullprod_click_count
    val sql=
      s"""
         |SELECT
         |    AREA,
         |    CASE
         |        WHEN AREA= '华北'
         |        OR AREA= '华东'
         |        THEN 'A Level'
         |        WHEN AREA= '华南'
         |        OR AREA= '华中'
         |        THEN 'B Level'
         |        WHEN AREA= '西北'
         |        OR AREA= '西南'
         |        THEN 'C Level'
         |        ELSE 'D Level'
         |    END area_level, product_id, city_infos, click_count, product_name, product_status,rank
         |FROM
         |    (SELECT
         |        *, row_number () over (
         |            PARTITION BY AREA
         |	          ORDER BY click_count DESC
         |	          ) rank
         |    FROM
         |        tmp_area_fullprod_click_count) t
         |WHERE rank <= 3
         |distribute by AREA
         |sort by click_count DESC
       """.stripMargin
    spark.sql(sql)
/*+----+----------+----------+----------+-----------+------------+--------------+----+
|AREA|area_level|product_id|city_infos|click_count|product_name|product_status|rank|
+----+----------+----------+----------+-----------+------------+--------------+----+
|  华东|   A Level|        76| 1:上海,2:南京|         41|   product76|          Self|   1|
|  华东|   A Level|        49| 1:上海,2:南京|         40|   product49|          Self|   2|
|  华东|   A Level|        81| 1:上海,2:南京|         39|   product81|   Third Party|   3|
|  西北|   C Level|        96|      7:西安|         26|   product96|          Self|   1|
|  西北|   C Level|        66|      7:西安|         24|   product66|   Third Party|   2|
|  西北|   C Level|         8|      7:西安|         22|    product8|   Third Party|   3|
|  华南|   B Level|        54| 3:广州,4:三亚|         39|   product54|          Self|   1|
|  华南|   B Level|        63| 3:广州,4:三亚|         39|   product63|   Third Party|   2|
|  华南|   B Level|        47| 3:广州,4:三亚|         38|   product47|   Third Party|   3|
|  华北|   A Level|        18|      0:北京|         25|   product18|          Self|   1|
|  华北|   A Level|        82|      0:北京|         22|   product82|   Third Party|   2|*/
  }

  /**
    * 需求一：获取各区域top3热门商品
    * 使用开窗函数先进行一个子查询,按照area进行分组，给每个分组内的数据，按照点击次数降序排序，打上一个组内的行号
    * 接着在外层查询中，过滤出各个组内的行号排名前3的数据
    *
    * @return
    */
  def getAreaTop3ProductRDD(taskid: String, spark: SparkSession): DataFrame = {
    // 华北、华东、华南、华中、西北、西南、东北
    // A级：华北、华东
    // B级：华南、华中
    // C级：西北、西南
    // D级：东北

    // case when
    // 根据多个条件，不同的条件对应不同的值
    // case when then ... when then ... else ... end

    val sql =
      s"""
         |SELECT
         |    AREA,
         |    CASE
         |        WHEN AREA= 'China North'
         |        OR AREA= 'China East'
         |        THEN 'A Level'
         |        WHEN AREA= 'China South'
         |        OR AREA= 'China Middle'
         |        THEN 'B Level'
         |        WHEN AREA= 'West North'
         |        OR AREA= 'West South'
         |        THEN 'C Level'
         |        ELSE 'D Level'
         |    END area_level, product_id, city_infos, click_count, product_name, product_status
         |FROM
         |    (SELECT
         |        AREA, product_id, click_count, city_infos, product_name, product_status,
         |        row_number () OVER (
         |            PARTITION BY AREA
         |            ORDER BY click_count DESC
         |        ) rank
         |    FROM
         |        tmp_area_fullprod_click_count) t
         |WHERE rank <= 3
       """.stripMargin

    spark.sql(sql)
  }

  /**
    * 生成区域商品点击次数临时表（包含了商品的完整信息）
    *
    */
  def generateTempAreaFullProductClickCountTable(spark: SparkSession) {

    // 将之前得到的各区域各商品点击次数表，product_id
    // 去关联商品信息表，product_id，product_name和product_status
    // product_status要特殊处理，0，1，分别代表了自营和第三方的商品，放在了一个json串里面
    // get_json_object()函数，可以从json串中获取指定的字段的值
    // if()函数，判断，如果product_status是0，那么就是自营商品；如果是1，那么就是第三方商品
    // area, product_id, click_count, city_infos, product_name, product_status

    // 你拿到到了某个区域top3热门的商品，那么其实这个商品是自营的，还是第三方的
    // 其实是很重要的一件事

    // 技术点：内置if函数的使用

    // tmp_area_product_click_count
    //"AREA", "product_id", "click_count", "city_infos"
    //(华中,40,13,(5:武汉,6:长沙))

    //spark.sql("select extend_info from product_info").rdd.foreach(println)
    //[{"product_status": 1}]
    val sql =
      s"""
         |SELECT
         |    tapcc.area, tapcc.product_id, tapcc.click_count, tapcc.city_infos, pi.product_name, IF(
         |        get_json_object (
         |            pi.extend_info, 'product_status'
         |        ) = '0', 'Self', 'Third Party'
         |    ) product_status
         |FROM
         |    tmp_area_product_click_count tapcc
         |    JOIN product_info PI
         |        ON tapcc.product_id = pi.product_id
       """.stripMargin

    val df = spark.sql(sql)
    //df.show
    /*
+----+----------+-----------+----------+------------+--------------+
|area|product_id|click_count|city_infos|product_name|product_status|
+----+----------+-----------+----------+------------+--------------+
|  西北|        43|         11|      7:西安|   product43|          Self|
|  西南|        70|         11|      8:成都|   product70|   Third Party|*/

    df.createOrReplaceTempView("tmp_area_fullprod_click_count")
  }

  /**
    * 生成各区域各商品点击次数临时表
    *
    */
  def generateTempAreaPrdocutClickCountTable(spark: SparkSession) {
    //  "city_id", "city_name", "area", "product_id"
    //  (2,南京,华东,8)

    // 按照area和product_id两个字段进行groupBy
    // 计算出各区域各商品的点击次数
    // 可以获取到每个area下的每个product_id的城市信息拼接起来的串
    val sql =
    s"""
       |SELECT
       |    AREA, product_id, COUNT(*) click_count, group_concat_distinct (
       |        concat_long_string (city_id, city_name, ':')
       |    ) city_infos
       |FROM
       |    tmp_click_product_basic
       |GROUP BY AREA, product_id
   """.stripMargin

    val df = spark.sql(sql)
    //df.show
    /*
+----+----------+-----------+----------+
|AREA|product_id|click_count|city_infos|
+----+----------+-----------+----------+
|  西北|        43|         11|      7:西安|
|  西南|        70|         11|      8:成都|
|  东北|        77|         13|     9:哈尔滨|
|  华中|        40|         13| 5:武汉,6:长沙|
|  华中|        57|         39| 5:武汉,6:长沙|*/

    // 各区域各商品的点击次数（以及额外的城市列表）,再次将查询出来的数据注册为一个临时表
    df.createOrReplaceTempView("tmp_area_product_click_count")
  }

  /**
    * 生成点击商品基础信息临时表
    *
    */
  def generateTempClickProductBasicTable(spark: SparkSession, cityid2clickActionRDD: RDD[(Long, Row)], cityid2cityInfoRDD: RDD[(Long, Row)]) {
    // (2,[2,8])          (cityId,(cityId,productId))
    //  (2,[2,成都,西南])  (city_id , 城市信息)
    // 执行join操作，进行点击行为数据和城市数据的关联
    val joinedRDD = cityid2clickActionRDD.join(cityid2cityInfoRDD)

    // 将上面的joinedRDD，转换成一个joinedRDD<Row>（才能将RDD转换为DataFrame）
    val mappedRDD = joinedRDD.map { case (cityid, (action, cityinfo)) =>
      val productid = action.getLong(1)
      val cityName = cityinfo.getString(1)
      val area = cityinfo.getString(2)
      (cityid, cityName, area, productid)
    }
    //mappedRDD.foreach(println)
    // 2, 成都, 西南, product08

    import spark.implicits._
    val df = mappedRDD.toDF("city_id", "city_name", "area", "product_id")
    // 为df创建临时表
    df.createOrReplaceTempView("tmp_click_product_basic")
  }

  /**
    * 使用Spark SQL从MySQL中查询城市信息
    *
    */
  def getCityid2CityInfoRDD(spark: SparkSession): RDD[(Long, Row)] = {

    val cityInfo = Array(
      (0L, "北京", "华北"), (1L, "上海", "华东"),
      (2L, "南京", "华东"), (3L, "广州", "华南"),
      (4L, "三亚", "华南"), (5L, "武汉", "华中"),
      (6L, "长沙", "华中"), (7L, "西安", "西北"),
      (8L, "成都", "西南"), (9L, "哈尔滨", "东北"))
    import spark.implicits._
    val cityInfoDF = spark.sparkContext.makeRDD(cityInfo).toDF("city_id", "city_name", "area")

    cityInfoDF.rdd.map(item => (item.getAs[Long]("city_id"), item))
  }

  /**
    * 查询指定日期范围内的点击行为数据
    *
    * @return 点击行为数据
    */
  def getCityid2ClickActionRDDByDate(spark: SparkSession, taskParam: JSONObject): RDD[(Long, Row)] = {
    // 从user_visit_action中，查询用户访问行为数据
    // 第一个限定：click_product_id，限定为不为空的访问行为，那么就代表着点击行为
    // 第二个限定：在用户指定的日期范围内的数据
    // 获取任务日期参数
    /*startDate 起始日期
    * endDate   截止日期*/
    val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)

    val sql =
      s"""
         |SELECT
         |    city_id, click_product_id
         |FROM
         |    user_visit_action
         |WHERE click_product_id IS NOT NULL
         |    AND click_product_id != -1
         |    AND date >= '$startDate'
         |    AND date <= '$endDate'
       """.stripMargin

    val clickActionDF = spark.sql(sql)
    //clickActionDF.show()

    //(cityid, row)
    clickActionDF.rdd.map(item => (item.getAs[Long]("city_id"), item))
  }

}
