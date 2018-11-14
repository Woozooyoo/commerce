/*
 * Copyright (c) 2018. Atguigu Inc. All Rights Reserved.
 * Date: 18-10-12 下午7:02.
 * Author: Adrian.
 */

package teacherArea

import java.util.UUID

import com.atguigu.commons.conf.ConfigurationManager
import com.atguigu.commons.constant.Constants
import com.atguigu.commons.utils.ParamUtils
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object AreaStat {

  def main(args: Array[String]): Unit = {

    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    //key:startDate value:2018-10-01
    val taskParams = JSONObject.fromObject(jsonStr)

    //获取主键id(在存储数据到mysql中,用于标识不同的统计)
    val taskUUID = UUID.randomUUID().toString

    //创建sparkconf
    val sparkconf = new SparkConf().setAppName("areaStat").setMaster("local[*]")

    //创建sparkSession
    val sparkSession = SparkSession.builder().config(sparkconf).enableHiveSupport().getOrCreate()

    //获取点击过商品id行为的数据
    //RDD [(city_id,click_product_id)]
    val cityId2PidRDD = getCityAndProductIdData(sparkSession,taskParams)

    //加载城市区域信息
    //RDD[(city_id,CityAreaInfo)]
    val cityAreaInfoRDD = getcityAndAreaData(sparkSession)

    //创建临时表
    getAreaInfoAndcityInfo(sparkSession,cityId2PidRDD,cityAreaInfoRDD)
    //sparkSession.sql("select * from tmp_area_basic_info").show()

    //先自定义UDF函数:city_id,city_name
    sparkSession.udf.register("concat_long_string",(v1:Long,v2:String,split:String) =>{
      v1+split+v2
    })
    //concat_long_string(city_id,city_name,":")

    //自定义UDAF函数
    sparkSession.udf.register("group_concat_distinct",new GroupConcatDistinct)

    //统计每个区域每个商品被点击的次数
    getAreaProductClickCount(sparkSession)

    //自定义获取json字符串的函数
    sparkSession.udf.register("get_json_field",(json:String,filed:String) =>{
      val jsonObject = JSONObject.fromObject(json)
      jsonObject.getString(filed)
    })
    //get_json_field(pi.extend_info,'product_status')

    //把商品的详情加入到数据当中
    getAreaProductClickCountAndProductInfo(sparkSession)




    sparkSession.sql("select * from tmp_area_count_product_info").show()

  }

  def getAreaProductClickCountAndProductInfo(sparkSession: SparkSession) = {
    //tmp_area_click_count : city_infos,area,pid,click_count acc
    //product_info : product_id,product_name,extend_info pi
    val sql = "select acc.area,acc.city_infos,acc.pid,acc.click_count,pi.product_name," +
      " if(get_json_field(pi.extend_info,'product_status')='0','自营','第三方') product_status " +
      "from tmp_area_click_count acc join product_info pi on acc.pid = pi.product_id"

    sparkSession.sql(sql).createOrReplaceTempView("tmp_area_count_product_info")
  }


  def getAreaProductClickCount(sparkSession: SparkSession)={
    val sql = "select group_concat_distinct(concat_long_string(city_id,city_name,':')) city_infos, area,pid,count(*) click_count from tmp_area_basic_info group by area,pid"
    sparkSession.sql(sql).createOrReplaceTempView("tmp_area_click_count")

  }

  def getAreaInfoAndcityInfo(sparkSession: SparkSession,
                             cityId2PidRDD: RDD[(Long, Long)],
                             cityAreaInfoRDD:RDD[(Long, CityAreaInfo)])={
    //(city_id,(pid,cityAreaInfo)
    val cityId2PRDD = cityId2PidRDD.join(cityAreaInfoRDD).map{
       case (city_id,(pid,cityAreaInfo)) =>
         (city_id,cityAreaInfo.city_name,cityAreaInfo.area,pid)
    }
    import sparkSession.implicits._

    cityId2PRDD.toDF("city_id","city_name","area","pid").createOrReplaceTempView("tmp_area_basic_info")

  }


  def getcityAndAreaData(sparkSession: SparkSession) = {
    val cityandAreaInfo = Array((0L, "北京", "华北"), (1L, "上海", "华东"), (2L, "南京", "华东"),
      (3L, "广州", "华南"), (4L, "三亚", "华南"), (5L, "武汉", "华中"),
      (6L, "长沙", "华中"), (7L, "西安", "西北"), (8L, "成都", "西南"),
      (9L, "哈尔滨", "东北"))
    //RDD[(city_id,CityAreaInfo)]
    sparkSession.sparkContext.makeRDD(cityandAreaInfo).map{
      case (city_id,city,area) =>
        (city_id,CityAreaInfo(city_id,city,area))
    }

  }

  def getCityAndProductIdData(sparkSession: SparkSession, taskParams: JSONObject)= {
      val startDate = ParamUtils.getParam(taskParams,Constants.PARAM_START_DATE)
      val endData = ParamUtils.getParam(taskParams,Constants.PARAM_END_DATE)

      //只获取有点击商品id的行为数据
      val sql = "select city_id,click_product_id from user_visit_action " +
        "where date >='"+startDate +"' and date <='"+ endData +"' and click_product_id !=-1L"
    import sparkSession.implicits._
     sparkSession.sql(sql).as[CityClickProduct].rdd.map{
       case (cityClickProduct) => (cityClickProduct.city_id,cityClickProduct.click_product_id)
     }


  }
}
