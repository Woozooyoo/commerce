/*
 * Copyright (c) 2018. Atguigu Inc. All Rights Reserved.
 * Date: 18-10-10 下午3:37.
 * Author: Adrian.
 */

package teacher

import java.util
import java.util.{Date, Random, UUID}
import com.atguigu.commons.conf.ConfigurationManager
import com.atguigu.commons.constant.Constants
import com.atguigu.commons.model.{UserInfo, UserVisitAction}
import com.atguigu.commons.utils._
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/** create time 2018/10/8 14:24 by liuxiaolong */
object SessionStaticsAgg {


  def main(args: Array[String]): Unit = {

    /***********需求一.session访问时长/步长占比统计*************/
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    //key:startDate value:2018-10-01
    val taskParams = JSONObject.fromObject(jsonStr)

    //获取主键id(在存储数据到mysql中,用于标识不同的统计)
    val taskUUID = UUID.randomUUID().toString

    //创建sparkconf
    val sparkconf = new SparkConf().setAppName("sessionStaticsAgg").setMaster("local[*]")

    //创建sparkSession
    val sparkSession = SparkSession.builder().config(sparkconf).enableHiveSupport().getOrCreate()

    //加载所有的Action数据 RDD[UserVisitAction]
    val actionRDD = getActionRDD(sparkSession,taskParams)

    //按照sessionid进行聚合
    val sessionId2actionRDD = actionRDD.map{
      item => (item.session_id,item)
    }

      //groupBykey
    val sessionId2GroupRDD: RDD[(String, Iterable[UserVisitAction])]= sessionId2actionRDD.groupByKey()

    //添加缓存
    sessionId2GroupRDD.cache()

    //获取所有统计信息数据
   val userId2AggrInfoRDD =  getFullDataRDD(sparkSession,sessionId2GroupRDD: RDD[(String, Iterable[UserVisitAction])])

    //创建累加器
    val sessionStatAccumulator = new SessionStatAccumulator

    //注册累加器
    sparkSession.sparkContext.register(sessionStatAccumulator)


    //过滤用户信息
    val  fiterDataRDD= getfilterData(taskParams,sessionStatAccumulator,sparkSession,userId2AggrInfoRDD)

//    fiterDataRDD.foreach(println(_))
//
//    //打印累加器的中是统计值
//    for((k,v) <- sessionStatAccumulator.value){
//      println("k="+k+",value ="+v)
//    }

    //获取最终的统计结果
    getFinalData(sparkSession,taskUUID,sessionStatAccumulator.value)


    /***********需求二:Session随机抽取*************/
    //fiterDataRDD : RDD[(sid,fullinfo)]  原来的数据一个sessionId对应一条fullinfo记录
    sessionIdRadomRDD(sparkSession,taskUUID,fiterDataRDD)

    /***********需求三:Top10热门品类统计*************/
    //sessionId2actionRDD: RDD[(sessionid,action)] 所有action的操作数据
    // fiterDataRDD : RDD[(sessionId,fullInfo)] 过滤后的RDD
    //sessionId2FilterActionRDD : join 可以取出过滤条件的RDD
    //获取到符合session条件的RDD
    //sessionId2FilterActionRDD: RDD[(sessionId,action)]
    val sessionId2FilterActionRDD = sessionId2actionRDD.join(fiterDataRDD).map{
      case (sessionId,(action,fullInfo)) =>
        (sessionId,action)
    }

    //Top10热门品类统计
    //sessionId2FilterActionRDD:RDD[(sessionId,action)]
    //top10CategorysArray:Array[sortkey,countInfo]
   val top10CategorysArray = top10HotCategorys(sparkSession,taskUUID,sessionId2FilterActionRDD)


   /***********需求四：Top10热门品类的Top10活跃Session统计*************/

    top10Session(sparkSession,taskUUID,sessionId2FilterActionRDD,top10CategorysArray)

  }


  //需求四：Top10热门品类的Top10活跃Session统计
  def top10Session(sparkSession: SparkSession,
                   taskUUID: String,
                   sessionId2FilterActionRDD: RDD[(String, UserVisitAction)],
                   top10CategorysArray: Array[(SoryKey, String)]): Unit = {

    //第一步:过滤出点击过top10品类的所有action
    //top10CategorysArray:Array[sortkey,countInfo]
    //获取top10的品类
    // cidArray : Array[Long]
    val cidArray = top10CategorysArray.map{
      case (sortkey,countInfo) =>
        val cid = StringUtils.getFieldFromConcatString(countInfo,"\\|",Constants.FIELD_CATEGORY_ID).toLong
        cid
    }
    //过滤出点击过top10品类的所有action
    val sessionIdActionRDD = sessionId2FilterActionRDD.filter{
      case (sessionId,action) =>
        cidArray.contains(action.click_category_id)
    }
    //根据sessionId聚合数据
    val sessionIdGroupActionRDD = sessionIdActionRDD.groupByKey()

    //RDD[(cid,sessionCount)]
    val cidSessionCountRDD = sessionIdGroupActionRDD.flatMap{
      case (sessionId,iterableAction) =>
        var categroyMap = new mutable.HashMap[Long,Long]()
        for(action <- iterableAction){
          val cid = action.click_category_id
          if(!categroyMap.contains(cid)){
            categroyMap += (cid -> 0)
          }
          categroyMap.update(cid,categroyMap(cid)+1)
        }

        //RDD[(cid,sessionCount)] 点击的品类和次数
      for((cid,count) <- categroyMap)
        yield (cid,sessionId+"="+count)
    }


    // cid2GroupRDD: RDD[(cid, iterableSessionCount)]
    // cid2GroupRDD每一条数据都是一个categoryid和它对应的所有点击过它的session对它的点击次数
    val cid2GroupRDD = cidSessionCountRDD.groupByKey()

    // top10SessionRDD: RDD[Top10Session]
    val top10SessionRDD = cid2GroupRDD.flatMap{
      case (cid, iterableSessionCount) =>
        // true: item1放在前面
        // flase: item2放在前面
        // item: sessionCount   String   "sessionId=count"
        val sortList = iterableSessionCount.toList.sortWith((item1, item2) => {
          item1.split("=")(1).toLong > item2.split("=")(1).toLong
        }).take(10)

        val top10Session = sortList.map{
          // item : sessionCount   String   "sessionId=count"
          case item =>
            val sessionId = item.split("=")(0)
            val count = item.split("=")(1).toLong
            Top10Session(taskUUID, cid, sessionId, count)
        }

        top10Session
    }

    import sparkSession.implicits._
    top10SessionRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "top10_session_0508")
      .mode(SaveMode.Append)
      .save()

  }


  //Top10热门品类统计
  def top10HotCategorys(sparkSession: SparkSession,
                        taskUUID: String,
                        sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]) = {
    //第一步:获取所有发生过点击,下单,付款的品类
    var cId2CidRDD = sessionId2FilterActionRDD.flatMap{
      case (cid,action) =>
        var categoryBuffer = new ArrayBuffer[(Long,Long)]()
        //点击品类
        if(action.click_category_id != -1L){
          categoryBuffer += ((action.click_category_id,action.click_category_id))
        }else if(action.order_category_ids != null){//下单品类
          for(orderCid <- action.order_category_ids.split(",")){
            categoryBuffer += ((orderCid.toLong,orderCid.toLong))
          }
        }else if(action.pay_category_ids != null){
          for(payCid <- action.pay_category_ids.split(",")){
            categoryBuffer += ((payCid.toLong,payCid.toLong))
          }
        }

        categoryBuffer
    }
    //过滤重复
    cId2CidRDD = cId2CidRDD.distinct()

    //第二步:求出点击,下单,付款次数
    //点击次数
    val cId2ClickCountRDD = getClickCount(sessionId2FilterActionRDD)
    //下单次数
    val cId2OrderCountRDD = getOrderClickCount(sessionId2FilterActionRDD)
    //付款次数
    val cId2PayCountRDD = getPayClickCount(sessionId2FilterActionRDD)

    //聚合全部数据
    val cid2FullRDD = getFullCount(cId2ClickCountRDD,cId2OrderCountRDD,cId2PayCountRDD,cId2CidRDD)

    //第三步:自定义排序
    val sortByRDD = cid2FullRDD.map{
      case (cid,aggInfo) =>
        val clickCount = StringUtils.getFieldFromConcatString(aggInfo,"\\|",Constants.FIELD_CLICK_COUNT).toLong
        val orderCount=StringUtils.getFieldFromConcatString(aggInfo,"\\|",Constants.FIELD_ORDER_COUNT).toLong
        val payCount = StringUtils.getFieldFromConcatString(aggInfo,"\\|",Constants.FIELD_PAY_COUNT).toLong

        val sortkey = SoryKey(clickCount,orderCount,payCount)

        (sortkey,aggInfo)
    }

    //获取出top10热门品类
    val top10Categorys: Array[(SoryKey, String)]=sortByRDD.sortByKey(false).take(10)

    //通过加载数据到SparkContent中,封装到样例类,再存储到mysql
   val Top10CategoryRDD = sparkSession.sparkContext.makeRDD(top10Categorys).map{
      case (sortKey,countInfo) =>
        val categoryid = StringUtils.getFieldFromConcatString(countInfo,"\\|",Constants.FIELD_CATEGORY_ID).toLong
        val clickCount = sortKey.clickCount
        val orderCount = sortKey.orderCount
        val payCount = sortKey.payCount
        //封装样例实例
        Top10Category(taskUUID,categoryid,clickCount,orderCount,payCount)
    }

    //转换为DF
    import sparkSession.implicits._
    Top10CategoryRDD.toDF().write.format("jdbc")
      .option("url",ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user",ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password",ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable","top10Category0508")
      .mode(SaveMode.Append)
      .save()

    top10Categorys
  }


  def getFullCount(cId2ClickCountRDD: RDD[(Long, Long)],
                   cId2OrderCountRDD: RDD[(Long, Long)],
                   cId2PayCountRDD: RDD[(Long, Long)],
                   cId2CidRDD: RDD[(Long, Long)]) = {
    //全部发生过点击/下单/付款的品类 join 点击次数
    val cIdClickInfo = cId2CidRDD.leftOuterJoin(cId2ClickCountRDD).map{
      case (cid,(categoryId,option)) =>
        var clickCount = if(option.isDefined) option.get else 0
        val aggInfo = Constants.FIELD_CATEGORY_ID + "=" +cid +"|"+ Constants.FIELD_CLICK_COUNT +"="+clickCount

        (cid,aggInfo)
    }

    val cIdOrderInfo = cIdClickInfo.leftOuterJoin(cId2OrderCountRDD).map{
      case (cid,(clickInfo,option)) =>
        var  orderCount = if(option.isDefined) option.get else 0
        val aggInfo = clickInfo+"|"+Constants.FIELD_ORDER_COUNT+"="+orderCount
        (cid,aggInfo)
    }

    val cidPayInfo = cIdOrderInfo.leftOuterJoin(cId2PayCountRDD).map{
      case (cid,(orderInfo,option)) =>
        val payCount =  if(option.isDefined) option.get else 0
        val aggInfo = orderInfo + "|"+Constants.FIELD_PAY_COUNT+"="+payCount

        (cid,aggInfo)
    }

    cidPayInfo
  }
  def getPayClickCount(sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]) = {
   val filterPayClickRDD = sessionId2FilterActionRDD.filter(item => item._2.pay_category_ids !=null)
    val payCidRDD =  filterPayClickRDD.flatMap{
      case (sessionId,action) =>action.pay_category_ids.split(",").map(item => (item.toLong,1L))
    }
    payCidRDD.reduceByKey(_+_)
  }

  //下单次数
  def getOrderClickCount(sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]) = {
    val filterOrderClickRDD = sessionId2FilterActionRDD.filter(item => item._2.order_category_ids != null)
     val orderCidRDD =  filterOrderClickRDD.flatMap{
      case (sessionId,action) =>action.order_category_ids.split(",").map(item => (item.toLong,1L))
    }
    orderCidRDD.reduceByKey(_+_)
  }
  //求出点击
  def getClickCount(sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]) = {
    //过滤点击的行为
    val filterClickRDD = sessionId2FilterActionRDD.filter(item => item._2.click_category_id != -1L)

    //转换格式
    val clickCidRDD = filterClickRDD.map{
      case (sessionId,action) => (action.click_category_id,1L)
    }

    clickCidRDD.reduceByKey(_+_)
  }


  def sessionIdRadomRDD(sparkSession: SparkSession, taskUUID: String, fiterDataRDD: RDD[(String, String)]): Unit = {
    //key的粒度转换  fiterDataRDD : RDD[(sid,fullinfo)] ==> RDD[(h,fullinfo)]
    val dataHour2FullInfo: RDD[(String, String)]  = fiterDataRDD.map{
      case (sid,fullinfo) => {
        //yyyy-MM-dd HH:mm:ss
        val startTime = StringUtils.getFieldFromConcatString(fullinfo,"\\|",Constants.FIELD_START_TIME)
        //yyyy-MM-dd_HH
        val dataHour = DateUtils.getDateHour(startTime)
        (dataHour,fullinfo)
      }
    }
    //Map[(dataHour,count)]
    val hourCountMap = dataHour2FullInfo.countByKey()

    //Map[(date,(hour,count))] 定义一种存储结构
    val dateHourCountMap = new mutable.HashMap[String,mutable.HashMap[String,Long]]()
    //yyyy-MM-dd_HH
    for((dataHour,count) <- hourCountMap){
      val date =  dataHour.split("_")(0)
      val hour = dataHour.split("_")(1)

      dateHourCountMap.get(date) match {
          //为什么要new HashMap  map.get --> option : none some
        case None => dateHourCountMap(date) = new mutable.HashMap[String,Long]()
          dateHourCountMap(date) += (hour -> count)
        case Some(map) => dateHourCountMap(date) += (hour -> count)
      }


    }

    //解决的问题一: 一共有多少天 dateHourCountMap.size
    //一天抽多少 : 100/dateHourCountMap.size
    val extractPreDay = 100/dateHourCountMap.size
    //解决问题二: 一个抽多少条session :dateHourCountMap(date).values.sum
    //解决问题三: 一个小时有多少Session: dateHourCountMap(date)(hour)

   val hourDateExtractMap =  new mutable.HashMap[String,mutable.HashMap[String,ListBuffer[Int]]]()

    //Map[date,Map(hour,List[count])]
    for((date,hourcountMap )<- dateHourCountMap){
      val dateSessionCount = hourcountMap.values.sum

      hourDateExtractMap.get(date) match {
        case None => hourDateExtractMap(date) = new mutable.HashMap[String,ListBuffer[Int]]()
          //抽取数据
          extractRadmonIndexList(extractPreDay,dateSessionCount,hourcountMap,hourDateExtractMap(date))
        case Some(map) =>
          extractRadmonIndexList(extractPreDay,dateSessionCount,hourcountMap,hourDateExtractMap(date))
      }

    }

    //为了提升性能,使用广播变量广播hourDateExtractMap
    val hourDateExtractMapBR = sparkSession.sparkContext.broadcast(hourDateExtractMap)

    //RDD[(h,fullinfo)]
    //RDD[datahour,Iterable]
    val dataHour2FullInfoRDD =  dataHour2FullInfo.groupByKey()

    val ramdomRDD = dataHour2FullInfoRDD.flatMap{
      case (dataHour,iterableInfo) =>
        val date = dataHour.split("_")(0)
        val hour = dataHour.split("_")(1)

        var extractMap = hourDateExtractMapBR.value.get(date).get(hour)

       var extractSessionArrayBuffer =  new ArrayBuffer[SessionRandomExtract]()

        var index = 0

        for(fullInfo <- iterableInfo){
          if(extractMap.contains(index)){
            val sessionId = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_SESSION_ID)
            val startTime = StringUtils.getFieldFromConcatString(fullInfo, "\\|",Constants.FIELD_START_TIME)
            val searchKeywords = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS)
            val clickCategories = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS)

            val extractSession = SessionRandomExtract(taskUUID, sessionId, startTime, searchKeywords, clickCategories)

            extractSessionArrayBuffer += extractSession
          }
          index += 1
        }

        extractSessionArrayBuffer
    }

    import sparkSession.implicits._
    ramdomRDD.toDF().write
      .format("jdbc").option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user",ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "session_extract_0508")
      .mode(SaveMode.Append)
      .save()
  }


  def extractRadmonIndexList(extractPreDay: Int,
                             dateSessionCount: Long,
                             hourcountMap: mutable.HashMap[String, Long],
                             hourListMap: mutable.HashMap[String, ListBuffer[Int]]) = {
    for((hour,count) <- hourcountMap){
      //获取一个小时抽取的数据
      var hourExtCount = (count/dateSessionCount.toDouble*extractPreDay).toInt
      //避免一个小时抽取的数据大于总数
      if(hourExtCount > count){
        hourExtCount = count.toInt
      }

      var random =  new Random()

      hourListMap.get(hour) match {
        case None => hourListMap(hour) = new ListBuffer[Int]
          for(i <- 0 until hourExtCount){
            var index = random.nextInt(count.toInt)
            while(hourListMap(hour).contains(index)){
              index = random.nextInt(count.toInt)
            }
            hourListMap(hour).append(index)
          }
        case Some(list) =>
          for(i <- 0 until hourExtCount){
            var index = random.nextInt(count.toInt)
            while(hourListMap(hour).contains(index)){
              index = random.nextInt(count.toInt)
            }
            hourListMap(hour).append(index)
          }
      }

    }

  }


  def getFinalData(sparkSession: SparkSession, taskUUID: String, value: mutable.HashMap[String, Int]): Unit = {
    //获取所有符合条件的Session个数
    val session_count = value.getOrElse(Constants.SESSION_COUNT,1).toDouble

    // 不同范围访问时长的session个数
    val visit_length_1s_3s = value.getOrElse(Constants.TIME_PERIOD_1s_3s, 0)
    val visit_length_4s_6s = value.getOrElse(Constants.TIME_PERIOD_4s_6s, 0)
    val visit_length_7s_9s = value.getOrElse(Constants.TIME_PERIOD_7s_9s, 0)
    val visit_length_10s_30s = value.getOrElse(Constants.TIME_PERIOD_10s_30s, 0)
    val visit_length_30s_60s = value.getOrElse(Constants.TIME_PERIOD_30s_60s, 0)
    val visit_length_1m_3m = value.getOrElse(Constants.TIME_PERIOD_1m_3m, 0)
    val visit_length_3m_10m = value.getOrElse(Constants.TIME_PERIOD_3m_10m, 0)
    val visit_length_10m_30m = value.getOrElse(Constants.TIME_PERIOD_10m_30m, 0)
    val visit_length_30m = value.getOrElse(Constants.TIME_PERIOD_30m, 0)

    // 不同访问步长的session个数
    val step_length_1_3 = value.getOrElse(Constants.STEP_PERIOD_1_3, 0)
    val step_length_4_6 = value.getOrElse(Constants.STEP_PERIOD_4_6, 0)
    val step_length_7_9 = value.getOrElse(Constants.STEP_PERIOD_7_9, 0)
    val step_length_10_30 = value.getOrElse(Constants.STEP_PERIOD_10_30, 0)
    val step_length_30_60 = value.getOrElse(Constants.STEP_PERIOD_30_60, 0)
    val step_length_60 = value.getOrElse(Constants.STEP_PERIOD_60, 0)

    val visit_length_1s_3s_ratio = NumberUtils.formatDouble(visit_length_1s_3s / session_count, 2)
    val visit_length_4s_6s_ratio = NumberUtils.formatDouble(visit_length_4s_6s / session_count, 2)
    val visit_length_7s_9s_ratio = NumberUtils.formatDouble(visit_length_7s_9s / session_count, 2)
    val visit_length_10s_30s_ratio = NumberUtils.formatDouble(visit_length_10s_30s / session_count, 2)
    val visit_length_30s_60s_ratio = NumberUtils.formatDouble(visit_length_30s_60s / session_count, 2)
    val visit_length_1m_3m_ratio = NumberUtils.formatDouble(visit_length_1m_3m / session_count, 2)
    val visit_length_3m_10m_ratio = NumberUtils.formatDouble(visit_length_3m_10m / session_count, 2)
    val visit_length_10m_30m_ratio = NumberUtils.formatDouble(visit_length_10m_30m / session_count, 2)
    val visit_length_30m_ratio = NumberUtils.formatDouble(visit_length_30m / session_count, 2)

    val step_length_1_3_ratio = NumberUtils.formatDouble(step_length_1_3 / session_count, 2)
    val step_length_4_6_ratio = NumberUtils.formatDouble(step_length_4_6 / session_count, 2)
    val step_length_7_9_ratio = NumberUtils.formatDouble(step_length_7_9 / session_count, 2)
    val step_length_10_30_ratio = NumberUtils.formatDouble(step_length_10_30 / session_count, 2)
    val step_length_30_60_ratio = NumberUtils.formatDouble(step_length_30_60 / session_count, 2)
    val step_length_60_ratio = NumberUtils.formatDouble(step_length_60 / session_count, 2)

    //将数据封装到样例类中
    val stat = SessionAggrStat(taskUUID, session_count.toInt, visit_length_1s_3s_ratio, visit_length_4s_6s_ratio, visit_length_7s_9s_ratio,
      visit_length_10s_30s_ratio, visit_length_30s_60s_ratio, visit_length_1m_3m_ratio,
      visit_length_3m_10m_ratio, visit_length_10m_30m_ratio, visit_length_30m_ratio,
      step_length_1_3_ratio, step_length_4_6_ratio, step_length_7_9_ratio,
      step_length_10_30_ratio, step_length_30_60_ratio, step_length_60_ratio)

    //装数据封装到RDD中
    val statRDD = sparkSession.sparkContext.makeRDD(Array(stat))

    //操作mysql数据库
    import sparkSession.implicits._
    statRDD.toDF().write.format("jdbc").option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "session_ration_0508")
      .mode(SaveMode.Append)
      .save()

  }

  //统计范围时长
  def countVisitLength(visitLength: Long, sessionStatAccumulator: SessionStatAccumulator) = {
    if(visitLength >=1 && visitLength<=3) {
      sessionStatAccumulator.add(Constants.TIME_PERIOD_1s_3s)
    } else if (visitLength >= 4 && visitLength <= 6) {
      sessionStatAccumulator.add(Constants.TIME_PERIOD_4s_6s)
    } else if (visitLength >= 7 && visitLength <= 9) {
      sessionStatAccumulator.add(Constants.TIME_PERIOD_7s_9s)
    } else if (visitLength >= 10 && visitLength <= 30) {
      sessionStatAccumulator.add(Constants.TIME_PERIOD_10s_30s)
    } else if (visitLength > 30 && visitLength <= 60) {
      sessionStatAccumulator.add(Constants.TIME_PERIOD_30s_60s)
    } else if (visitLength > 60 && visitLength <= 180) {
      sessionStatAccumulator.add(Constants.TIME_PERIOD_1m_3m)
    } else if (visitLength > 180 && visitLength <= 600) {
      sessionStatAccumulator.add(Constants.TIME_PERIOD_3m_10m)
    } else if (visitLength > 600 && visitLength <= 1800) {
      sessionStatAccumulator.add(Constants.TIME_PERIOD_10m_30m)
    } else if (visitLength > 1800) {
      sessionStatAccumulator.add(Constants.TIME_PERIOD_30m)
    }
  }

  //统计步长
  def countstepLength(stepLength: Long, sessionStatAccumulator: SessionStatAccumulator) = {
    if(stepLength >=1 && stepLength <=3){
      sessionStatAccumulator.add(Constants.STEP_PERIOD_1_3)
    }else if (stepLength >= 4 && stepLength <= 6) {
      sessionStatAccumulator.add(Constants.STEP_PERIOD_4_6)
    } else if (stepLength >= 7 && stepLength <= 9) {
      sessionStatAccumulator.add(Constants.STEP_PERIOD_7_9)
    } else if (stepLength >= 10 && stepLength <= 30) {
      sessionStatAccumulator.add(Constants.STEP_PERIOD_10_30)
    } else if (stepLength > 30 && stepLength <= 60) {
      sessionStatAccumulator.add(Constants.STEP_PERIOD_30_60)
    } else if (stepLength > 60) {
      sessionStatAccumulator.add(Constants.STEP_PERIOD_60)
    }
  }

  //进行数据的过滤
  def getfilterData(taskParams:JSONObject,
                    sessionStatAccumulator:SessionStatAccumulator,
                    sparkSession: SparkSession,
                    userId2AggrInfoRDD: RDD[(String, String)]) = {
    //取出所有的过滤条件
    val startAge = ParamUtils.getParam(taskParams,Constants.PARAM_START_AGE)
    val endAge= ParamUtils.getParam(taskParams,Constants.PARAM_END_AGE)
    val professionals= ParamUtils.getParam(taskParams,Constants.PARAM_PROFESSIONALS)
    val cities= ParamUtils.getParam(taskParams,Constants.PARAM_CITIES)
    val sex = ParamUtils.getParam(taskParams,Constants.PARAM_SEX)
    val keywords = ParamUtils.getParam(taskParams,Constants.PARAM_KEYWORDS)
    val categoryIds= ParamUtils.getParam(taskParams,Constants.PARAM_CATEGORY_IDS)


    val filterInfo = (if(startAge!=null) Constants.PARAM_START_AGE+"="+startAge+"|" else "")+
      (if(endAge!=null) Constants.PARAM_END_AGE+"="+endAge+"|"  else "")+
      (if(professionals!=null) Constants.PARAM_PROFESSIONALS+"="+professionals+"|"  else "")+
      (if(cities!=null) Constants.PARAM_CITIES+"="+cities+"|"  else "")+
      (if(keywords!=null) Constants.PARAM_KEYWORDS+"="+keywords+"|"  else "")+
      (if(categoryIds!=null) Constants.PARAM_CATEGORY_IDS+"="+categoryIds+"|"  else "")
    //"|" 去掉
    if(filterInfo.endsWith("\\|")){
      filterInfo.substring(0,filterInfo.length-1)
    }

    val sessionId2FilterRDD = userId2AggrInfoRDD.filter{
      case (sessionId,fullInfo) =>
        var success = true
        //过滤年龄范围
        if(!ValidUtils.between(fullInfo, Constants.FIELD_AGE, filterInfo, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE))
          success = false

        if(!ValidUtils.in(fullInfo, Constants.FIELD_PROFESSIONAL, filterInfo, Constants.PARAM_PROFESSIONALS))
          success = false

        if (!ValidUtils.in(fullInfo, Constants.FIELD_CITY, filterInfo, Constants.PARAM_CITIES))
          success = false

        if (!ValidUtils.equal(fullInfo, Constants.FIELD_SEX, filterInfo, Constants.PARAM_SEX))
          success = false

        if (!ValidUtils.in(fullInfo, Constants.FIELD_SEARCH_KEYWORDS, filterInfo, Constants.PARAM_KEYWORDS))
          success = false

        if (!ValidUtils.in(fullInfo, Constants.FIELD_CATEGORY_ID, filterInfo, Constants.PARAM_CATEGORY_IDS))
          success = false

        //对步长和时长
        if(success){
          //统计占比-->累加器累加
          sessionStatAccumulator.add(Constants.SESSION_COUNT)
          //时长
          val visitLength =  StringUtils.getFieldFromConcatString(fullInfo,"\\|",Constants.FIELD_VISIT_LENGTH).toLong
          //步长
          val stepLength = StringUtils.getFieldFromConcatString(fullInfo,"\\|",Constants.FIELD_STEP_LENGTH).toLong

          //求时长
          countVisitLength(visitLength,sessionStatAccumulator)
          //求步长
          countstepLength(stepLength,sessionStatAccumulator)
        }
        success
    }
    sessionId2FilterRDD

  }

  //根据日期加载所有的Action数据 RDD[UserVisitAction]
  def getActionRDD(sparkSession: SparkSession, taskParams: JSONObject) = {
    val startDate = ParamUtils.getParam(taskParams,Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParams,Constants.PARAM_END_DATE)

    val sql = "select * from user_visit_action where date >='"+ startDate + "' and date<='"+endDate+"'"

    import sparkSession.implicits._
    //DataFrame  --> Dataset[Row]
    // DataSet[UserVisitAction]
    //RDD[UserVisitAction]
    sparkSession.sql(sql).as[UserVisitAction].rdd

  }

  def getFullDataRDD(sparkSession: SparkSession,
                     sessionId2GroupRDD: RDD[(String, Iterable[UserVisitAction])]) = {

    val userId2AggrInfoRDD = sessionId2GroupRDD.map {
      case (sessionId, iterableAction) =>
        var startTime: Date = null
        var endTime: Date = null
        //同一个Action中的userId是相同的
        var userId = -1L
        var search_KeyWords = new StringBuffer("")

        var clickCategorys = new util.HashSet[String]()
        var step_length = 0


        //遍历所有的Action
        for (action <- iterableAction) {
          if (userId == -1L) {
            userId = action.user_id
          }
          val clickCategory = action.click_category_id

          //获取动作发生的时间
          val actionTime = DateUtils.parseTime(action.action_time)

          //更新startTime
          if (startTime == null || startTime.after(actionTime)) {
            startTime = actionTime
          }

          if (endTime == null || endTime.before(actionTime)) {
            endTime = actionTime
          }

          val search_KeyWord = action.search_keyword


          if (StringUtils.isNotEmpty(search_KeyWord)) {
            search_KeyWords.append(action.search_keyword + ",")
          }


          if (clickCategory != -1L) {
            clickCategorys.add(clickCategory + "")
          }
          //统计步长
          step_length += 1

        }
        var clickCg: String = null

        val searchKeyWords = StringUtils.trimComma(search_KeyWords.toString)

        if (!clickCategorys.toString.isEmpty) {

          clickCg = clickCategorys.toString.substring(1, clickCategorys.toString.length - 1)
        }
        //计算session访问时长(秒)
        val visitLength = (endTime.getTime - startTime.getTime) / 1000
        //聚合数据,使用key=value|key=value
        val aggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|" +
          Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeyWords + "|" +
          Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCg + "|" +
          Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
          Constants.FIELD_STEP_LENGTH + "=" + step_length + "|" +
          Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime)
        (userId, aggrInfo)
    }
    val sql = "select * from user_info "

    import sparkSession.implicits._
    //Dataset[Row] --> Dataset[UserInfo] --> rdd[UserInfo]
    val userInfoRDD = sparkSession.sql(sql).as[UserInfo].rdd.map(item => (item.user_id, item))

    userId2AggrInfoRDD.join(userInfoRDD).map {
      //[sessionid,fullResult]
      case (userId, (aggrInfo, userInfo)) =>
        val age = userInfo.age
        val sex = userInfo.sex
        val professional = userInfo.professional
        val city = userInfo.city

        val fullInfo = aggrInfo + "|" + Constants.FIELD_AGE + "=" + age + "|" +
          Constants.FIELD_PROFESSIONAL + "=" + professional + "|" + Constants.FIELD_SEX + "=" + sex + "|" + Constants.FIELD_CITY + "=" + city

        val sessionId = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_SESSION_ID)

        (sessionId, fullInfo)
    }
  }


}
