package teacherPage

import java.util.UUID

import com.atguigu.commons.conf.ConfigurationManager
import com.atguigu.commons.constant.Constants
import com.atguigu.commons.model.UserVisitAction
import com.atguigu.commons.utils.{DateUtils, NumberUtils, ParamUtils}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable

object PageStat {
  def main(args: Array[String]): Unit = {

    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    //key:startDate value:2018-10-01
    val taskParams = JSONObject.fromObject(jsonStr)

    //获取主键id(在存储数据到mysql中,用于标识不同的统计)
    val taskUUID = UUID.randomUUID().toString

    //创建sparkconf
    val sparkconf = new SparkConf().setAppName("pageStat").setMaster("local[*]")

    //创建sparkSession
    val sparkSession = SparkSession.builder().config(sparkconf).enableHiveSupport().getOrCreate()

    //加载所有的Action数据 RDD[UserVisitAction]
    val actionRDD = getActionRDD(sparkSession,taskParams)

    //按照sessionId进行聚合
    val sessionId2actionRDD = actionRDD.map{
      item => (item.session_id,item)
    }

    //groupBykey  sessionId2GroupRDD : RDD[(sessionId,Iterater[UserAction])]
    val sessionId2GroupRDD: RDD[(String, Iterable[UserVisitAction])]= sessionId2actionRDD.groupByKey()


    //"1,2,3,4,5,6,7"
    val pageStatStr = ParamUtils.getParam(taskParams,Constants.PARAM_TARGET_PAGE_FLOW)

    //splitPage:Array(1,2,3,4,5,6,7)
    val splitPage = pageStatStr.split(",")

    // splitPage.slice(0,splitPage.length-1):[1,2,3,4,5,6]
    // splitPage.tail : [2,3,4,5,6,7]
    // splitPage.slice(0,splitPage.length-1).zip(splitPage.tail) : [(1,2),(2,3)...]
     // 1_2,2_3....
    val  targetPageSplit= splitPage.slice(0,splitPage.length-1).zip(splitPage.tail).map{
      case (page1,page2) => page1+"_"+page2
    }

    //按时间排序行为数据
    //RDD[(sessionId,Iterater[UserAction])]
    //pageSplitNumRDD: RDD(pageSplit,1L)
    val pageSplitNumRDD = sessionId2GroupRDD.flatMap{
      case (sessionId,iterableAction) =>
        //获取时间 sortList:List[UserVistAction]
        val sortList = iterableAction.toList.sortWith((item1,item2)=>{
          DateUtils.parseTime(item1.action_time).getTime > DateUtils.parseTime(item2.action_time).getTime
        })
        //pageList:List[1,2,3,4...]
        val pageList = sortList.map{
          case action => action.page_id
        }
        // pageSplit :1_2,2_3....
        val  pageSplit= pageList.slice(0,pageList.length-1).zip(pageList.tail).map{
          case (page1,page2) => page1+"_"+page2
        }

        val pageSplitFilter = pageSplit.filter{
          case pageSplit => targetPageSplit.contains(pageSplit)
        }

        //转化为 (split,1L)
        pageSplitFilter.map{
          case pageSplit => (pageSplit,1L)
        }
    }
    //统计切片点击数
    val pageSplitRadio = pageSplitNumRDD.countByKey()

    //统计首页的点击数
    val startPage = splitPage(0).toLong
    val startPageCount = sessionId2actionRDD.filter{
      case (sessionId,action) => action.page_id ==startPage
    }.count()

    //统计页面转化率
    getConvertStat(startPageCount,taskUUID,targetPageSplit,sparkSession,pageSplitRadio)

  }

  def getConvertStat(startPageCount: Long,
                     taskUUID: String,
                     targetPageSplit: Array[String],
                     sparkSession: SparkSession,
                     pageSplitRadio: collection.Map[String, Long]) = {
    //定义存储转化率的结构 [(String,Double)]
    val stringToDoubleMap = new mutable.HashMap[String,Double]()

    var lastCount = startPageCount.toDouble

    for(pageSplit <- targetPageSplit){
      //当前的切片数
      val cuspCount = pageSplitRadio(pageSplit).toDouble
      val radio = cuspCount/lastCount
      stringToDoubleMap.put(pageSplit,radio)
      lastCount =cuspCount
    }
    //将统计结果保存到数据库中
    val pageSplitStr = stringToDoubleMap.map{
      case (tisk,radio) => tisk + "=" +radio
    }.mkString("|")
    //封装数据到样例类中
   val pageSplitConvertRate = PageSplitConvertRate(taskUUID,pageSplitStr)

   val pageSplitConvertRateRDD = sparkSession.sparkContext.makeRDD(Array(pageSplitConvertRate))
    import sparkSession.implicits._

    pageSplitConvertRateRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "page_split_convert_rate_0508")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()
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
}
