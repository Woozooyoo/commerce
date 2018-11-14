package com.atguigu.stream

import java.util.Date

import com.atguigu.commons.conf.ConfigurationManager
import com.atguigu.commons.utils.DateUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming._

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * 日志格式：
  * timestamp province city userid adid
  * 某个时间点 某个省份 某个城市 某个用户 某个广告
  */
object AdClickRealTimeStatOptimize {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("streamingRecommendingSystem").setMaster("local[*]")

    // 创建Spark客户端
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(5))
    // 设置检查点目录
    ssc.checkpoint("./analyse/advertising/OptimizeCheckpoint")

    // 获取Kafka配置
    val broker_list = ConfigurationManager.config.getString("kafka.broker.list")
    val topics = ConfigurationManager.config.getString("kafka.topics")

    // kafka消费者配置
    val kafkaParam = Map(
      "bootstrap.servers" -> broker_list,//用于初始化链接到集群的地址
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      //用于标识这个消费者属于哪个消费团体
      "group.id" -> "commerce-consumer-group",
      //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
      //可以使用这个配置，latest自动重置偏移量为最新的偏移量
      "auto.offset.reset" -> "latest",
      //如果是true，则这个消费者的偏移量会在后台自动提交
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // 创建DStream，返回接收到的输入数据
    // LocationStrategies：根据给定的主题和集群地址创建consumer
    // LocationStrategies.PreferConsistent：持续的在所有Executor之间分配分区 平均分配消息到所有worker 这个参数会将分区尽量均匀地分配到所有的可用Executor上去
    // ConsumerStrategies：选择如何在Driver和Executor上创建和配置Kafka Consumer
    // ConsumerStrategies.Subscribe：订阅一系列主题
    val adRealTimeLogDStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topics), kafkaParam))

    // 根据动态黑名单进行数据过滤 (timestamp province city userId adId)
    val adRealTimeValueDStream = adRealTimeLogDStream.map { consumerRecordRDD => consumerRecordRDD.value() }

    // 用于Kafka Stream的线程非安全问题，重新分区切断血统
    //adRealTimeValueDStream = adRealTimeValueDStream.repartition(400)
    adRealTimeValueDStream.cache()

    // 根据动态黑名单进行数据过滤 (userId, timestamp province city userId adId)
    val filteredAdRealTimeLogDStream: DStream[(Long, String)] = filterByBlacklist(spark,adRealTimeValueDStream)

    // 业务功能一：生成动态黑名单
    generateDynamicBlacklist(filteredAdRealTimeLogDStream)

    // 业务功能二：计算广告点击流量实时统计结果 得到（yyyyMMdd_province_city_adId,clickCount）
    val adRealTimeStatDStream = calculateRealTimeStat(filteredAdRealTimeLogDStream)

    // 业务功能三：实时统计每天每个省份top3热门广告
    calculateProvinceTop3Ad(spark,adRealTimeStatDStream)

    // 业务功能四：实时统计每天每个广告在最近1小时的滑动窗口内的点击趋势（每分钟的点击量）
    calculateAdClickCountByWindow(adRealTimeValueDStream)

    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 业务功能四：计算最近1小时滑动窗口内的广告点击趋势   这里是否应该传入已黑名单过滤的广告点击 而不是传入所有的点击
    * 实时统计每天每个广告在最近1小时的滑动窗口内的（每分钟统计一次）
    */
  def calculateAdClickCountByWindow(adRealTimeValueDStream:DStream[String]) {

    //(timestamp province city userId adId)
    val timeMinuteAdId2count: DStream[(String, Long)] = adRealTimeValueDStream.map { consumerRecord =>
      val split = consumerRecord.split(" ")
      val timestamp = split(0)
      val timeMinute = DateUtils.formatTimeMinute(new Date(timestamp.toLong))
      val adId = split(4)

      (timeMinute + "_" + adId, 1L)
    }

    val aggrRDD: DStream[(String, Long)] = timeMinuteAdId2count.reduceByKeyAndWindow((a:Long, b:Long) => a + b, Minutes(2L),Seconds(10L))

    aggrRDD.foreachRDD { rdd =>
      rdd.foreachPartition { items =>
        val adClickTrendBuffer = ListBuffer[AdClickTrend]()
        for (item <- items) {
          val split = item._1.split("_")
          val timeMinute = split(0)
          val adId = split(1).toLong

          val date = DateUtils.formatDate(DateUtils.parseDateKey(timeMinute.substring(0, 8)))
          val hour = timeMinute.substring(8, 10)
          val minute = timeMinute.substring(10)

          adClickTrendBuffer += AdClickTrend(date, hour, minute, adId, item._2)
        }
        AdClickTrendDAO.updateBatch(adClickTrendBuffer.toArray)
      }
    }

  }

  /**
    * 业务功能三：计算每天各省份的top3热门广告
    */
  def calculateProvinceTop3Ad(spark:SparkSession, adRealTimeStatDStream:DStream[(String, Long)]) {
    //(yyyyMMdd_province_city_adid, count)
    val dateProvince2count = adRealTimeStatDStream.map { case (key, count) =>
      val split = key.split("_")
      val date = split(0)
      val province = split(1)
      val adId = split(3)
      (date + "_" + province + "_" + adId, count)
    }

    val dateProvince2counts = dateProvince2count.reduceByKey(_+_)//不需要再updateStateByKey 因为传来的stream是stateful的
    // (date_province_adId, counts)
    import spark.implicits._

    val dateProvince2Top3: DStream[AdProvinceTop3] = dateProvince2counts.transform{ dateProvince2countsRDD =>
      val df = dateProvince2countsRDD.map { case (dpa, counts) =>
        val split = dpa.split("_")
        val datekey = split(0)
        val province = split(1)
        val adId = split(2).toLong
        val date = DateUtils.formatDate(DateUtils.parseDateKey(datekey))

        AdProvinceTop3(date, province, adId, counts)
      }.toDF

      df.createOrReplaceTempView("tmp_date_province_adId_count")

      val sql =
        s"""
           |SELECT
           |    DATE,province,adid,clickCount,rank
           |FROM
           |    (SELECT
           |        *, row_number () over (
           |            PARTITION BY DATE,province
           |	          ORDER BY clickCount DESC
           |	          ) rank
           |    FROM
           |        tmp_date_province_adId_count) t
           |WHERE rank <= 3
           |distribute BY DATE,province
           |sort BY clickCount DESC
         """.stripMargin
      val sql1 =
        s"""
           |SELECT
           |    DATE,province,adid,clickCount
           |FROM
           |    (SELECT
           |        *, row_number () over (
           |            PARTITION BY DATE,province
           |	          ORDER BY clickCount DESC
           |	          ) rank
           |    FROM
           |        tmp_date_province_adId_count) t
           |WHERE rank <= 3
           |distribute BY DATE,province
           |sort BY clickCount DESC
         """.stripMargin

      //spark.sql(sql).show
      spark.sql(sql1).as[AdProvinceTop3].rdd
    }

    dateProvince2Top3.foreachRDD{rdd=>
      rdd.foreachPartition{ items=>
        val adProvinceTop3List = ArrayBuffer[AdProvinceTop3]()
        for(item <- items){
          adProvinceTop3List += item
        }
        //批量保存到数据库
        AdProvinceTop3DAO.updateBatch(adProvinceTop3List.toArray)
      }
    }


  }

  /**
    * 业务功能二：计算广告点击流量实时统计
    */
  def calculateRealTimeStat(filteredAdRealTimeLogDStream:DStream[(Long, String)]): DStream[(String, Long)] = {
    val dateProvinceCityAdid2count = filteredAdRealTimeLogDStream.map { case (userId, log) =>
      //(userId,    timestamp province city userId adId)
      //（yyyyMMdd_province_city_adid,clickCount）
      val logSplited = log.split(" ")
      // 提取出日期（yyyyMMdd）
      val timestamp = logSplited(0)
      val date = new Date(timestamp.toLong)
      val datekey = DateUtils.formatDateKey(date)

      val province = logSplited(1)
      val city = logSplited(2)
      val adid = logSplited(4)

      (datekey + "_" + province + "_" + city + "_" + adid, 1L)
    }

    // values在这里是所有key 里相同key 的所有的1  stateful
    val aggregatedDStream = dateProvinceCityAdid2count.updateStateByKey[Long](
      (values: Seq[Long], state: Option[Long]) => {
        // 计算当前批次相同key的单词总数
        val currentCount = values.sum
        // 获取上一次保存的单词总数
        val previousCount = state.getOrElse(0L)
        // 返回新的单词总数
        Some(currentCount + previousCount)
      }
    )

    aggregatedDStream.foreachRDD { rdd =>
      rdd.foreachPartition { items =>
        //批量保存到数据库
        val adStats = ArrayBuffer[AdStat]()

        for(item <- items){
          val keySplited = item._1.split("_")
          val date = keySplited(0)
          val province = keySplited(1)
          val city = keySplited(2)
          val adid = keySplited(3).toLong

          val clickCount = item._2
          adStats += AdStat(date,province,city,adid,clickCount)
        }
        AdStatDAO.updateBatch(adStats.toArray)
      }

    }
    aggregatedDStream
  }

  /**
    * 业务功能一：生成动态黑名单
    */
  def generateDynamicBlacklist(filteredAdRealTimeLogDStream: DStream[(Long, String)]) {

    // 计算出每5个秒内的数据中，每天每个用户每个广告的点击量
    // 通过对原始实时日志的处理
    // 将日志的格式处理成<yyyyMMdd_userid_adid, 1L>格式
    val dailyUserAdClickDStream = filteredAdRealTimeLogDStream.map{ case (userid,log) =>
      //             (userId, timestamp province city userId adId)

      // 从tuple中获取到每一条原始的实时日志
      val logSplited = log.split(" ")

      // 提取出日期（yyyyMMdd）、userid、adid
      val timestamp = logSplited(0)
      val date = new Date(timestamp.toLong)
      val datekey = DateUtils.formatDateKey(date)

      val userid = logSplited(3).toLong
      val adid = logSplited(4)

      // 拼接key
      val key = datekey + "_" + userid + "_" + adid
      (key, 1L)
    }

    // 针对处理后的日志格式，执行reduceByKey算子即可，（每个batch中）每天每个用户对每个广告的点击量
    val dailyUserAdClickCountDStream = dailyUserAdClickDStream.reduceByKey(_ + _)

    // 源源不断的，每个5s的batch中，当天每个用户对每支广告的点击次数
    // <yyyyMMdd_userid_adid, clickCount>
    dailyUserAdClickCountDStream.foreachRDD{ rdd =>
      rdd.foreachPartition{ items =>
        // 对每个分区的数据就去获取一次连接对象
        // 每次都是从连接池中获取，而不是每次都创建
        // 写数据库操作，性能已经提到最高了
        val adUserClickCounts = ArrayBuffer[AdUserClickCount]()
        for(item <- items){
          val keySplited = item._1.split("_")
          val date = DateUtils.formatDate(DateUtils.parseDateKey(keySplited(0)))
          // yyyy-MM-dd
          val userid = keySplited(1).toLong
          val adid = keySplited(2).toLong
          val clickCount = item._2

          //批量插入
          adUserClickCounts += AdUserClickCount(date, userid,adid,clickCount)
        }
        AdUserClickCountDAO.updateBatch(adUserClickCounts.toArray)
      }
    }

    /* 待优化 应该先查出来 某天对某个广告的点击量大于100的 用户名单  然后再contains*/

    // 现在我们在mysql里面，已经有了累计的每天各用户对各广告的点击量
    // 遍历每个batch中的所有记录，对每条记录都要去查询一下，这一天这个用户对这个广告的累计点击量是多少
    // 从mysql中查询
    // 查询出来的结果，如果是100，如果你发现某个用户某天对某个广告的点击量已经大于等于100了
    // 那么就判定这个用户就是黑名单用户，就写入mysql的表中，持久化
    val blacklistDStream = dailyUserAdClickCountDStream.filter{ case (key, count) =>
      val keySplited = key.split("_")

      // yyyyMMdd -> yyyy-MM-dd
      val date = DateUtils.formatDate(DateUtils.parseDateKey(keySplited(0)))
      val userid = keySplited(1).toLong
      val adid = keySplited(2).toLong

      // 从mysql中查询指定日期指定用户对指定广告的点击量
      val clickCount = AdUserClickCountDAO.findClickCountByMultiKey(date, userid, adid)

      // 判断，如果点击量大于等于100，ok，那么不好意思，你就是黑名单用户
      // 那么就拉入黑名单，返回true
      if(clickCount >= 9) {
        true
      }else{
        // 反之，如果点击量小于100的，那么就暂时不要管它了
        false
      }
    }

    // blacklistDStream
    // 里面的每个batch，其实就是都是过滤出来的已经在某天对某个广告点击量超过100的用户
    // 遍历这个dstream中的每个rdd，然后将黑名单用户增加到mysql中
    // 这里一旦增加以后，在整个这段程序的前面，会加上根据黑名单动态过滤用户的逻辑
    // 我们可以认为，一旦用户被拉入黑名单之后，以后就不会再出现在这里了
    // 所以直接插入mysql即可

    // 我们在插入前要进行去重
    // yyyyMMdd_userid_adid
    // 20151220_10001_10002 100
    // 20151220_10001_10003 100
    // 10001这个userid就重复了

    // 实际上，是要通过对dstream执行操作，得到其中的userid
    val blacklistUseridDStream: DStream[Long] = blacklistDStream.map(item => item._1.split("_")(1).toLong)
    //对其中的rdd中的userid进行全局的去重, 返回Userid
    val distinctBlacklistUseridDStream: DStream[Long] = blacklistUseridDStream.transform(uidStream => uidStream.distinct() )

    // 到这一步为止，distinctBlacklistUseridDStream
    // 每一个rdd，只包含了userid，而且还进行了全局的去重，保证每一次过滤出来的黑名单用户都没有重复的
    distinctBlacklistUseridDStream.foreachRDD{ rdd =>
      rdd.foreachPartition{ items =>
        val adBlacklists = ArrayBuffer[AdBlacklist]()

        for(item <- items)
          adBlacklists += AdBlacklist(item)

        AdBlacklistDAO.insertBatch(adBlacklists.toArray)
      }
    }
  }

  /**
    * 根据黑名单进行过滤
    * @return
    */
  def filterByBlacklist(spark: SparkSession, adRealTimeValueDStream: DStream[String]): DStream[(Long, String)] = {
    // 刚刚接受到原始的用户点击行为日志之后
    // 根据mysql中的动态黑名单，进行实时的黑名单过滤（黑名单用户的点击行为，直接过滤掉，不要了）
    // 使用transform算子（将dstream中的每个batch RDD进行处理，转换为任意的其他RDD，功能很强大）

    val filteredAdRealTimeLogDStream: DStream[(Long, String)] = adRealTimeValueDStream.transform { consumerValueRDD =>
      // 首先，从mysql中查询所有黑名单用户，将其转换为一个rdd  这个业务一定要放在RDD transformation方法里 否则只调用一次
      val adBlacklists = AdBlacklistDAO.findAll()
      val blacklistRDD: RDD[(Long, Boolean)] = spark.sparkContext.makeRDD(adBlacklists.map(item => (item.userid, true)))
      // 将原始数据rdd映射成<userid, log>>
      val mappedRDD: RDD[(Long, String)] = consumerValueRDD.map(consumerRecord => {
        val userid = consumerRecord.split(" ")(3).toLong
        (userid, consumerRecord)
      })
      // 将原始日志数据rdd，与黑名单rdd，进行左外连接
      // 如果说原始日志的userId，没有在对应的黑名单中，join不到，左外连接
      // 用inner join，内连接，会导致数据丢失
      val joinedRDD = mappedRDD.leftOuterJoin(blacklistRDD)

      val filteredRDD = joinedRDD.filter { case (userid, (log, black)) =>
        // 如果这个值存在，那么说明原始日志中的userid，join到了某个黑名单用户
        if (black.isDefined && black.get) false else true
      }
      val finalRDD: RDD[(Long, String)] = filteredRDD.map { case (userid, (log, black)) => (userid, log) }
      finalRDD
    }//transform over

    filteredAdRealTimeLogDStream
  }

}
