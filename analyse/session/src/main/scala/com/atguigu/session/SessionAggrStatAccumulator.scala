package com.atguigu.session

import org.apache.spark.util.AccumulatorV2
import scala.collection.mutable

/**
  * 自定义累加器
  */
class SessionAggrStatAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Int]] {

  // 保存所有聚合数据
  private val aggrStatMap = mutable.HashMap[String, Int]()

  override def isZero: Boolean = {
    aggrStatMap.isEmpty
  }

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = {
    val newAcc = new SessionAggrStatAccumulator
    aggrStatMap.synchronized{
      newAcc.aggrStatMap ++= this.aggrStatMap
    }
    newAcc
  }

  override def reset(): Unit = {
    aggrStatMap.clear()
  }

  override def add(k: String): Unit = {
    if (!aggrStatMap.contains(k))
      aggrStatMap += (k -> 0)
    aggrStatMap.update(k, aggrStatMap(k) + 1)
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {
    other match {
      case acc:SessionAggrStatAccumulator => {
        (this.aggrStatMap /: acc.value){ case (map, (k,v)) => map += ( k -> (v + map.getOrElse(k, 0)) )}
      }
    }
  }

  override def value: mutable.HashMap[String, Int] = {
    this.aggrStatMap
  }
}
