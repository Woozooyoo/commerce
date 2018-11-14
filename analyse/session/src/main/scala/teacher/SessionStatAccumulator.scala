/*
 * Copyright (c) 2018. Atguigu Inc. All Rights Reserved.
 * Date: 18-10-10 下午3:37.
 * Author: Adrian.
 */

package teacher

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/** create time 2018/10/9 10:31 by liuxiaolong */
class SessionStatAccumulator extends AccumulatorV2[String,mutable.HashMap[String,Int]]{

  val countMap= new mutable.HashMap[String,Int]()
  //累加器的数据结构是否为空
  override def isZero: Boolean = countMap.isEmpty

  //把对象复制到累加器
  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = {
    var acc = new SessionStatAccumulator
    acc.countMap ++= this.countMap
    acc
  }

  //重置
  override def reset(): Unit = countMap.clear()

  //添加到累加器
  override def add(v: String): Unit = {
    //不存在,添加
    if(!countMap.contains(v)){
      countMap += (v -> 0)
    }
    //存在,累加
    countMap.update(v,countMap(v)+1)
  }

  //把分区的数据进行累加(局部累加)
  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {
    // (0/:(1 to 100))(_+_)
    // (1 to 100).foldLeft(0)(_+_)
    // (0/:(1 to 100)){case (item,item) => item+item}
    other match {
      case acc:SessionStatAccumulator =>
        //this.countMap : acc.countMap
        // 初始值:this.countMap
        // 迭代对象: acc.countMap
        acc.countMap.foldLeft(this.countMap){
          case (map,(k,v)) => map += (k -> (map.getOrElse(k,0)+v))
        }
    }

  }

  //全局累加
  override def value: mutable.HashMap[String, Int] = {
    this.countMap
  }
}
