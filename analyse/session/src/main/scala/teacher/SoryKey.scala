/*
 * Copyright (c) 2018. Atguigu Inc. All Rights Reserved.
 * Date: 18-10-10 下午3:37.
 * Author: Adrian.
 */

package teacher

/** create time 2018/10/10 11:19 by liuxiaolong */
case class SoryKey(clickCount:Long,orderCount:Long,payCount:Long) extends Ordered[SoryKey]{

  //先点击排序,下单排序,付款排序
  override def compare(that: SoryKey): Int = {
    if(this.clickCount -that.clickCount !=0){
      return (this.clickCount -that.clickCount).toInt
    }else if(this.orderCount - that.orderCount !=0){
      return (this.orderCount - that.orderCount).toInt
    }else{
      return (this.payCount - that.payCount).toInt
    }
  }
}
