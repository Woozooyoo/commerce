/*
 * Copyright (c) 2018. Atguigu Inc. All Rights Reserved.
 * Date: 18-10-12 下午7:02.
 * Author: Adrian.
 */

package teacherArea

case class CityClickProduct(city_id:Long,
                            click_product_id:Long)

case class CityAreaInfo(city_id:Long,
                          city_name:String,
                          area:String)

//***************** 输出表 *********************

/**
  *
  * @param taskid
  * @param area
  * @param areaLevel
  * @param productid
  * @param cityInfos
  * @param clickCount
  * @param productName
  * @param productStatus
  */
case class AreaTop3Product(taskid:String,
                           area:String,
                           areaLevel:String,
                           productid:Long,
                           cityInfos:String,
                           clickCount:Long,
                           productName:String,
                           productStatus:String)