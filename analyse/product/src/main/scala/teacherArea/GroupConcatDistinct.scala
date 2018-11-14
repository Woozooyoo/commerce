package teacherArea

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

class GroupConcatDistinct extends UserDefinedAggregateFunction{
  //输入参数类型:string
  override def inputSchema: StructType = StructType(StructField("city_info",StringType)::Nil)

  //缓冲区的数据类型
  override def bufferSchema: StructType = StructType(StructField("buffer_city_info",StringType)::Nil)

  //返回值数据类型
  override def dataType: DataType = StringType

  //对于相同的输入返回相同的输出
  override def deterministic: Boolean = true
  //初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0)=""
  }
  //在同一个Excutor中的值更新
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    var bufferCityInfo = buffer(0).toString
    val inputcityInfo = input.getString(0)
    if(!bufferCityInfo.contains(inputcityInfo)){
      if("".equals(bufferCityInfo)){
        bufferCityInfo += inputcityInfo
      }else{
        bufferCityInfo += ","+inputcityInfo
      }
    }

    buffer.update(0,bufferCityInfo)
  }
  //不同Excutor的数据更新
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //c=v ,c=v, ...
    var bufferInfo1 = buffer1(0).toString
    var bufferInfo2 = buffer2(0).toString
    //把缓冲区的数据取出,并切割
    for(cityInfo <- bufferInfo2.split(",")){
      //去重
      if(!bufferInfo1.contains(bufferInfo2)){
        if("".equals(bufferInfo1)){
          bufferInfo1 +=cityInfo
        }else{
          bufferInfo1 +=","+cityInfo
        }
      }
    }
    buffer1.update(0,bufferInfo1)
  }
  //计算最终结果
  override def evaluate(buffer: Row): Any = {
    buffer(0).toString
  }
}
