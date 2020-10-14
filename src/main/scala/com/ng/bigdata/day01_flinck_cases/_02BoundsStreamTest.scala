package com.ng.bigdata.day01_flinck_cases

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @User: kaisy
 * @Date: 2020/10/13 11:33
 * @Desc: 初识Flink 无界流
 */
object _02BoundsStreamTest {
  def main(args: Array[String]): Unit = {
    // todo 创建无界流对象
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // todo 读取数据
    val stream: DataStream[String] = env.socketTextStream("spark101", 9999)
    // todo 对流式数据进行转换计算
    import org.apache.flink.api.scala._

    stream.flatMap(_.split(" "))
      .map(WC(_,1))
      .keyBy("word")
      // tip 类似于SparkStreaming中的滑动窗口操作，滑动时间5s，滑动距离5s
      // tip 在无界流上是此案批次结果的提交操作
      .timeWindow(Time.seconds(5),Time.seconds(5))
      .sum("count")
      .print()

    // todo 环境执行操作
    env.execute("unbounded wc")
  }
}


case class WC(word:String,count:Int)




