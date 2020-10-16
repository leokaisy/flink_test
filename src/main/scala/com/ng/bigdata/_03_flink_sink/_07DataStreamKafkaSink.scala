package com.ng.bigdata._03_flink_sink

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
/**
 * @User: kaisy
 * @Date: 2020/10/15 14:07
 * @Desc: 自定义Kafka Sink
 */
object _07DataStreamKafkaSink {
  def main(args: Array[String]): Unit = {
    // todo 创建上下文
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // todo 获取数据
    val dStream: DataStream[String] = env.socketTextStream("spark101", 9999)



  }
}







