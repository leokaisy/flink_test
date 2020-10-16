package com.ng.bigdata._03_flink_sink

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * @User: kaisy
 * @Date: 2020/10/15 10:42
 * @Desc: 数据 Sink API
 */
object _04DataStreamSinkTest {
  def main(args: Array[String]): Unit = {
    // todo 创建上下文
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // todo 获取数据源
    val dStream: DataStream[(Int, Int)] = env.fromElements((1,2),(2,4),(3,6),(4,8),(5,10))
    // todo 将数据存储
    dStream.writeAsText("output/s1")
    dStream.writeAsCsv("output/s2")

    // todo 执行
    env.execute("sink")

  }
}











