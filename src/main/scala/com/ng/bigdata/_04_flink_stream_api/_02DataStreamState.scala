package com.ng.bigdata._04_flink_stream_api

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
/**
 * @User: kaisy
 * @Date: 2020/10/16 10:26
 * @Desc:
 */
object _02DataStreamState {
  def main(args: Array[String]): Unit = {
    // todo 创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // todo 创建dStream
    val dStream: KeyedStream[(Int, Int), Tuple] = env.fromElements((1, 11), (2, 22), (1, 33)).keyBy(0)

    env.fromElements(1,2,3).map((_,1)).filter(_ != null)


  }
}













