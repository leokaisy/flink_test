package com.ng.bigdata._03_flink_sink

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, SplitStream, StreamExecutionEnvironment}

/**
 * @User: kaisy
 * @Date: 2020/10/15 9:30
 * @Desc: split和select拆流处理
 */
object _01DataStraemSplitTest {
  def main(args: Array[String]): Unit = {
    // todo 创建上下文
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // todo 获取数据源
    val dStream: DataStream[String] = env.socketTextStream("spark101", 9999)
    // todo 简单处理：xiaoming，36.8  数据样式
    val words: SplitStream[(String, Double)] = dStream.map(t => (t.split(",")(0), t.split(",")(1).toDouble))
      .split(x => if (x._2 >= 37.2) List("innormal") else List("normal"))
    // tip 很对不同流结果打印不同数据
    words.select("innormal").print("体温异常，需要隔离")
    words.select("normal").print("体温正常")

    // todo 执行
    env.execute("split_")
  }
}














