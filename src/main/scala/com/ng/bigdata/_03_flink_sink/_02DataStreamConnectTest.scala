package com.ng.bigdata._03_flink_sink

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, SplitStream, StreamExecutionEnvironment}

/**
 * @User: kaisy
 * @Date: 2020/10/15 9:46
 * @Desc: 合并流 union、connect
 */
object _02DataStreamConnectTest {
  def main(args: Array[String]): Unit = {
    // todo 创建上下文
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // todo 获取数据流
    val dStream: DataStream[String] = env.socketTextStream("spark101", 9999)
    // todo 简单处理：xiaoming，36.8  数据样式
    val words: SplitStream[(String, Double)] = dStream.map(t => (t.split(",")(0), t.split(",")(1).toDouble))
      // tip 按照条件拆分两个不同结果数据
      .split(x => if (x._2 >= 37.2) List("innormal") else List("normal"))
    // tip 很对不同流结果打印不同数据
    val stream1: DataStream[(String, Double)] = words.select("innormal")
    val stream2: DataStream[(String, Double)] = words.select("normal")

    // todo 将两个流合并 union
    stream1.union(stream2).print("合并结果：")
    // todo 将两个流合并 connect  不能直接打印输出  将两个流变成两个函数，需要再次处理后，才能输出
    stream1.connect(stream2).map(
      func1 => func1+"function1",
      func2 => func2+"function2"
    ).print("")
    // todo 执行
    env.execute("union")

  }
}









