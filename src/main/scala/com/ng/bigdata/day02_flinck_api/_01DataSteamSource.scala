package com.ng.bigdata.day02_flinck_api

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
/**
 * @User: kaisy
 * @Date: 2020/10/14 14:30
 * @Desc: 流式API 数据源
 */
object _01DataSteamSource {
  def main(args: Array[String]): Unit = {
    // todo 创建程序上下文
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    println("-------------------集合------------------")
    // todo 通过集合创建数据流
    val list = Seq(1,2,3,4,5,6,7,8,9)
    val dStream: DataStream[Int] = env.fromCollection(list)
    // todo 打印输出
    dStream.print().setParallelism(1)

    println("-------------------元素------------------")

    // todo 各种类型的多个元素创建
    val dStream2: DataStream[String] = env.fromElements("spark flink scala")
    // todo 输出
    dStream2.print().setParallelism(1)

    println("-------------------文件------------------")
    // tip 读取txt文件
    val dStream3: DataStream[String] = env.readTextFile("input/test.txt")
    dStream3.print().setParallelism(1)

    // tip csv
    val dStream4: DataStream[String] = env.readTextFile("input/2019.csv")
    dStream4.print().setParallelism(1)

    // tip json
    val dStream5: DataStream[String] = env.readTextFile("input/rating.json")
    dStream5.print().setParallelism(1)

    // todo 执行
    env.execute("dataSource")
  }
}










