package com.ng.bigdata._04_flink_stream_api

import org.apache.flink.api.common.functions.{FlatMapFunction, RichFlatMapFunction}
import org.apache.flink.api.common.state.{State, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
/**
 * @User: kaisy
 * @Date: 2020/10/16 10:35
 * @Desc: 状态（）存储数据操作
 */
object _03ValueStateOperation {
  def main(args: Array[String]): Unit = {
    // todo 创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // todo 获取数据流
    val dStream: DataStream[(Int, Int)] = env.fromElements((1, 1), (2, 2), (1, 1), (3, 1), (3, 2))
    dStream.keyBy(0)
      .flatMap(new MyFMFunction)
      .print("flatMpa:")

    // todo 执行环境
    env.execute()
  }
}

/**
 * 自定义状态管理State
 */
class MyFMFunction extends RichFlatMapFunction[(Int,Int),(Int,Int)]{
  // todo 初始化状态
  // 要的返回值是sum，也就是让flarMap函数额内部状态改变，进行聚合操作
  var sum:ValueState[(Int,Int)] = _

  // todo 初始化状态描述器   open 用于初始化操作
  override def open(parameters: Configuration): Unit = {
    // todo 初始化描述器，将数据初始化，
    val average = new ValueStateDescriptor[(Int, Int)](
      "average",
      TypeInformation.of(new TypeHint[(Int, Int)] {}),
      (0, 0))

    // todo 先将历史的state变成求和的操作
    sum = getRuntimeContext.getState(average)
  }

  // todo 重写flatMap方法，进行状态管理 进行求和计算
  override def flatMap(value: (Int, Int), out: Collector[(Int, Int)]): Unit = {
    // todo 获取当前的状态
    val tupSum: (Int, Int) = sum.value()
    // todo 将状态累加
    val count: Int = tupSum._1 + 1
    // todo 求sum
    val sumed: Int = tupSum._2 + value._2
    // todo 更新状态
    sum.update(count,sumed)
    // todo 将状态结果返回
    out.collect(value._1,sumed)
  }
}