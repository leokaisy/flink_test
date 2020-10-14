package com.ng.bigdata.day02_flinck_api

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}

import scala.util.Random
/**
 * @User: kaisy
 * @Date: 2020/10/14 14:54
 * @Desc: 自定义Source
 *       为了实现Flink整个多种Source源，那么我们需要自定义一些数据源，如MySQL、kafka等
 */
object _02DataStreamSource {
  def main(args: Array[String]): Unit = {
    // todo 创建上下文
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // todo 添加自定义数据源，要继承SourceFunction
//    val dStream: DataStream[String] = env.addSource(new SourceFunction[String] {
    val dStream: DataStream[String] = env.addSource(new ParallelSourceFunction[String] {
        override def run(sourceContext: SourceFunction.SourceContext[String]): Unit = {
        // tip 随机产生数据
        val random = new Random()
        // tip 无限循环输出数据
        while(true) {
          val i: Int = random.nextInt(10)
          sourceContext.collect("随机数："+i)
          // tip 线程休眠
          Thread.sleep(1000)
        }
      }

      override def cancel(): Unit = ???
    })
    dStream.print().setParallelism(1)

    // todo 执行
    env.execute("random_source")

  }
}













