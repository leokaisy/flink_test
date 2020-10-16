package com.ng.bigdata._04_flink_stream_api

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
/**
 * @User: kaisy
 * @Date: 2020/10/16 9:41
 * @Desc: 设置并行度
 */
object _01DataStreamParallel {
  def main(args: Array[String]): Unit = {
    /**
     * todo 参数设置并行度，默认为1，可以进行参数修改
     * 启动命令设置并行度，可以指定-p 这个参数，修改器初识的配置并行度
     */
    // todo 创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // todo 设置并行度，针对于整个运行环境开始进行设置
    env.setParallelism(2)

    // todo 获取数据后，进行设置并行度
    val dStream: DataStream[Int] = env.fromElements(1, 2, 3)
    dStream.map((_,1)).filter(_ != null)
      .keyBy(0)
      .sum(1)
      .setParallelism(3) // tip 算子修改并行度

    //todo 设置最大并行度
    dStream.setMaxParallelism(5)

    // todo 结果输出设置并行度

  }
}
