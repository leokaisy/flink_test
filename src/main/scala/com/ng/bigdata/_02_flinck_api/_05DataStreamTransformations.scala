package com.ng.bigdata._02_flinck_api
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * @User: kaisy
 * @Date: 2020/10/14 16:54
 * @Desc:
 */
object _05DataStreamTransformations {
  def main(args: Array[String]): Unit = {
    // todo 创建上下文
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // todo 读取数据
    val dStream: DataStream[String] = env.socketTextStream("spark101", 9999)
    // todo 处理流数据（算子转换操作）
    dStream.flatMap(_.split("　"))
        .filter(_.length>3)
        .map((_,1))
        .keyBy(0)
//        .sum(1)
        .reduce((x,y)=>(x._1,x._2+y._2))
        .print()
        .setParallelism(1)

    // todo 执行
    env.execute()


  }
}










