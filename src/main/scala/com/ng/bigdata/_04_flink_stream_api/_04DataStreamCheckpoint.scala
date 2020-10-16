package com.ng.bigdata._04_flink_stream_api

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
/**
 * @User: kaisy
 * @Date: 2020/10/16 11:37
 * @Desc: 检查点机制(快照|缓存)
 */
object _04DataStreamCheckpoint {
  def main(args: Array[String]): Unit = {
    // todo 创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // todo 设置内部检查点
    env.setStateBackend(new MemoryStateBackend())

    // todo 将数据写入到本地或者HDFS上
    // tip 嵌入式数据库
    env.setStateBackend(new RocksDBStateBackend("file:///D:\\output/checkpoint",true))
    // tip HDFS中
    env.setStateBackend(new FsStateBackend("hdfs://spark101:8020/flink_check"))


    // todo 获取检查点配置对象，进行检查点配置
    val config: CheckpointConfig = env.getCheckpointConfig
    // todo 设置检查点周期 3s
    config.setCheckpointInterval(3000)
    // todo 设置检查点保存模式
    config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // todo 设置任务取消或者故障时保存数据，如果这个流数据真的发生故障，那么可以将这次故障数据暂时保存到检查点
    // tip RETAIN_ON_CANCELLATION 如果作业取消保存检查点数据，这样你就必须手动清理
    // tip DELETE_ON_CANCELLATION 如果作业取消，检查点不会保留，自动删除
    config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    // todo 设置检查点写入超时时间
    config.setCheckpointTimeout(60000)
    // todo 设置同一时间只允许进行一个检查点写入
    config.setMaxConcurrentCheckpoints(1)

    // 创建数据流
    val dStream: DataStream[String] = env.fromElements("aa bb cc dd")
    dStream.flatMap(_.split(" "))
        .map((_,1)).keyBy(0).sum(1)
        .print()

    // 执行环境
    env.execute("checkpoint")

  }
}
