package com.ng.bigdata._03_flink_sink

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}


/**
 * @User: kaisy
 * @Date: 2020/10/15 14:35
 * @Desc: 自定义Redis Sink
 */
object _08DataStreamRedisSink {
  def main(args: Array[String]): Unit = {
    // todo 创建上下文
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // todo 获取数据
    val dStream: DataStream[String] = env.socketTextStream("spark101", 9999)
    // todo 将数据写入到Redis数据库
    // tip 单机连接池配置
    val conf: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
      .setHost("spark101")
      .setPort(6379)
      .setPassword("123")
      .build()
    val sink: RedisSink[(String, String)]= new RedisSink[(String, String)](conf, new MyRedisSink)

    // todo 写入Redis
    dStream.map((_,"1")).addSink(sink)

    // todo 执行
    env.execute("Redis_sink")
  }
}

/**
 * 继承RedisMapper接口，实现抽象方法，写入Redis数据库
 */
class MyRedisSink extends RedisMapper[(String,String)]{
  // todo 设置写入的命令
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.SET,null)
  }
  // 设置写入的Key值
  override def getKeyFromData(data: (String, String)): String = data._1

  // 设置写入的Value值
  override def getValueFromData(data: (String, String)): String = data._2
}













