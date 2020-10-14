package com.ng.bigdata.day02_flinck_api

import java.util.Properties

import org.apache.flink.api.common.serialization.{DeserializationSchema, SimpleStringSchema}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
 * @User: kaisy
 * @Date: 2020/10/14 16:26
 * @Desc: 自定义Source-Kafka
 */
object _04DataStreamKafkaSource {
  def main(args: Array[String]): Unit = {
    // todo 创建上下文
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // todo 创建反序列化类型
    val schema: DeserializationSchema[String] = new SimpleStringSchema()
    // todo 加载Kafka配置文件
    val prop = new Properties()
    prop.load(this.getClass.getClassLoader.getResourceAsStream("consumer.properties"))

    // todo 获取KafkaSource
    val kafkaStream = new FlinkKafkaConsumer[String]("flink-kafka", schema, prop)

    // todo 获取流数据对象
    val dStream: DataStream[String] = env.addSource(kafkaStream)
    // todo 打印
    dStream.print()

    // 执行 todo
    env.execute("kafka-source")
  }
}












