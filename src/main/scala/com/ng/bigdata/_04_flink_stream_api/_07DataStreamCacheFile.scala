package com.ng.bigdata._04_flink_stream_api


import java.io.File

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.collection.mutable
import scala.io.{BufferedSource, Source}
/**
 * @User: kaisy
 * @Date: 2020/10/16 15:59
 * @Desc: 加载缓存文件
 */
object _07DataStreamCacheFile {
  def main(args: Array[String]): Unit = {
    // todo 创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // todo 读取文件，将其变成缓存文件
    env.registerCachedFile("input/cache","gender")
    // 读取数据流
    env.fromElements("1 bj 1 age20", "2 sz 2 age22", "3 hz 2 age18", "4 hn 1 age23")
      .map(new RichMapFunction[String,(String,String,Char,String)] {
        val map: mutable.Map[Int, Char] = mutable.HashMap[Int,Char]()
        var source: BufferedSource = _
        // todo 初始化读取，只会读取一次
        override def open(parameters: Configuration): Unit = {
          // todo 获取文件状态
          val gender_file: File = getRuntimeContext.getDistributedCache.getFile("gender")
          // todo 用IO流额方式加载数据
          source = Source.fromFile(gender_file)
          for(line <- source.getLines().toList){
            // todo 切分数据
            val arr: Array[String] = line.split(" ")
            // 创建集合保存数据
            map.put(arr(0).toInt,arr(1).toCharArray()(0))
          }
        }

        override def map(value: String): (String, String, Char, String) = {
          // todo 将数据流切分
          val arr: Array[String] = value.split(" ")
          val gender_flag: Int = arr(2).toInt

          // todo 匹配集合
          val c: Char = map.getOrElse(gender_flag, '人')
          (arr(0),arr(1),c,arr(3))
        }

        override def close(): Unit = {
          if(source != null){
            source.close()
          }
        }
      }).print()

    env.execute("list state")
  }
}









