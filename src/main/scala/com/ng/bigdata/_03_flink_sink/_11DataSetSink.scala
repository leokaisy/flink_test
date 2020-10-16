package com.ng.bigdata._03_flink_sink

import java.util

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
 * @User: kaisy
 * @Date: 2020/10/15 16:38
 * @Desc:
 */
object _11DataSetSink {
  def main(args: Array[String]): Unit = {
    // todo 创建上下文
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    // todo 读取数据
    val collDs: DataSet[(String,Int)] = env.fromElements(("aa",1), ("bb",1), ("cc",1))
    // todo 写入文件 textFile
    collDs.writeAsText("output/ds_sink")
    // todo csv文件
    collDs.writeAsCsv("output/csv","\n",",",WriteMode.NO_OVERWRITE)
    // todo 自定义文件写入，需要去继承OutputFormat接口


    // todo 执行
    env.execute("ds_write")
  }
}



















