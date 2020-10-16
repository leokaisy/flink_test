package com.ng.bigdata._03_flink_sink

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

/**
 * @User: kaisy
 * @Date: 2020/10/15 15:32
 * @Desc: 批次source 基于文件
 */
object _09DataSetSource {
  def main(args: Array[String]): Unit = {
    // todo 创建上下文
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    // todo 读取数据
    val ds: DataSet[String] = env.readTextFile("input/test.txt")
    // todo 输出数据
    ds.print()
    // todo 读取csv、集合
    val csvDs: DataSet[(Int,String)] = env.readCsvFile("input/2019.csv")
    csvDs.print()
    val collDs: DataSet[String] = env.fromElements("aa", "bb", "cc")
    collDs.print()


  }
}
