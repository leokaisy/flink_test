package com.ng.bigdata._01_flinck_cases

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.core.fs.FileSystem

/**
 * @User: kaisy
 * @Date: 2020/10/13 10:41
 * @Desc: 初识Flink
 */
object _01Boundsteam {
  def main(args: Array[String]): Unit = {
    // todo 业务需求：单词统计
    // todo 1. 读取数据源
    // tip 执行环境（有界流）
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    // todo
    val params: ParameterTool =  ParameterTool.fromArgs(args)
    val inputPath: String = params.get("inputPath")
    val outputPath: String = params.get("outputPath")


    // todo 2. 后去数据后进行数据计算
    // tip 读取数据
    val ds: DataSet[String] = env.readTextFile(inputPath)
    // tip 数据据计算统计单词
    // tip 注意：当我们使用scalaAPI进行编程开发时，一定要
    import org.apache.flink.api.scala._

    val rs: AggregateDataSet[(String, Int)] = ds.flatMap(_.split(" "))
      .filter(_.trim.nonEmpty)
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    // todo 3. 计算完成后，将结果打印输出到控制台或者保存到本地
    rs.writeAsText(outputPath,FileSystem.WriteMode.NO_OVERWRITE)
    // tip 注意，如果将有界流的数据保存起来，一定要出发执行条件，也就是执行操作
    env.execute("wordcount")

  }
}
