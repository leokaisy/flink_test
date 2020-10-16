package com.ng.bigdata._03_flink_sink

import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment

/**
 * @User: kaisy
 * @Date: 2020/10/15 15:45
 * @Desc: DataSet算子操作
 */
object _10DataSetAPI {
  def main(args: Array[String]): Unit = {
    // todo 创建上下文
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    // todo 读取数据
    val lines: DataSet[String] = env.fromCollection(List("zhangsan", "lisi", "wangwu", "zhangsan"))
    // todo map
    lines.map((_,1)).print()
    // todo flatMap
    lines.flatMap(_.split(" ")).print()
    // todo mapPartition
    lines.mapPartition(t=> t.map((_,1))).print()
    // todo filter
    lines.filter(_.length>=2).print()
    // todo distinct
    lines.distinct().print()
    // todo groupBy
    lines.map((_,1)).groupBy(0).aggregate(Aggregations.SUM,1).print()
    // todo 排序
    lines.map((_,1)).groupBy(0).sortGroup(1,Order.DESCENDING).sum(1).print()
    // todo join
    val line1: DataSet[(String, Int)] = env.fromElements(("a", 1), ("b", 1), ("c", 1), ("d", 1), ("e", 1))
    val line2: DataSet[(String, Int)] = env.fromElements(("a", 1), ("b", 1), ("c", 1), ("d", 1), ("e", 1))
    line1.join(line2).where(0).equalTo(0).print()
    // todo union 合并数据集
    line1.union(line2).print()
    // todo 笛卡尔积
    line1.cross(line2)
    // todo 分区 HashPartition分区器，对key的Hash进行数据分区
    lines.rebalance().partitionByHash(0).print()
    // tip RangePartition，范围分区，一般会在排序时使用
    lines.rebalance().partitionByRange(0).print()
    // tip 自定义分区
    lines.rebalance().partitionCustom(new Mytitioner,0).print()
    // tip 排序分区
    lines.sortPartition(0,Order.DESCENDING)
    // tip task = first
    lines.first(2).print()

  }
}

class Mytitioner extends Partitioner[String] {
  override def partition(k: String, i: Int): Int = {
    1
  }
}











