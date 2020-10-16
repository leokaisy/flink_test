package com.ng.bigdata._04_flink_stream_api

import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.util.Collector
/**
 * @User: kaisy
 * @Date: 2020/10/16 15:29
 * @Desc: 广播变量使用
 *       1. 获取数据
 *       2. 加载广播变量
 *       3. 获取广播变量
 */
object _06DataStreamBroadcast {
  def main(args: Array[String]): Unit = {
    // todo 创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // todo 获取广播数据
    val broadcast: DataStream[(Int, Char)] = env.fromElements((1, '男'), (2, '女'))
    // todo 获取流数据
    val dStream: DataStream[String] = env.fromElements("1 bj 1 age20", "2 sz 2 age22", "3 hz 2 age18", "4 hn 1 age23")

    // todo 处理数据
    val ds2: DataStream[(String, String, Int, String)] = dStream.map(line => {
      val arr: Array[String] = line.split(" ")
      (arr(0).trim, arr(1).trim, arr(2).trim.toInt, arr(3).trim)
    })


    // todo 设置MapState
    val gender = new MapStateDescriptor[Integer, Character](
      "gender",
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.CHAR_TYPE_INFO)
    // todo 将数据广播
    val bc: BroadcastStream[(Int, Char)] = broadcast.broadcast(gender)
    ds2.connect(bc)  // tip 合并流
      .process(new BroadcastProcessFunction[(String,String,Int,String),
        (Int,Char),(String,String,Char,String)]{
        override def processElement(// tip 数据类型
                                     value: (String, String, Int, String),
                                   // tip 广播数据类型
                                    ctx: BroadcastProcessFunction[(String, String, Int, String),
                                      (Int, Char), (String, String, Char, String)]#ReadOnlyContext,
                                   // tip 输出数据类型
                                    out: Collector[(String, String, Char, String)]): Unit = {
          // tip 获取性别
          val gender_flag: Int = value._3
          // tip 拿到广播变量数据，然后匹配数据
          var gender_value: Character = ctx.getBroadcastState(gender).get(gender_flag)
          // tip 判断是否获取到值
          if(gender_value == null){
            gender_value = '泰'
          }
          // tip 返回输出数据
          out.collect((value._1,value._2,gender_value,value._4))
        }

        override def processBroadcastElement(value: (Int, Char),
                                             ctx: BroadcastProcessFunction[(String, String, Int, String),
                                               (Int, Char),
                                               (String, String, Char, String)]#Context,
                                             out: Collector[(String, String, Char, String)]): Unit = {
          // tip 更新状态的广播变量
          ctx.getBroadcastState(gender).put(value._1,value._2)
        }
      }).print()

    // todo 执行
    env.execute("bro_test")
  }
}








