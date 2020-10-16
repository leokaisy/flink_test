package com.ng.bigdata._03_flink_sink

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * @User: kaisy
 * @Date: 2020/10/15 10:55
 * @Desc: 自定义SInk，将结果写入MySQL中
 */
object _05DataStreamMySink {
  def main(args: Array[String]): Unit = {
    // todo 创建上下文
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // todo 获取数据
    val dStream: DataStream[String] = env.socketTextStream("spark101", 9999)
    // todo 输出处理
    dStream.map(t=>{
      val arr: Array[String] = t.split(" ")
      (arr(0)+"-"+arr(1),(arr(3).toInt,arr(3).toInt))
    })
      .keyBy(0)
      .reduce((x,y)=>(x._1,(x._2._1+y._2._1,x._2._2+y._2._2)))
      .map(x=>(x._1.split("-")(0),x._1.split("-")(1),x._2._1,x._2._2))
    // todo 写入数据
      .writeUsingOutputFormat(new MySqlOutputFormat)

    // todo 执行
    env.execute("")

  }
}

class MySqlOutputFormat extends OutputFormat[(String,String,Int,Int)]{
  // todo 创建连接
  var ps:PreparedStatement = _
  var conn:Connection = _
  var rs:ResultSet = _

  // todo 加载配置信息
  override def configure(conf: Configuration): Unit = {


  }
  // todo 初始化操作
  override def open(taskNumber: Int, numTasks: Int): Unit = {
    try {
      Class.forName("com.mysql.jdbc.Driver")
      conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "201212")
    }catch {
      case e:Exception => e.printStackTrace()
    }
  }

  // todo 写入数据
  override def writeRecord(record: (String,String,Int,Int)): Unit = {
    ps = conn.prepareStatement("insert into yq(dt,province,adds,possibles) values(?,?,?,?)")
    // tip 插入数据
    ps.setString(1,record._1)
    ps.setString(2,record._2)
    ps.setInt(3,record._3)
    ps.setInt(4,record._4)


  }

  // todo 关闭连接
  override def close(): Unit = {

  }
}






