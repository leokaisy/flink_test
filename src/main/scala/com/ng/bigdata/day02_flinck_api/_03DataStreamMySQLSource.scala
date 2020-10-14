package com.ng.bigdata.day02_flinck_api

import java.sql.{Connection,DriverManager, PreparedStatement, ResultSet, SQLException}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, RichSourceFunction, SourceFunction}


/**
 * @User: kaisy
 * @Date: 2020/10/14 15:35
 * @Desc: 自定义Source源-MySQL
 */
object _03DataStreamMySQLSource {
  def main(args: Array[String]): Unit = {
    // todo 创建上下文
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // todo 创建自定义Source源
    val dStream: DataStream[Stu] = env.addSource(new MySQLSource)

    // todo 直接输出
    dStream.print()

    // todo 执行
    env.execute("mysql_source")
  }
}

/**
 * 自定义MySQL数据源
 */
class MySQLSource() extends RichSourceFunction[Stu]{
  // todo 创建连接
  var ps:PreparedStatement = _
  var conn:Connection = _
  var rs:ResultSet = _

  /**
   * 初始化连接
   * @param parameters
   */
  override def open(parameters: Configuration): Unit = {
    try {
      Class.forName("com.mysql.jdbc.Driver")
      conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "201212")
      ps = conn.prepareStatement("select * from test1")
    }catch {
      case e:Exception => e.printStackTrace()
    }
  }
  override def run(sourceContext: SourceFunction.SourceContext[Stu]): Unit = {

    try {
      rs = ps.executeQuery()
      while (rs.next()) {
        // tip 封装结果集数据bean
        val stu = new Stu(rs.getInt(1), rs.getString(2))
        sourceContext.collect(stu)
      }
    }catch {
      case e:SQLException => e.printStackTrace()
    }
  }

  override def cancel(): Unit = {  }

  override def close(): Unit = {
    if(rs != null) {
      rs.close()
    }
    if(ps != null) {
      ps.close()
    }
    if(conn != null) {
      conn.close()
    }
  }
}

class Stu(id:Int,name:String){
  override def toString: String = id +" : "+name
}











