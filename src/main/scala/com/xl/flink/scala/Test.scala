package com.xl.flink.scala

import java.sql.DriverManager

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.types.Row

/**
 *
 * @author 夏龙
 * @date 2020-09-18
 */
object Test {
  def main(args: Array[String]): Unit = {
    //获取flink流式运行环境
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    //设置最少处理一次和恰好一次
    env.enableCheckpointing(2000,CheckpointingMode.AT_LEAST_ONCE)
    //设置checkpoint位置
    env.setStateBackend(new FsStateBackend("file:///C:\\Users\\xialong\\Desktop\\2020-08-25Scala开始\\flink\\flinkoutput"))
    //设置checkpoint清楚策略
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    val envm=ExecutionEnvironment.getExecutionEnvironment
    val mysqlData:DataSet[Row]=jdbcRead(envm)

    mysqlData.print()

  }



  def jdbcRead(env: ExecutionEnvironment):DataSet[Row] ={
    //添加隐式转换
    import org.apache.flink.api.scala._
    val inputMysql: DataSet[Row] = env.createInput(JDBCInputFormat.buildJDBCInputFormat()
      //    .指定驱动名称
      .setDrivername("com.mysql.cj.jdbc.Driver")
      //      url
      .setDBUrl("jdbc:mysql://localhost:3306/test?characterEncoding=utf-8&serverTimezone=UTC")
      .setUsername("root")
      .setPassword("145112")
      .setQuery("select ip,level from importantip")
      .setRowTypeInfo(new RowTypeInfo
      (BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO))
      .finish()
    )
    inputMysql
  }
}
