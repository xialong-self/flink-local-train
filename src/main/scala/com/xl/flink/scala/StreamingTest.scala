package com.xl.flink.scala

import java.sql.DriverManager
import java.util.{Date, Properties}

import com.lp.scala.demo.datastream.trigger.CustomProcessTimeTrigger
import com.lp.scala.demo.utils.ConfigUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig


/**
 *
 * @author 夏龙
 * @date 2020-09-16
 */

object StreamingTest {
  case class Logtest( ip:String, time:String, html:String)
  def main(args: Array[String]): Unit = {
    //获取flink流式运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置最少处理一次语义和恰好一次语义
    env.enableCheckpointing(20000, CheckpointingMode.AT_LEAST_ONCE)
    //checkpointing可以分开设置
    env.enableCheckpointing(20000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    //设置checkpoint目录 state存储路径
    env.setStateBackend(new FsStateBackend("file:///C:\\Users\\xialong\\Desktop\\2020-08-25Scala开始\\flink\\flinkoutput"));
    //设置checkpoint的清除策略
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    /**
     * 设置重启策略/5次尝试/每次重试间隔50s
     */
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 50000))

    //设置flink以身为时间为基准作，处理事件
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val kafkaConfig = ConfigUtils.apply("string")

    val kafkaConsumer = new FlinkKafkaConsumer(kafkaConfig._1, new SimpleStringSchema(), kafkaConfig._2)
      .setStartFromLatest()

    import org.apache.flink.api.scala._

    val stream:DataStream[String] = env
      .addSource(kafkaConsumer)
      //设置flink并行度
      .setParallelism(3)


    val data=stream.map(mp=> {
      val sp=mp.split("\t")
      val ip=sp(0)
      val time=sp(1)
      if(sp.length<3){
        Logtest(ip, time, "0")
      }else {
        val html = sp(2)
          .split("/")(2)
        var html_yx="0"
          if(html.contains(".html")){
             html_yx=html.substring(0,8)
          }
        Logtest(ip, time, html_yx)
      }
    })


    //分组求和
    val data_sc=data.map(line=>(line.ip,1)).keyBy(0).sum(1)
    data_sc.print()



//    val text = stream.flatMap{ _.toLowerCase().split("\\W+")filter { _.nonEmpty} }
//      .map{(_,1)}
//      .keyBy(0)
//      .timeWindow(Time.seconds(5))
//      .sum(1)
//    text.print()

    //启动执行
    env.execute("kafkawd")
  }


}