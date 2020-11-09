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
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
/**
 *
 * @author 夏龙
 * @date 2020-09-17
 */
object FlinkRichMapFunction {
  case class Logtest(startDate:String, ip:String, time:String, html:String,endDate:String)
  def main(args: Array[String]): Unit = {
    //获取flink流式运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置最少处理一次语义和恰好一次语义
    env.enableCheckpointing(20000, CheckpointingMode.AT_LEAST_ONCE)
    //checkpointing可以分开设置
//    env.enableCheckpointing(20000)
//    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
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
    //创建flink Kafka消费者
    val kafkaConsumer = new FlinkKafkaConsumer(kafkaConfig._1, new SimpleStringSchema(), kafkaConfig._2)
      .setStartFromLatest()

    import org.apache.flink.api.scala._
    val stream:DataStream[String] = env
      .addSource(kafkaConsumer)
      //设置flink并行度
      .setParallelism(3)

    val dataAddTime= stream.map(new RichTest)

    //
    val data_sc=dataAddTime.filter(_.ip!="0").map(line=>(line.ip,1)).keyBy(0).timeWindow(Time.seconds(10)).sum(1)



    data_sc.print()




    //启动执行
    env.execute("RichTest")
  }

  /**
   * RichMapFunction富函数
   * open、close函数可在map前后实现
   * RichMapFunction[in,out]
//   */
    class RichTest extends RichMapFunction[String,Logtest]{
      //数据处理前时间
      var startTime: Long = _
      override def open(parameters: Configuration): Unit = {
        startTime = System.currentTimeMillis()
      }
      //数据处理
      override def map(in: String): Logtest = {
        // 每条记录的处理时间
        try {
          val sp = in.split("\t")
          val ip = sp(0)
          val time = sp(1)
          if (sp.length < 3) {
            Logtest(s"$startTime", "0", "0", "0", System.currentTimeMillis().toString)
          } else {
            val html = sp(2)
              .split("/")(2)
            var html_yx = "0"
            if (html.contains(".html")) {
              html_yx = html.substring(0, 8)
            }
            Logtest(s"$startTime", ip, time, html_yx, System.currentTimeMillis().toString)
          }
        } catch {
          case e: Exception => {
            e.printStackTrace()
            Logtest(s"$startTime", "0", "0", "0", System.currentTimeMillis().toString)
          }
        }


      }

      override def close(): Unit = {}
    }



}