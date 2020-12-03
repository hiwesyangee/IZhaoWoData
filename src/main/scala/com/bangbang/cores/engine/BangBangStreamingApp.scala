package com.bangbang.cores.engine

import com.bangbang.cores.client.SandTimer
import com.bangbang.cores.properties.JavaBangBangProperties
import com.izhaowo.cores.utils.JavaSparkUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

/**
  * 爱找我————棒棒StreamingApp应用类
  *
  * @version 1.4
  * @since 2019/06/18 by Hiwes
  */
object BangBangStreamingApp {

  // 开始流式数据接收
  def runningStreaming(): Unit = {
    // 创建StreamingContext的链接
    val spark = JavaSparkUtils.getInstance().getSparkSession
    import spark.implicits._
    val ssc = new StreamingContext(spark.sparkContext, Durations.seconds(2))
    ssc.checkpoint("hdfs://master:8020/opt/checkpoint/stream1.5")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> JavaBangBangProperties.BOOTSTARP_SERVERS,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> JavaBangBangProperties.GROUP,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "request.required.acks" -> "1"
    )

    val topics = JavaBangBangProperties.StringTOPIC

    // 创建KafkaStream
    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val lines: DStream[String] = kafkaStream.map(record => record.value())

    /** 测试打印输入结果 */
    lines.foreachRDD(rdd => {
      rdd.foreachPartition(ite => {
        ite.foreach(str => {
          try {
            // 针对收录的数据进行处理
            BangBangStreamingDataProcessEngine.DBContentsDispose(str)
          } catch {
            case e: Exception => e.printStackTrace()
          }
        })
      })
    })

    // 定义时间Timer任务
    //    UpdateRecResult.timeMaker()
    //    new SandTimer().timerRun()

    try {
      ssc.start()
      ssc.awaitTermination()
    } catch {
      case e: InterruptedException => e.printStackTrace()
    }

  }


  def main(args: Array[String]): Unit = {
    runningStreaming()
  }
}
