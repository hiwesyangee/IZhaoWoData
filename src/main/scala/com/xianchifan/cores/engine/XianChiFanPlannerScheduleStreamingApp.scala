package com.xianchifan.cores.engine

import com.izhaowo.cores.utils.JavaSparkUtils
import com.xianchifan.cores.properties.JavaXianChiFanPlannerScheduleProperties
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

/**
  * Xianchifan项目Streaming类
  *
  * @version 1.6
  * @since 2019/07/04 by Hiwes
  */
object XianChiFanPlannerScheduleStreamingApp {

  // 开始流式数据接收
  def runningStreaming(): Unit = {
    // 创建StreamingContext的链接
    val spark = JavaSparkUtils.getInstance().getSparkSession
    import spark.implicits._
    val ssc = new StreamingContext(spark.sparkContext, Durations.seconds(2))
    ssc.checkpoint("hdfs://master:8020/opt/checkpoint/stream1.6")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> JavaXianChiFanPlannerScheduleProperties.BOOTSTARP_SERVERS,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> JavaXianChiFanPlannerScheduleProperties.GROUP,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = JavaXianChiFanPlannerScheduleProperties.StringTOPIC

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
            XianChiFanPlannerScheduleStreamingDataProcessEngine.DBContentsDispose(str)
          } catch {
            case e: Exception => e.printStackTrace()
          }
        })
      })
    })

    try {
      ssc.start()
      ssc.awaitTermination()
    } catch {
      case e: InterruptedException => e.printStackTrace()
    }

  }
}
