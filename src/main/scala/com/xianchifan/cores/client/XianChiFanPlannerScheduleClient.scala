package com.xianchifan.cores.client

import com.xianchifan.cores.engine.XianChiFanPlannerScheduleStreamingApp
import org.apache.log4j.{Level, Logger}

/**
  * Xianchifan项目client类
  *
  * @version 1.6
  * @since 2019/07/04 by Hiwes
  */
object XianChiFanPlannerScheduleClient {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hbase").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.kafka").setLevel(Level.WARN)

    // 触发业务逻辑类
    XianChiFanPlannerScheduleStreamingApp.runningStreaming()
  }
}
