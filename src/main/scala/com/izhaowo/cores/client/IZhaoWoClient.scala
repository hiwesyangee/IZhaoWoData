package com.izhaowo.cores.client

import com.izhaowo.cores.engine.XianChiFanStreamingApp
import com.izhaowo.cores.utils.JavaHBaseUtils
import org.apache.log4j.{Level, Logger}

/**
  * IZhaoWoData项目client类
  *
  * @version 2.0
  * @since 2019/06/17 by Hiwes
  */
object IZhaoWoClient {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hbase").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.kafka").setLevel(Level.WARN)

    // 触发业务逻辑类
    XianChiFanStreamingApp.runningStreaming()
  }
}
