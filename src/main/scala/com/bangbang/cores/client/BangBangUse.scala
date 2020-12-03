package com.bangbang.cores.client

import com.bangbang.cores.engine.BangBangEngine
import com.izhaowo.cores.utils.JavaDateUtils
import org.apache.log4j.{Level, Logger}

/**
  *
  */
object BangBangUse {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hbase").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.kafka").setLevel(Level.WARN)
    val start = System.currentTimeMillis
    val today = JavaDateUtils.stamp2DateYMD(String.valueOf(start))

    // 开始记录任务时长
    BangBangEngine.readAndWriteDemandSupplyData2SQLServer()
    //    BangBangEngine.savePlannnerDemand2SQLServer()

    val stop = System.currentTimeMillis
    System.out.println(today + " 每日定时任务——读写策划师供给需求统计结果表,用时: " + (stop - start) / 1000 + "s.")
  }
}
