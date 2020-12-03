package com.realleaderAndyanjing.client

import com.izhaowo.cores.utils.JavaDateUtils
import com.realleaderAndyanjing.engine.RealLeaderAndYanJingEngine
import org.apache.log4j.{Level, Logger}

/**
  * RealLeader和YanJing Use日常使用类
  */
object RealLeaderAndYanJingUse {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hbase").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.kafka").setLevel(Level.WARN)

    val start = System.currentTimeMillis
    val today = JavaDateUtils.stamp2DateYMD(String.valueOf(start))

    // todo 包含RealLeader和YanJing两部分的数据统计和数据读写。
    RealLeaderAndYanJingEngine.realLeaderDataStatistics()
    val stop = System.currentTimeMillis

    RealLeaderAndYanJingEngine.yanjingDataStatistics()
    val stop2 = System.currentTimeMillis

    System.out.println(today + " 每日定时任务——RealLeader策划师指标数据关联,用时: " + (stop - start) / 1000 + "s.")
    System.out.println(today + " 每日定时任务——Yanjing可预订策划师数据统计,用时: " + (stop2 - stop) / 1000 + "s.")

  }
}
