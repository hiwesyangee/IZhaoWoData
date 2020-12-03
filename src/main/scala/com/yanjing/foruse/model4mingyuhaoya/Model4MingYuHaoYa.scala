package com.yanjing.foruse.model4mingyuhaoya

import com.yanjing.foruse.model4AllHotel.Model4AllHotel
import org.apache.log4j.{Level, Logger}

object Model4MingYuHaoYa {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hbase").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.kafka").setLevel(Level.WARN)

    val start = System.currentTimeMillis()
    // hotel_id,hotel_name,section,ID,[arrA],[arrB]
    Model4AllHotel.doDataValidation("af155b48-3149-45af-8625-16c42d5570e3", "明宇豪雅饭店(科华店)", 4, args(0), args(1), args(2))
    val stop = System.currentTimeMillis()
    println("all use time: " + (stop - start) / 1000 + "s.")
  }
}
