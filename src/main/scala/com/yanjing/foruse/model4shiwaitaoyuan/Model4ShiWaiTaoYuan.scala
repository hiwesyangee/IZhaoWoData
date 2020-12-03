package com.yanjing.foruse.model4shiwaitaoyuan

import com.yanjing.foruse.model4AllHotel.Model4AllHotel
import org.apache.log4j.{Level, Logger}

object Model4ShiWaiTaoYuan {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hbase").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.kafka").setLevel(Level.WARN)

    val start = System.currentTimeMillis()
    // hotel_id,hotel_name,section,ID,[arrA],[arrB]
    Model4AllHotel.doDataValidation("1fa69245-c5bc-4107-9e06-73ebbe0879c0", "世外桃源酒店", 3, args(0), args(1), args(2))
    val stop = System.currentTimeMillis()
    println("all use time: " + (stop - start) / 1000 + "s.")
  }
}
