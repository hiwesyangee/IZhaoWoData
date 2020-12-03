package com.yanjing.foruse.model4ruichengmingren

import com.yanjing.foruse.model4AllHotel.Model4AllHotel
import org.apache.log4j.{Level, Logger}

object Model4RuiChengMingRen {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hbase").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.kafka").setLevel(Level.WARN)

    val start = System.currentTimeMillis()
    // hotel_id,hotel_name,section,ID,[arrA],[arrB]
    Model4AllHotel.doDataValidation("f8655d30-1153-4622-9a43-0e43e544d7e9", "成都瑞城名人酒店", 3, args(0), args(1), args(2))
    val stop = System.currentTimeMillis()
    println("all use time: " + (stop - start) / 1000 + "s.")
  }
}
