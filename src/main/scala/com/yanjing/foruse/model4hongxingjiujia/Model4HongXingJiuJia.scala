package com.yanjing.foruse.model4hongxingjiujia

import com.yanjing.foruse.model4AllHotel.Model4AllHotel
import org.apache.log4j.{Level, Logger}

/**
  * 通过数据模型对望江宾馆数据进行验证。
  */
object Model4HongXingJiuJia {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hbase").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.kafka").setLevel(Level.WARN)

    val start = System.currentTimeMillis()
    // hotel_id,hotel_name,section,ID,[arrA],[arrB]
    Model4AllHotel.doDataValidation("e55e194f-007d-4c3c-b162-66a2f5619dbb", "红杏酒家(光华店)", 3, args(0), args(1), args(2))
    val stop = System.currentTimeMillis()
    println("all use time: " + (stop - start) / 1000 + "s.")
  }
}
