package com.yanjing.foruse.model4chognqingxilaideng

import com.yanjing.foruse.model4AllHotel.Model4AllHotel
import org.apache.log4j.{Level, Logger}

object Model4ChongQingXiLaiDeng {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hbase").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.kafka").setLevel(Level.WARN)

    val start = System.currentTimeMillis()
    // hotel_id,hotel_name,section,ID,[arrA],[arrB]
//    Model4AllHotel.doDataValidation("0a8c5c89-2705-4e1a-ba37-071f9176a417", "重庆喜来登大酒店", 3, args(0), args(1), args(2))
    Model4AllHotel.doDataValidation("0a8c5c89-2705-4e1a-ba37-071f9176a417", "重庆喜来登大酒店", 3, "142", "-0.95,-0.9,-1,-1,-1,0,-0.9,-1,900,-0.1", "0.6,1,0,0.9,0.9,1,0.9,1,1800,0.1")
    val stop = System.currentTimeMillis()
    println("all use time: " + (stop - start) / 1000 + "s.")
  }
}
