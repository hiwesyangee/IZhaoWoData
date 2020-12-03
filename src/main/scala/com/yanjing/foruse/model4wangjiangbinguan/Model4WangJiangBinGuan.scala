package com.yanjing.foruse.model4wangjiangbinguan

import com.izhaowo.cores.utils.JavaSparkUtils
import com.yanjing.foruse.model4AllHotel.Model4AllHotel
import org.apache.log4j.{Level, Logger}

/**
  * 通过数据模型对望江宾馆数据进行验证。
  */
object Model4WangJiangBinGuan {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hbase").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.kafka").setLevel(Level.WARN)

    val start = System.currentTimeMillis()
    // hotel_id,hotel_name,section,ID,[arrA],[arrB]
    // doDataValidation("09f8e3bd-0053-44c0-85ff-3939690ecb09", "望江宾馆", 4)
    Model4AllHotel.doDataValidation("09f8e3bd-0053-44c0-85ff-3939690ecb09", "望江宾馆", 4, args(0), args(1), args(2))
    val stop = System.currentTimeMillis()
    println("all use time: " + (stop - start) / 1000 + "s.")
  }
}
