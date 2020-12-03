package com.sida.cores.client

import com.sida.cores.engine.SidaEngine
import org.apache.log4j.{Level, Logger}

/**
  * 使用任务类，直接对四大信息表进行读取，并写入到HBase。
  *
  * @since 2020/05/19 by Hiwes
  */
object SidaUse {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hbase").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.kafka").setLevel(Level.WARN)
    
    SidaEngine.sidaDataTransfer()
  }
}
