package com.bangbang.cores.foruse

import java.util

import com.izhaowo.cores.utils.JavaHBaseUtils
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 历史文件写入HBase和MySQL。
  *
  * 直接使用的类，一次性使用价值。
  */
object HistoryData2HBase {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hbase").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.kafka").setLevel(Level.WARN)

    val start = System.currentTimeMillis()
    val conf = new SparkConf().setMaster("local[4]").setAppName("SaveData")
    val sc = new SparkContext(conf)

    val line = sc.textFile("file:///opt/data/xcf/history.txt")
    println(line.count())

//    line.foreach(l => {
//      val arr = l.split(",")
//      // 调用存储方法
//      saveTbWorkerWeddingOrderData2HBase(arr)
//    })
    println("use time= " + (System.currentTimeMillis() - start) / 1000 + "s")

  }

  /**
    * 存储tb_worker_wedding_order表数据
    *
    * @param arr
    */
  def saveTbWorkerWeddingOrderData2HBase(arr: Array[String]): Unit = {
    val id = arr(0)
    val order_id = arr(1)
    val worker_id = arr(2)
    val status = arr(5)
    val ctime = arr(6)
    val utime = arr(7)
    val wedding_date = arr(8)
    val flag = arr(10)
    try {
      val put = new Put(Bytes.toBytes(id))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("order_id"), Bytes.toBytes(order_id))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("worker_id"), Bytes.toBytes(worker_id))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("status"), Bytes.toBytes(status))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("ctime"), Bytes.toBytes(ctime))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("utime"), Bytes.toBytes(utime))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("wedding_date"), Bytes.toBytes(wedding_date))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("flag"), Bytes.toBytes(flag))
      val list = new util.ArrayList[Put]()
      list.add(put)
      JavaHBaseUtils.putRows("tb_worker_wedding_order", list)
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }
}
