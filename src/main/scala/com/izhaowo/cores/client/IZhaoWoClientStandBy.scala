package com.izhaowo.cores.client

import java.util

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Put, ResultScanner}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, Logger}

/**
  * IZhaoWoData项目client备用入口
  *
  * @version 2.0
  * @since 2019/06/17 by Hiwes
  */
object IZhaoWoClientStandBy {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hbase").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.kafka").setLevel(Level.WARN)

    //    // 获取配置SparkConf
    //    val conf = JavaSparkUtils.getInstance().getSparkConf
    //    // 获取配置SparkSession
    //    val spark = JavaSparkUtils.getInstance().getSparkSession
    //    // 获取配置SparkContext
    //    val sc = spark.sparkContext // 只能有一个sc，但是可以根据这个sc，创建多个对象实例
    //    // 获取配置SQLContext
    //    val sqlSc = spark.sqlContext
    //
    //    // 可以创建多个ssc实例
    //
    //
    //    val ssc = new StreamingContext(sc, Durations.seconds(5))
    //    ssc.checkpoint("hdfs://hiwes:8020/opt/checkpoint/")
    //    // 可以创建多个ssc实例
    //    val ssc2 = new StreamingContext(sc, Durations.seconds(5))
    //    ssc2.checkpoint("hdfs://hiwes:8020/opt/checkpoint2/")

    //    // 遍历HBase表的方法很简单。
    //    val result: ResultScanner = JavaHBaseUtils.getScanner("test1")
    //    var res = result.next()
    //    while (res != null) {
    //      println(Bytes.toString(res.getRow()))
    //      println(Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"))))
    //      res = result.next()
    //    }

    //    val pool = JavaHBasePoolUtils.getPool
    //    val conn = pool.getConnection.getTable(TableName.valueOf("test1"))
    //    println(conn)
    //    try {
    //      Thread.sleep(3000)
    //    } catch {
    //      case e: Exception => e.printStackTrace()
    //    }
    //    JavaHBasePoolUtils.closePool() // 注意：这个方法一旦调用。再次创建就会报错。
    //

    //    JavaHBaseUtils.getRow("test1", "1")
    //    JavaHBasePoolUtils.getRow("test1", "1")
    //    println("开始计时。")
    ////    val start = System.currentTimeMillis()
    ////    for (i <- 1 to 10000) {
    ////      JavaHBaseUtils.putRow("test1", i.toString, "info", "name", "name" + i.toString)
    ////    }
    ////    val stop = System.currentTimeMillis()
    //
    //    val start1 = System.currentTimeMillis()
    //    val list = new util.ArrayList[Put]()
    //    for (i <- 1 to 2000000) {
    //      val put = new Put(Bytes.toBytes(i.toString))
    //      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(i.toString))
    //      list.add(put)
    //    }
    //    JavaHBaseUtils.putRows("test1", list)
    //    val stop1 = System.currentTimeMillis()
    //
    //
    ////    val start2 = System.currentTimeMillis()
    ////    for (i <- 1 to 10000) {
    ////      JavaHBasePoolUtils.putRow("test1", i.toString, "info", "name", "name" + i.toString)
    ////    }
    ////    val stop2 = System.currentTimeMillis()
    //
    //
    //    val start2 = System.currentTimeMillis()
    //    val list2 = new util.ArrayList[Put]()
    //    for (i <- 1 to 2000000) {
    //      val put = new Put(Bytes.toBytes(i.toString))
    //      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(i.toString))
    //      list2.add(put)
    //    }
    //    JavaHBasePoolUtils.putRows("test1", list2)
    //    val stop2 = System.currentTimeMillis()
    //
    ////    println("normal: " + (stop - start) / 1000 + "s.")
    //    println("normalBatch: " + (stop1 - start1) / 1000 + "s.")
    ////    println("pool: " + (stop2 - start2) / 1000 + "s.")
    //    println("poolBatch: " + (stop2 - start2) / 1000 + "s.")
    //
    //    JavaHBasePoolUtils.closePool()

  }

}
