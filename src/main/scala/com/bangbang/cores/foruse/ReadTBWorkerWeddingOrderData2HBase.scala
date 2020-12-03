package com.bangbang.cores.foruse

import java.util

import com.bangbang.cores.engine.BangBangStreamingDataProcessEngine
import com.bangbang.cores.engine.BangBangStreamingDataProcessEngine.isNotTestData
import com.bangbang.cores.properties.JavaBangBangProperties
import com.izhaowo.cores.utils.{JavaHBaseUtils, JavaSparkUtils}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, Logger}

/**
  * 读取MYSQL数据库3张表到HBase
  */
object ReadTBWorkerWeddingOrderData2HBase {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hbase").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.kafka").setLevel(Level.WARN)

    val start = System.currentTimeMillis()
    val spark = JavaSparkUtils.getInstance().getSparkSession

    /** */
    // 读取izhaowo_order下的tb_worker_wedding_order
    val tb_worker_wedding_order_Df = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://rm-m5ejc86pt76j133u3.mysql.rds.aliyuncs.com:3306/izhaowo_order?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false&serverTimezone=Asia/Shanghai&useSSL=true&dontTrackOpenResources=true&defaultFetchSize=10000&useCursorFetch=true")
      .option("dbtable", "tb_worker_wedding_order")
      .option("user", JavaBangBangProperties.MYSQLUSER14)
      .option("password", JavaBangBangProperties.MYSQLPASSWORD14)
      .load()

    tb_worker_wedding_order_Df.na.fill("null") // 空值补全
    println("========needDF:count==========" + tb_worker_wedding_order_Df.count())

    tb_worker_wedding_order_Df.foreachPartition(ite => {
      ite.foreach(row => {
        val id = row.getAs[String]("id")
        if (id != null) {
          var order_id = row.getAs[String]("order_id")
          if (order_id == null) {
            order_id = ""
          }
          var worker_id = row.getAs[String]("worker_id")
          if (worker_id == null) {
            worker_id = ""
          }
          var wedding_id = row.getAs[String]("wedding_id")
          if (wedding_id == null) {
            wedding_id = ""
          }
          var schedule_id = row.getAs[String]("schedule_id")
          if (schedule_id == null) {
            schedule_id = ""
          }
          var status = row.getAs[Int]("status").toString
          if (status == null) {
            status = ""
          }
          var ctime = row.getAs("ctime").toString
          if (ctime == null) {
            ctime = ""
          }
          var utime = row.getAs("utime").toString
          if (utime == null) {
            utime = ""
          }
          var wedding_date = row.getAs("wedding_date").toString
          if (wedding_date == null) {
            wedding_date = ""
          }
          var remote_fee = row.getAs[Int]("remote_fee").toString
          if (remote_fee == null) {
            remote_fee = ""
          }
          var flag = row.getAs[Int]("flag").toString
          if (flag == null) {
            flag = ""
          }

          val put = new Put(Bytes.toBytes(id))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("order_id"), Bytes.toBytes(order_id))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("worker_id"), Bytes.toBytes(worker_id))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("wedding_id"), Bytes.toBytes(wedding_id))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("schedule_id"), Bytes.toBytes(schedule_id))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("status"), Bytes.toBytes(status))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("ctime"), Bytes.toBytes(ctime))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("utime"), Bytes.toBytes(utime))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("wedding_date"), Bytes.toBytes(wedding_date))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("remote_fee"), Bytes.toBytes(remote_fee))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("flag"), Bytes.toBytes(flag))
          val list = new util.ArrayList[Put]()
          list.add(put)
          JavaHBaseUtils.putRows("tb_worker_wedding_order", list)
        }
      })
    })

    val stop = System.currentTimeMillis()
    println("use time: " + (stop - start) / 1000 + "s.")


  }
}
