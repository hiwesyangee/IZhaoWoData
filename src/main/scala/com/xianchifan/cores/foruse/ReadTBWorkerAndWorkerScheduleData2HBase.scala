package com.xianchifan.cores.foruse

import java.util

import com.izhaowo.cores.utils.{JavaHBaseUtils, JavaSparkUtils}
import com.xianchifan.cores.properties.JavaXianChiFanPlannerScheduleProperties
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, Logger}

/**
  * 读取MYSQL数据库2张表到HBase——tb_worker_schedule和tb_worker。
  */
object ReadTBWorkerAndWorkerScheduleData2HBase {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hbase").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.kafka").setLevel(Level.WARN)

    val start = System.currentTimeMillis()
    val spark = JavaSparkUtils.getInstance().getSparkSession

    /** */
    // 读取izhaowo_schedule下的tb_worker_schedule
    val tb_worker_schedule_Df = spark.read.format("jdbc")
      .option("url", JavaXianChiFanPlannerScheduleProperties.MYSQLURLIZHAOWOWORKERSCHEDULE)
      .option("dbtable", JavaXianChiFanPlannerScheduleProperties.TBWORKERSCHEDULE)
      .option("user", JavaXianChiFanPlannerScheduleProperties.MYSQLUSER16)
      .option("password", JavaXianChiFanPlannerScheduleProperties.MYSQLPASSWORD16)
      .load()

    tb_worker_schedule_Df.na.fill("") // 空值补全
    println("========needDF:count==========" + tb_worker_schedule_Df.count())

    tb_worker_schedule_Df.foreachPartition(ite => {
      ite.foreach(row => {
        val id = row.getAs[String]("id")
        if (id != null) {
          try {
            var worker_id = row.getAs[String]("worker_id")
            if (worker_id == null) {
              worker_id = ""
            }
            var schedule_id = row.getAs[String]("schedule_id")
            if (schedule_id == null) {
              schedule_id = ""
            }
            var schedule_date = row.getAs("schedule_date").toString
            if (schedule_date == null) {
              schedule_date = ""
            }
            var ttype = row.getAs[Int]("type").toString
            if (ttype == null) {
              ttype = ""
            }
            var ctime = row.getAs("ctime").toString
            if (ctime == null) {
              ctime = ""
            }
            var utime = row.getAs("utime").toString
            if (utime == null) {
              utime = ""
            }
            var flag = row.getAs[Int]("flag").toString
            if (flag == null) {
              flag = ""
            }
            val put = new Put(Bytes.toBytes(id))
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("worker_id"), Bytes.toBytes(worker_id))
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("schedule_id"), Bytes.toBytes(schedule_id))
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("schedule_date"), Bytes.toBytes(schedule_date))
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("type"), Bytes.toBytes(ttype))
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("ctime"), Bytes.toBytes(ctime))
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("utime"), Bytes.toBytes(utime))
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("flag"), Bytes.toBytes(flag))
            val list = new util.ArrayList[Put]()
            list.add(put)
            JavaHBaseUtils.putRows(JavaXianChiFanPlannerScheduleProperties.TBWORKERSCHEDULE, list)
          } catch {
            case e: Exception => e.printStackTrace()
          }
        }
      })
    })

    /** */
    // 读取izhaowo_schedule下的tb_worker_schedule
    val tb_worker_Df = spark.read.format("jdbc")
      .option("url", JavaXianChiFanPlannerScheduleProperties.MYSQLURLIZHAOWOWORKER)
      .option("dbtable", JavaXianChiFanPlannerScheduleProperties.TBWORKER)
      .option("user", JavaXianChiFanPlannerScheduleProperties.MYSQLUSER16)
      .option("password", JavaXianChiFanPlannerScheduleProperties.MYSQLPASSWORD16)
      .load()

    tb_worker_Df.na.fill("") // 空值补全
    println("========needDF:count==========" + tb_worker_Df.count())

    tb_worker_Df.foreachPartition(ite => {
      ite.foreach(row => {
        val id = row.getAs[String]("id")
        if (id != null) {
          try {
            var name = row.getAs[String]("name")
            if (name == null) {
              name = ""
            }
            var vocation_id = row.getAs[String]("vocation_id")
            if (vocation_id == null) {
              vocation_id = ""
            }
            var user_id = row.getAs[String]("user_id")
            if (user_id == null) {
              user_id = ""
            }
            var height = row.getAs[Int]("height").toString
            if (height == null) {
              height = ""
            }
            var weight = row.getAs[Int]("weight").toString
            if (weight == null) {
              weight = ""
            }
            var sex = row.getAs[Int]("sex").toString
            if (sex == null) {
              sex = ""
            }
            var birthday = row.getAs("birthday").toString
            if (birthday == null) {
              birthday = ""
            }
            var profile = row.getAs[String]("profile")
            if (profile == null) {
              profile = ""
            }
            var home_page = row.getAs[String]("home_page")
            if (home_page == null) {
              home_page = ""
            }
            var daily_limit = row.getAs[Int]("daily_limit").toString
            if (daily_limit == null) {
              daily_limit = ""
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
            var tag = row.getAs[Int]("tag").toString
            if (tag == null) {
              tag = ""
            }
            var real_name = row.getAs[String]("real_name")
            if (real_name == null) {
              real_name = ""
            }
            var avator = row.getAs[String]("avator")
            if (avator == null) {
              avator = ""
            }
            val put = new Put(Bytes.toBytes(id))
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(name))
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("vocation_id"), Bytes.toBytes(vocation_id))
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("user_id"), Bytes.toBytes(user_id))
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("height"), Bytes.toBytes(height))
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("weight"), Bytes.toBytes(weight))
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("sex"), Bytes.toBytes(sex))
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("birthday"), Bytes.toBytes(birthday))
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("birthday"), Bytes.toBytes(birthday))
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("home_page"), Bytes.toBytes(home_page))
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("daily_limit"), Bytes.toBytes(daily_limit))
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("status"), Bytes.toBytes(status))
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("ctime"), Bytes.toBytes(ctime))
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("utime"), Bytes.toBytes(utime))
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("tag"), Bytes.toBytes(tag))
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("real_name"), Bytes.toBytes(real_name))
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("avator"), Bytes.toBytes(avator))
            val list = new util.ArrayList[Put]()
            list.add(put)
            JavaHBaseUtils.putRows(JavaXianChiFanPlannerScheduleProperties.TBWORKER, list)
          } catch {
            case e: Exception => e.printStackTrace()
          }
        }
      })
    })

    val stop = System.currentTimeMillis()
    println("use time: " + (stop - start) / 1000 + "s.")

  }
}
