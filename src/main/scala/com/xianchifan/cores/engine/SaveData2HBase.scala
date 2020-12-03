package com.xianchifan.cores.engine

import java.sql.{ResultSet, Statement}
import java.util

import com.bangbang.cores.properties.JavaBangBangProperties
import com.izhaowo.cores.utils._
import com.xianchifan.cores.properties.JavaXianChiFanPlannerScheduleProperties
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes

object SaveData2HBase {

//  /**
//    * 1.读取tb_low_budget_hotel，写入HBase。
//    */
//  def saveTbLowBudgetHotel(): Unit = {
//    val spark = JavaSparkUtils.getInstance().getSparkSession
//    /** */
//    // 读取izhaowo_schedule下的tb_worker_schedule
//    val tb_low_budget_hotel_Df = spark.read.format("jdbc")
//      .option("url", JavaXianChiFanPlannerScheduleProperties.MYSQLURLIZHAODATAMING)
//      .option("dbtable", JavaXianChiFanPlannerScheduleProperties.TBLOWBUDGETHOTEL)
//      .option("user", JavaXianChiFanPlannerScheduleProperties.MYSQLUSER16)
//      .option("password", JavaXianChiFanPlannerScheduleProperties.MYSQLPASSWORD16)
//      .load()
//
//    tb_low_budget_hotel_Df.na.fill("") // 空值补全
//    println("========needDF:count==========" + tb_low_budget_hotel_Df.count())
//
//    tb_low_budget_hotel_Df.foreachPartition(ite => {
//      ite.foreach(row => {
//        val worker_id = row.getAs[String]("worker_id")
//        if (worker_id != null) {
//          try {
//            var province = row.getAs[String]("省")
//            if (province == null) {
//              province = ""
//            }
//            var city = row.getAs[String]("市")
//            if (city == null) {
//              city = ""
//            }
//            var zone = row.getAs[String]("区")
//            if (zone == null) {
//              zone = ""
//            }
//            var hotel = row.getAs[String]("hotel")
//            if (hotel == null) {
//              hotel = ""
//            }
//            var sot = row.getAs[Float]("sot").toString
//            if (sot == null) {
//              sot = ""
//            }
//            var section = row.getAs[Int]("section").toString
//            if (section == null) {
//              section = ""
//            }
//            var sot_state = row.getAs[Int]("sot_state").toString
//            if (sot_state == null) {
//              sot_state = ""
//            }
//            var bound_con = row.getAs[Int]("bound_con").toString
//            if (bound_con == null) {
//              bound_con = ""
//            }
//
//            val put = new Put(Bytes.toBytes(worker_id))
//            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("worker_id"), Bytes.toBytes(worker_id))
//            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("schedule_id"), Bytes.toBytes(schedule_id))
//            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("schedule_date"), Bytes.toBytes(schedule_date))
//            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("type"), Bytes.toBytes(ttype))
//            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("ctime"), Bytes.toBytes(ctime))
//            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("utime"), Bytes.toBytes(utime))
//            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("flag"), Bytes.toBytes(flag))
//            val list = new util.ArrayList[Put]()
//            list.add(put)
//            JavaHBaseUtils.putRows(JavaXianChiFanPlannerScheduleProperties.TBWORKERSCHEDULE, list)
//          } catch {
//            case e: Exception => e.printStackTrace()
//          }
//        }
//      })
//    })
//  }
}
