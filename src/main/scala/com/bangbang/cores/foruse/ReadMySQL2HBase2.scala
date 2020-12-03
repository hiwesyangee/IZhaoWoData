package com.bangbang.cores.foruse

import java.util

import com.bangbang.cores.engine.BangBangStreamingDataProcessEngine
import com.bangbang.cores.engine.BangBangStreamingDataProcessEngine.isNotTestData
import com.bangbang.cores.properties.JavaBangBangProperties
import com.izhaowo.cores.utils.JavaSparkUtils
import org.apache.log4j.{Level, Logger}

/**
  * 读取MYSQL数据库3张表到HBase
  */
object ReadMySQL2HBase2 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hbase").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.kafka").setLevel(Level.WARN)

    val start = System.currentTimeMillis()
    val spark = JavaSparkUtils.getInstance().getSparkSession

    // 读取izhaowo_user_wedding下的tb_planner_recom_record ___
    val tb_planner_recom_record_Df = spark.read.format("jdbc")
      .option("url", JavaBangBangProperties.MYSQLURLTBUSERWEDDING)
      .option("dbtable", JavaBangBangProperties.tb_planner_recom_record)
      .option("user", JavaBangBangProperties.MYSQLUSER14)
      .option("password", JavaBangBangProperties.MYSQLPASSWORD14)
      .load()
    val endDF = tb_planner_recom_record_Df.filter(row => {
      row.getAs[Int]("type") == 0 &&
        row.getAs[String]("wedding_id").length > 0 &&
        isNotTestData(row.getAs[String]("wedding_id")) == true &&
        BangBangStreamingDataProcessEngine.getWeddingInfo(row.getAs[String]("wedding_id"))(0).replaceAll("-", "").toInt >= 20190601
    })

    endDF.na.fill("null").cache() // 空值补全
    println("========endDF:count==========" + endDF.count())

    endDF.foreachPartition(ite => {
      ite.foreach(row => {
        val id = row.getAs[String]("id")
        val worker_id = row.getAs[String]("worker_id")
        val wedding_id = row.getAs[String]("wedding_id")
        val ctime = row.getAs("ctime").toString
        val utime = row.getAs("utime").toString
        val ranking = row.getAs[Int]("utime").toString
        val type2 = row.getAs[Int]("type").toString
        val crm_user_id = row.getAs[String]("crm_user_id")
        if (wedding_id.length > 0) {
          val arr = Array(id, worker_id, wedding_id, ctime, utime, ranking, type2, crm_user_id)
          if (BangBangStreamingDataProcessEngine.getWeddingHotel(wedding_id) != null &&
            BangBangStreamingDataProcessEngine.getWeddingInfo(wedding_id).length == 3
          ) {
            try {
              BangBangStreamingDataProcessEngine.saveTbPlannerRecomRecordData2HBase(arr)
              BangBangStreamingDataProcessEngine.saveCanNum4statistical2HBase(arr) // todo 录入历史数据
            } catch {
              case e: Exception => e.printStackTrace()
            }
          }
        }
      })
    })
    endDF.unpersist()

    /** */
    // 读取izhaowo_user_wedding下的tb_user_wedding_team_member
    val tb_user__wedding_team_member_Df = spark.read.format("jdbc")
      .option("url", JavaBangBangProperties.MYSQLURLTBUSERWEDDING)
      .option("dbtable", JavaBangBangProperties.TBUSERWEDDINGTEAMMEMBER)
      .option("user", JavaBangBangProperties.MYSQLUSER14)
      .option("password", JavaBangBangProperties.MYSQLPASSWORD14)
      .load()
    val needDF = tb_user__wedding_team_member_Df.filter(row => {
      row.getAs[Int]("commit_status") != 0 &&
        row.getAs[String]("wedding_id").length > 0 &&
        row.getAs[String]("vocation").equals("策划师") &&
        isNotTestData(row.getAs[String]("wedding_id")) == true &&
        BangBangStreamingDataProcessEngine.getWeddingInfo(row.getAs[String]("wedding_id"))(0).replaceAll("-", "").toInt >= 20190601
    })

    needDF.na.fill("null") // 空值补全
    println("========needDF:count==========" + needDF.count())

    needDF.foreachPartition(ite => {
      ite.foreach(row => {
        val id = row.getAs[String]("id")
        val user_wedding_team_id = row.getAs[String]("user_wedding_team_id")
        val wedding_worker_id = row.getAs[String]("wedding_worker_id")
        val wedding_id = row.getAs[String]("wedding_id")
        val name = row.getAs[String]("name")
        val avator = row.getAs[String]("avator")
        val home_page = row.getAs[String]("home_page")
        val vocation = row.getAs[String]("vocation")
        val sort = row.getAs[Int]("sort").toString
        val commit_status = row.getAs[Int]("commit_status").toString
        val ctime = row.getAs("ctime").toString
        val utime = row.getAs("utime").toString
        val msisdn = row.getAs[String]("msisdn")
        val service_name = row.getAs[String]("service_name")
        val display_amount = row.getAs[Int]("display_amount").toString
        val settlement_amount = row.getAs[Int]("settlement_amount").toString
        val worker_service_id = row.getAs[String]("worker_service_id")
        val people = row.getAs[Int]("people").toString
        val base_display_amount = row.getAs[Int]("base_display_amount").toString
        val base_settlement_amount = row.getAs[Int]("base_settlement_amount").toString
        val price_rise_id = row.getAs[String]("price_rise_id")
        val service_id = row.getAs[String]("service_id")
        val type2 = row.getAs[Int]("type").toString
        val recom_type = row.getAs[Int]("recom_type").toString
        val recom_num = row.getAs[Int]("recom_num").toString
        val sot = row.getAs[Double]("recom_num").toString
        if (wedding_id.length > 0) {
          val arr = Array[String](id, user_wedding_team_id, wedding_worker_id, wedding_id, name, avator, home_page, vocation, sort, commit_status, ctime, utime, msisdn, service_name, display_amount, settlement_amount, worker_service_id, people, base_display_amount, base_settlement_amount, price_rise_id, service_id, type2, recom_type, recom_num, sot)
          if (BangBangStreamingDataProcessEngine.getWeddingHotel(wedding_id) != null &&
            BangBangStreamingDataProcessEngine.getWeddingInfo(wedding_id).length == 3) {
            try {
              BangBangStreamingDataProcessEngine.saveTbUserWeddingTeamMemberData2HBase(arr)
              BangBangStreamingDataProcessEngine.saveDoneNum4statistical2HBase(arr)
            } catch {
              case e: Exception => e.printStackTrace()
            }
          }
        }
      })
    })
    needDF.unpersist()

    val stop = System.currentTimeMillis()
    println("use time: " + (stop - start) / 1000 + "s.")
  }
}
