package com.sida.cores.engine

import java.util

import com.izhaowo.cores.utils.{JavaHBaseUtils, JavaSparkUtils}
import com.sida.cores.properties.SidaProperties
import org.apache.hadoop.hbase.client.{Put, ResultScanner}
import org.apache.hadoop.hbase.util.Bytes

/**
  * 仅用于对四大数据进行读写，相对简单
  *
  * @since 2020/05/19 by Hiwes
  */
object SidaEngine {

  val spark = JavaSparkUtils.getInstance().getSparkSession

  import spark.implicits._

  /**
    * 四大数据主入口方法
    */
  def sidaDataTransfer(): Unit = {
    try {
      // 0.Truncate planner_sida表和sida_rank表
      truncateHBaseTable4Sida
      // 1.读写planner_sida表
      readAndWritePlannerSida
      // 2.读写sida_rank表___增加对经纬度的信息录入
      readAndWriteSidaRank
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  /**
    * 数据表的Truncate操作
    */
  def truncateHBaseTable4Sida(): Unit = {
    try {
      JavaHBaseUtils.deleteTable(SidaProperties.PLANNERSIDA4HBASE)
      JavaHBaseUtils.deleteTable(SidaProperties.SIDARANK4HBASE)
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      JavaHBaseUtils.createTable(SidaProperties.PLANNERSIDA4HBASE, SidaProperties.cfsOfPLANNERSIDA4HBASE)
      JavaHBaseUtils.createTable(SidaProperties.SIDARANK4HBASE, SidaProperties.cfsOfSIDARANK4HBASE)
    }
  }


  /**
    * 读写planner_sida表
    */
  def readAndWritePlannerSida(): Unit = {
    val plannerSidaDF = spark.read.format("jdbc")
      .option("url", SidaProperties.MYSQLURL)
      .option("dbtable", SidaProperties.PLANNERSIDA4HBASE)
      .option("user", SidaProperties.MYSQLUSER)
      .option("password", SidaProperties.MYSQLPASSWORD)
      .load()
      .withColumnRenamed("主持人id", "hid")
      .withColumnRenamed("主持人name", "hname")
      .withColumnRenamed("化妆师id", "aid")
      .withColumnRenamed("化妆师name", "aname")
      .withColumnRenamed("摄像师id", "cid")
      .withColumnRenamed("摄像师name", "cname")
      .withColumnRenamed("摄影师id", "pid")
      .withColumnRenamed("摄影师name", "pname")
      .withColumnRenamed("策划师id", "planner_id")
      .withColumnRenamed("策划师name", "planner_name")

    plannerSidaDF.cache()

    plannerSidaDF.foreachPartition(ite => {
      ite.foreach(row => {
        try {
          val flag = row.getAs[String]("flag")
          val sot = row.getAs[Double]("sot").toString
          val hid = row.getAs[String]("hid")
          val hname = row.getAs[String]("hname")
          val aid = row.getAs[String]("aid")
          val aname = row.getAs[String]("aname")
          val cid = row.getAs[String]("cid")
          val cname = row.getAs[String]("cname")
          val pid = row.getAs[String]("pid")
          val pname = row.getAs[String]("pname")
          val planner_id = row.getAs[String]("planner_id")
          val planner_name = row.getAs[String]("planner_name")

          //          if (null == hid) println("hid is empty。")
          //          if (null == aid) println("aid is empty。")
          //          if (null == cid) println("cid is empty。")
          //          if (null == pid) println("pid is empty。")

          if (null != planner_id) {
            val rowkey = checkPlannerSidaRowkey(planner_id)
            val put = new Put(Bytes.toBytes(rowkey))
            // 存储最基本的策划师+摄影师
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("flag"), Bytes.toBytes(flag))
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("sot"), Bytes.toBytes(sot))
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("pid"), Bytes.toBytes(pid))
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("pname"), Bytes.toBytes(pname))
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("planner_id"), Bytes.toBytes(planner_id))
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("planner_name"), Bytes.toBytes(planner_name))

            // 对职业人的多重判定。
            if (flag.contains("012")) {
              // 存储 摄像师
              put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("cid"), Bytes.toBytes(cid))
              put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("cname"), Bytes.toBytes(cname))
              if (flag.contains("0123")) {
                // 存储化妆师
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("aid"), Bytes.toBytes(aid))
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("aname"), Bytes.toBytes(aname))
                if (flag.contains("01234")) {
                  // 存储主持人
                  put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("hid"), Bytes.toBytes(hid))
                  put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("hname"), Bytes.toBytes(hname))
                }
              }
            }
            val list = new java.util.ArrayList[Put]()
            list.add(put)
            JavaHBaseUtils.putRows(SidaProperties.PLANNERSIDA4HBASE, list)
          }
        } catch {
          case e: Exception => e.printStackTrace()
        }
      })
    })
    plannerSidaDF.unpersist()
  }

  /**
    * 通过planner_id，返回新的planner_sida表的Rowkey
    *
    * @param planner_id
    * @return rowkey
    */
  def checkPlannerSidaRowkey(planner_id: String): String = {
    val start = planner_id + "=0"
    val stop = planner_id + "=999"
    val result: ResultScanner = JavaHBaseUtils.getScanner(SidaProperties.PLANNERSIDA4HBASE, start, stop)
    var res = result.next()
    var index = 1
    while (res != null) {
      index = index + 1
      res = result.next()
    }
    planner_id + "=" + index
  }

  /**
    * 读写sida_rank表
    */
  def readAndWriteSidaRank(): Unit = {
    val sidaRankDF = spark.read.format("jdbc")
      .option("url", SidaProperties.MYSQLURL)
      .option("dbtable", SidaProperties.SIDARANK4HBASE)
      .option("user", SidaProperties.MYSQLUSER)
      .option("password", SidaProperties.MYSQLPASSWORD)
      .load()

    sidaRankDF.cache()
    sidaRankDF.foreachPartition(ite => {
      ite.foreach(row => {
        try {
          val worker_id = row.getAs[String]("worker_id")
          val name = row.getAs[String]("name")
          val sot = row.getAs[Double]("sot").toString
          val flag = row.getAs[Long]("flag").toString

          if (null != worker_id && worker_id.length == 36) {
            val address_id = JavaHBaseUtils.getValue("v2_rp_tb_worker_location", worker_id, "info", "address_id");
            if (null != address_id && address_id.length == 36) {
              val longitude = JavaHBaseUtils.getValue("v2_rp_tb_hotel", address_id, "info", "longitude")
              val latitude = JavaHBaseUtils.getValue("v2_rp_tb_hotel", address_id, "info", "latitude")
              val lonAndLat = longitude + "," + latitude
              if (null != longitude && null != latitude) {
                val put = new Put(Bytes.toBytes(worker_id))
                // 存储最基本的策划师+摄影师 // "name", "sot", "flag", "worker_id"
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(name))
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("sot"), Bytes.toBytes(sot))
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("flag"), Bytes.toBytes(flag))
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("worker_id"), Bytes.toBytes(worker_id))

                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("longitude"), Bytes.toBytes(longitude))
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("latitude"), Bytes.toBytes(latitude))
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("lonAndLat"), Bytes.toBytes(lonAndLat))

                val list = new java.util.ArrayList[Put]()
                list.add(put)
                JavaHBaseUtils.putRows(SidaProperties.SIDARANK4HBASE, list)
              }
            }
          }
        } catch {
          case e: Exception => e.printStackTrace()
        }
      })
    })
    sidaRankDF.unpersist()
  }

}
