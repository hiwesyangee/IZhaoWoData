package com.bangbang.cores.engine

import java.sql.{ResultSet, Statement}
import java.util

import com.bangbang.cores.properties.JavaBangBangProperties
import com.izhaowo.cores.utils.{JavaDateUtils, JavaHBaseUtils, JavaSQLServerConn, JavaSparkUtils}
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.util.Bytes

/**
  * BangBang业务：存储策划师供给需求统计数据到SQLServer。
  */
object BangBangEngine {
  val spark = JavaSparkUtils.getInstance().getSparkSession

  import spark.implicits._

  def readAndWriteDemandSupplyData2SQLServer(): Unit = {
    val start = System.currentTimeMillis()
    // 1.清空原来的数据表
    truncateTableInHBase()
    val step1 = System.currentTimeMillis()
    println("______1.清空数据表用时: " + (step1 - start) + " ms.")

    // 2.读取MySQL数据库，对tb_user_wedding_team_member表进行统计，得到已预订策划师并存入tb_planner_demand
    savePlannerDemandData2HBase()
    val step2 = System.currentTimeMillis()
    println("______2.写入已预订策划师数据到HBase: " + (step2 - step1) + " ms.")

    // 3.读取v2_rp_tb_planner_recom_record，针对utime在20190819之前的数据进行统计，得到可预订策划师历史数据并存入tb_planner_supply；
    saveHistoryRecordData2HBase()
    val step3 = System.currentTimeMillis()
    println("______3.写入历史可预订策划师数据到HBase: " + (step3 - step2) + " ms.")

    // 4.读取HBase数据库，对recom_planner_result表进行统计，得到可预订策划师并存入tb_planner_supply
    saveNowRecordData2HBase()
    val step4 = System.currentTimeMillis()
    println("______4.写入可预订策划师数据到HBase: " + (step4 - step3) + " ms.")

    // 5.读取tb_planner_supply，写入SQLServer
    savePlannnerSupply2SQLServer()
    val step5 = System.currentTimeMillis()
    println("______5.写入可预订策划师数据到sqlserver: " + (step5 - step4) + " ms.")

    // 6.读取tb_planner_demand，写入SQLServer
    savePlannnerDemand2SQLServer()
    val step6 = System.currentTimeMillis()
    println("______6.写入已预订策划师数据到sqlserver: " + (step6 - step5) + " ms.")
  }

  // 1.每日清空HBase数据表，tb_planner_supply和tb_planner_demand;
  def truncateTableInHBase(): Unit = {
    val cfs = Array("info")
    try {
      JavaHBaseUtils.deleteTable("tb_planner_demand")
      JavaHBaseUtils.deleteTable("tb_planner_supply")

      JavaHBaseUtils.createTable("tb_planner_demand", cfs)
      JavaHBaseUtils.createTable("tb_planner_supply", cfs)
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  // 2.读取MySQL数据库，对tb_user_wedding_team_member表进行统计，得到已预订策划师并存入tb_planner_demand；
  def savePlannerDemandData2HBase(): Unit = {
    // rowkey = hotel_id=weddingData=len

    // todo 这个地方可进行优化，直接读已经Streaming后的v2_rp_tb_user_wedding_team_member进行数据读取。
    val weddingTeamMemberDf = spark.read.format("jdbc")
      .option("url", JavaBangBangProperties.MYSQLURLTBUSERWEDDING)
      .option("dbtable", JavaBangBangProperties.TBUSERWEDDINGTEAMMEMBER)
      .option("user", JavaBangBangProperties.MYSQLUSER14)
      .option("password", JavaBangBangProperties.MYSQLPASSWORD14)
      .load()

    val needDf = weddingTeamMemberDf.filter(row => {
      val sort = row.getAs[Int]("sort")
      (sort != null) && (sort == 0) // 指定策划师数据
    })

    needDf.foreachPartition(ite => {
      ite.foreach(row => {
        val planner_id = row.getAs[String]("wedding_worker_id")
        val wedding_id = row.getAs[String]("wedding_id")
        val hotel_id = JavaHBaseUtils.getValue("v2_rp_tb_user_wedding", wedding_id, "info", "hotel_id")
        if (hotel_id != null && hotel_id.length > 0 && wedding_id != null && isNotTestData(wedding_id)) {
          val weddingDataArr = getNeedDataByWeddingID(wedding_id)
          if (!weddingDataArr.contains(null) && (weddingDataArr.length == 8)) {
            val allDataArr = Array(wedding_id, planner_id) ++ weddingDataArr
            saveTbPlannerDamendData2HBase(allDataArr)
          }
        }
      })
    })
  }

  // 2.1存储tb_planner_demand表数据
  def saveTbPlannerDamendData2HBase(allDataArr: Array[String]): Unit = {
    try {
      // Array(wedding_id, planner_id, province, city, zone, name, wedding_date, budget_min, budget_max, hotel_id)
      if (allDataArr.length == 10) {
        val start = allDataArr(9) + "=" + (allDataArr(6).replaceAll("-", "")) + "=0"
        val stop = allDataArr(9) + "=" + (allDataArr(6).replaceAll("-", "")) + "=999"

        val result = JavaHBaseUtils.getScanner(JavaBangBangProperties.TBPLANNERDEMAND, start, stop)
        var len = 1
        var index = 1
        var res = result.next()
        while (res != null) {
          val hotel_id = Bytes.toString(res.getRow).split("=")(0)
          val planner_id = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("planner_id")))
          val wedding_date = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("wedding_date")))
          if (!hotel_id.equals(allDataArr(9)) && !planner_id.equals(allDataArr(1)) && !wedding_date.equals(allDataArr(6))) {
            len += 1
          }
          res = result.next()
          index += 1
        }
        if (index == len) {
          val rowkey = allDataArr(9) + "=" + (allDataArr(6).replaceAll("-", "")) + "=" + len.toString
          val put = new Put(Bytes.toBytes(rowkey))
          for (i <- 0 to 8) {
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(JavaBangBangProperties.columnsOfTBPLANNERDEMAND(i)), Bytes.toBytes(allDataArr(i)))
          }
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(JavaBangBangProperties.columnsOfTBPLANNERDEMAND(9)), Bytes.toBytes(len.toString))
          val list = new util.ArrayList[Put]()
          list.add(put)
          JavaHBaseUtils.putRows(JavaBangBangProperties.TBPLANNERDEMAND, list)
        }
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  // 3.读取v2_rp_tb_planner_recom_record，针对utime在20190819之前的数据进行统计，得到可预订策划师历史数据并存入tb_planner_supply；
  def saveHistoryRecordData2HBase(): Unit = {
    try {
      val result = JavaHBaseUtils.getScanner("v2_rp_tb_planner_recom_record")
      var res = result.next()
      while (res != null) {
        val utime = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("utime")))
        if (utime.substring(0, 10).replaceAll("-", "").toLong <= 20190819) {
          val wedding_id = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("wedding_id")))
          val worker_id = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("worker_id")))
          if (wedding_id != null && wedding_id.length > 0 && isNotTestData(wedding_id)) {
            val weddingDataArr = getNeedDataByWeddingID(wedding_id)
            val allDataArr = Array(wedding_id, worker_id) ++ weddingDataArr
            if (!allDataArr.contains(null)) {
              saveTbPlannerSupplyData2HBase(allDataArr)
            }
          }
        }
        res = result.next()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  // 3.1存储tb_planner_supply表数据
  def saveTbPlannerSupplyData2HBase(allDataArr: Array[String]) = {
    try {
      // Array(wedding_id, planner_id, province, city, zone, name, wedding_date, budget_min, budget_max, hotel_id)
      if (allDataArr.length == 10) {
        val start = allDataArr(9) + "=" + (allDataArr(6).replaceAll("-", "")) + "=0"
        val stop = allDataArr(9) + "=" + (allDataArr(6).replaceAll("-", "")) + "=999"

        val result = JavaHBaseUtils.getScanner(JavaBangBangProperties.TBPLANNERSUPPLY, start, stop)
        var len = 1
        var index = 1
        var res = result.next()
        while (res != null) {
          val wedding_id = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("wedding_id")))
          val planner_id = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("planner_id")))
          val wedding_date = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("wedding_date")))
          if (!wedding_id.equals(allDataArr(0)) || !planner_id.equals(allDataArr(1)) || !wedding_date.equals(allDataArr(6))) {
            len += 1
          }
          res = result.next()
          index += 1
        }
        if (index == len) {
          val rowkey = allDataArr(9) + "=" + (allDataArr(6).replaceAll("-", "")) + "=" + len.toString
          val put = new Put(Bytes.toBytes(rowkey))
          for (i <- 0 to 8) {
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(JavaBangBangProperties.columnsOfTBPLANNERSUPPLY(i)), Bytes.toBytes(allDataArr(i)))
          }
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(JavaBangBangProperties.columnsOfTBPLANNERSUPPLY(9)), Bytes.toBytes(len.toString))
          val list = new util.ArrayList[Put]()
          list.add(put)
          JavaHBaseUtils.putRows(JavaBangBangProperties.TBPLANNERSUPPLY, list)
        }
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  // 4.读取HBase数据库，对recom_planner_result表进行统计，得到可预订策划师并存入tb_planner_supply
  def saveNowRecordData2HBase(): Unit = {
    val result = JavaHBaseUtils.getScanner("recom_planner_result")
    var res = result.next()
    while (res != null) {
      val wedding_id = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("wedding_id")))
      val worker_id = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("planner_id")))
      val hotel_id = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("hotel_id")))
      var weddingDataArr: Array[String] = Array()
      if (wedding_id.length > 0 && hotel_id.length > 0) {
        weddingDataArr = getNeedDataByWeddingID(wedding_id, hotel_id)
      }
      if (weddingDataArr != null && weddingDataArr.length == 8 && !weddingDataArr.contains(null)) {
        val allDataArr = Array(wedding_id, worker_id) ++ weddingDataArr
        if (!allDataArr.contains(null) && isNotTestData(wedding_id)) {
          saveTbPlannerSupplyData2HBase(allDataArr)
        }
      }
      res = result.next()
    }
  }

  /**
    * 5.存储可预订策划师数据到SQLServer
    */
  def savePlannnerSupply2SQLServer(): Unit = {
    // todo scan读取HBase数据tb_planner_supply，然后逐条进行写入到sqlserver
    val scanner = JavaHBaseUtils.getScanner(JavaBangBangProperties.TBPLANNERSUPPLY)
    val conn = JavaSQLServerConn.getConnection
    var res = scanner.next()
    while (res != null) {
      val wedding_id = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("wedding_id")))
      val planner_id = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("planner_id")))
      val province = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("province")))
      val city = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("city")))
      val zone = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("zone")))
      val hotel_name = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("hotel_name")))
      val wedding_date = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("wedding_date")))
      var budget_min = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("budget_min")))
      var budget_max = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("budget_max")))
      val can_num = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("can_num")))
      // todo
      if (can_num.equals("0")) println(Bytes.toString(res.getRow))

      val st: Statement = conn.createStatement();
      if (budget_max.toInt < budget_min.toInt) {
        val next = budget_min
        budget_min = budget_max
        budget_max = next
      }
      val sql = s"insert into tb_planner_supply(wedding_id,planner_id,province,city,zone,hotel_name,wedding_date,budget_min,budget_max,can_num,itime) VALUES ('$wedding_id','$planner_id','$province','$city','$zone','$hotel_name','$wedding_date',$budget_min,$budget_max,$can_num,getdate());"
      st.executeUpdate(sql)
      // 调用副表存储方法
      savePlannnerSupplyAttach2SQLServer(wedding_id, budget_min, budget_max, st)
      JavaSQLServerConn.closeStatement(st)
      res = scanner.next()
    }
    JavaSQLServerConn.closeConnection(conn)
  }

  /**
    * 5.存储supply副表
    */
  def savePlannnerSupplyAttach2SQLServer(wedding_id: String, budget_min: String, budget_max: String, st: Statement): Unit = {
    var min = 0
    var max = 0
    try {
      min = budget_min.toInt
      max = budget_max.toInt
    } catch {
      case e: Exception => e.printStackTrace()
    }

    /**
      * 1.左右在区间边界上,不包含50000以上。
      */
    if ((min == 0 && max == 0) || (min == 0 && max == 12000) || (min == 12000 && max == 18000) || (min == 18000 && max == 25000) || (min == 25000 && max == 34000) || (min == 34000 && max == 50000)) {
      val sql = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,${min.toString},${max.toString},getdate());"
      st.executeUpdate(sql)
    } else {
      /**
        * 2.左右在区间边界中。
        */
      // 0-12000
      if ((min >= 0 && min <= 12000) && (max >= 0 && max <= 12000)) {
        val sql = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,0,12000,getdate());"
        st.executeUpdate(sql)
      }
      // 12000-18000
      if ((min >= 12000 && min <= 18000) && (max >= 12000 && max <= 18000)) {
        val sql = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,12000,18000,getdate());"
        st.executeUpdate(sql)
      }
      // 18000-25000
      if ((min >= 18000 && min <= 25000) && (max >= 18000 && max <= 25000)) {
        val sql = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,18000,25000,getdate());"
        st.executeUpdate(sql)
      }
      // 25000-34000
      if ((min >= 25000 && min <= 34000) && (max >= 25000 && max <= 34000)) {
        val sql = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,25000,34000,getdate());"
        st.executeUpdate(sql)
      }
      // 34000-50000
      if ((min >= 34000 && min <= 50000) && (max >= 34000 && max <= 50000)) {
        val sql = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,34000,50000,getdate());"
        st.executeUpdate(sql)
      }
      // 34000-50000
      if (min >= 50000 && max >= 50000) {
        val sql = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,50000,200000,getdate());"
        st.executeUpdate(sql)
      }

      /**
        * 3.跨越区间。
        */
      // 0-12000 || 12000-18000
      if (min < 12000 && (max > 12000 && max <= 18000)) {
        val sql = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,0,12000,getdate());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,12000,18000,getdate());"
        st.executeUpdate(sql2)
      }

      // 0-12000 || 12000-18000 || 18000-25000
      if (min < 12000 && (max > 18000 && max <= 25000)) {
        val sql = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,0,12000,getdate());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,12000,18000,getdate());"
        st.executeUpdate(sql2)
        val sql3 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',3,18000,25000,getdate());"
        st.executeUpdate(sql3)
      }

      // 0-12000 || 12000-18000 || 18000-25000 || 25000-34000
      if (min < 12000 && (max > 25000 && max <= 34000)) {
        val sql = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,0,12000,getdate());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,12000,18000,getdate());"
        st.executeUpdate(sql2)
        val sql3 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',3,18000,25000,getdate());"
        st.executeUpdate(sql3)
        val sql4 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',4,25000,34000,getdate());"
        st.executeUpdate(sql4)
      }

      // 0-12000 || 12000-18000 || 18000-25000 || 25000-34000 || 34000-50000
      if (min < 12000 && (max > 34000 && max <= 50000)) {
        val sql = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,0,12000,getdate());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,12000,18000,getdate());"
        st.executeUpdate(sql2)
        val sql3 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',3,18000,25000,getdate());"
        st.executeUpdate(sql3)
        val sql4 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',4,25000,34000,getdate());"
        st.executeUpdate(sql4)
        val sql5 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',5,34000,50000,getdate());"
        st.executeUpdate(sql5)
      }

      // 0-12000 || 12000-18000 || 18000-25000 || 25000-34000 || 34000-50000 || 50000-
      if (min < 12000 && max > 50000) {
        val sql = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,0,12000,getdate());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,12000,18000,getdate());"
        st.executeUpdate(sql2)
        val sql3 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',3,18000,25000,getdate());"
        st.executeUpdate(sql3)
        val sql4 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',4,25000,34000,getdate());"
        st.executeUpdate(sql4)
        val sql5 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',5,34000,50000,getdate());"
        st.executeUpdate(sql5)
        val sql6 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',6,50000,200000,getdate());"
        st.executeUpdate(sql6)
      }

      // 12000-18000 || 18000-25000
      if ((min >= 12000 && min < 18000) && (max > 18000 && max <= 25000)) {
        val sql = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,12000,18000,getdate());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,18000,25000,getdate());"
        st.executeUpdate(sql2)
      }

      // 12000-18000 || 18000-25000 || 25000-34000
      if ((min >= 12000 && min < 18000) && (max > 25000 && max <= 34000)) {
        val sql = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,12000,18000,getdate());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,18000,25000,getdate());"
        st.executeUpdate(sql2)
        val sql3 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',3,25000,34000,getdate());"
        st.executeUpdate(sql3)
      }

      // 12000-18000 || 18000-25000 || 25000-34000 || 34000-50000
      if ((min >= 12000 && min < 18000) && (max > 34000 && max <= 50000)) {
        val sql = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,12000,18000,getdate());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,18000,25000,getdate());"
        st.executeUpdate(sql2)
        val sql3 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',3,25000,34000,getdate());"
        st.executeUpdate(sql3)
        val sql4 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',4,34000,50000,getdate());"
        st.executeUpdate(sql4)
      }

      // 12000-18000 || 18000-25000 || 25000-34000 || 34000-50000 || 50000-
      if ((min >= 12000 && min < 18000) && (max > 50000)) {
        val sql = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,12000,18000,getdate());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,18000,25000,getdate());"
        st.executeUpdate(sql2)
        val sql3 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',3,25000,34000,getdate());"
        st.executeUpdate(sql3)
        val sql4 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',4,34000,50000,getdate());"
        st.executeUpdate(sql4)
        val sql5 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',5,50000,200000,getdate());"
        st.executeUpdate(sql5)
      }

      // 18000-25000 || 25000-34000
      if ((min >= 18000 && min < 25000) && (max > 25000 && max <= 34000)) {
        val sql = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,18000,25000,getdate());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,25000,34000,getdate());"
        st.executeUpdate(sql2)
      }

      // 18000-25000 || 25000-34000 || 34000-50000
      if ((min >= 18000 && min < 25000) && (max > 34000 && max <= 50000)) {
        val sql = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,18000,25000,getdate());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,25000,34000,getdate());"
        st.executeUpdate(sql2)
        val sql3 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',3,34000,50000,getdate());"
        st.executeUpdate(sql3)
      }

      // 18000-25000 || 25000-34000 || 34000-50000 || 50000-100000
      if ((min >= 18000 && min < 25000) && (max > 50000)) {
        val sql = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,18000,25000,getdate());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,25000,34000,getdate());"
        st.executeUpdate(sql2)
        val sql3 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',3,34000,50000,getdate());"
        st.executeUpdate(sql3)
        val sql4 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',4,50000,200000,getdate());"
        st.executeUpdate(sql4)
      }

      // 25000-34000 || 34000-50000
      if ((min >= 25000 && min < 34000) && (max > 34000 && max <= 50000)) {
        val sql = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,25000,34000,getdate());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,34000,50000,getdate());"
        st.executeUpdate(sql2)
      }

      // 25000-34000 || 34000-50000 || 50000-
      if ((min >= 25000 && min < 34000) && (max > 50000)) {
        val sql = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,25000,34000,getdate());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,34000,50000,getdate());"
        st.executeUpdate(sql2)
        val sql3 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',3,50000,200000,getdate());"
        st.executeUpdate(sql3)
      }

      // 34000-50000 || 50000-
      if ((min >= 34000 && min < 50000) && (max > 50000)) {
        val sql = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,34000,50000,getdate());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,50000,200000,getdate());"
        st.executeUpdate(sql2)
      }
    }
  }

  /**
    * 6.存储已预订策划师数据到SQLServer
    */
  def savePlannnerDemand2SQLServer(): Unit = {
    // todo scan读取HBase数据tb_planner_demand，然后逐条进行写入到sqlserver
    val scanner = JavaHBaseUtils.getScanner(JavaBangBangProperties.TBPLANNERDEMAND)
    val conn = JavaSQLServerConn.getConnection
    var res = scanner.next()
    while (res != null) {
      val wedding_id = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("wedding_id")))
      val planner_id = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("planner_id")))
      val province = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("province")))
      val city = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("city")))
      val zone = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("zone")))
      val hotel_name = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("hotel_name")))
      val wedding_date = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("wedding_date")))
      var budget_min = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("budget_min")))
      var budget_max = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("budget_max")))
      val done_num = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("done_num")))
      val st: Statement = conn.createStatement()

      if (budget_max.toInt < budget_min.toInt) {
        val next = budget_min
        budget_min = budget_max
        budget_max = next
      }
      val sql = s"insert into tb_planner_demand(wedding_id,planner_id,province,city,zone,hotel_name,wedding_date,budget_min,budget_max,done_num,itime) VALUES ('$wedding_id','$planner_id','$province','$city','$zone','$hotel_name','$wedding_date',$budget_min,$budget_max,$done_num,getdate());"
      st.executeUpdate(sql)
      // 调用副表存储方法
      savePlannnerDemandAttach2SQLServer(wedding_id, budget_min, budget_max, st)

      // TODO 查询此条数据在tb_planner_supply中是否存在，如果不存在，则写入
      val query = s"select * from tb_planner_supply where wedding_id = '$wedding_id' and wedding_date = '$wedding_date' and DateDiff(dd,itime,getdate())=0;"
      val end: ResultSet = st.executeQuery(query)
      var bool = false
      if (end.next()) {
        bool = true
      }
      if (!bool) { // bool不等于true，则往supply写数据。
        val sql = s"insert into tb_planner_supply(wedding_id,planner_id,province,city,zone,hotel_name,wedding_date,budget_min,budget_max,can_num,itime) VALUES ('$wedding_id','$planner_id','$province','$city','$zone','$hotel_name','$wedding_date',$budget_min,$budget_max,0,getdate());"
        st.executeUpdate(sql)
        // 调用副表存储方法
        savePlannnerSupplyAttach2SQLServer(wedding_id, budget_min, budget_max, st)
      }
      JavaSQLServerConn.closeStatement(st)
      res = scanner.next()
    }
    JavaSQLServerConn.closeConnection(conn)
  }

  /**
    * 6.存储demand副表
    */
  def savePlannnerDemandAttach2SQLServer(wedding_id: String, budget_min: String, budget_max: String, st: Statement): Unit = {
    var min = 0
    var max = 0
    try {
      min = budget_min.toInt
      max = budget_max.toInt
    } catch {
      case e: Exception => e.printStackTrace()
    }

    /**
      * 1.左右在区间边界上,不包含50000以上。
      */
    if ((min == 0 && max == 0) || (min == 0 && max == 12000) || (min == 12000 && max == 18000) || (min == 18000 && max == 25000) || (min == 25000 && max == 34000) || (min == 34000 && max == 50000)) {
      val sql = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,${min.toString},${max.toString},getdate());"
      st.executeUpdate(sql)
    } else {
      /**
        * 2.左右在区间边界中。
        */
      // 0-12000
      if ((min >= 0 && min <= 12000) && (max >= 0 && max <= 12000)) {
        val sql = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,0,12000,getdate());"
        st.executeUpdate(sql)
      }
      // 12000-18000
      if ((min >= 12000 && min <= 18000) && (max >= 12000 && max <= 18000)) {
        val sql = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,12000,18000,getdate());"
        st.executeUpdate(sql)
      }
      // 18000-25000
      if ((min >= 18000 && min <= 25000) && (max >= 18000 && max <= 25000)) {
        val sql = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,18000,25000,getdate());"
        st.executeUpdate(sql)
      }
      // 25000-34000
      if ((min >= 25000 && min <= 34000) && (max >= 25000 && max <= 34000)) {
        val sql = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,25000,34000,getdate());"
        st.executeUpdate(sql)
      }
      // 34000-50000
      if ((min >= 34000 && min <= 50000) && (max >= 34000 && max <= 50000)) {
        val sql = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,34000,50000,getdate());"
        st.executeUpdate(sql)
      }
      // 34000-50000
      if (min >= 50000 && max >= 50000) {
        val sql = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,50000,200000,getdate());"
        st.executeUpdate(sql)
      }

      /**
        * 3.跨越区间。
        */
      // 0-12000 || 12000-18000
      if (min < 12000 && (max > 12000 && max <= 18000)) {
        val sql = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,0,12000,getdate());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,12000,18000,getdate());"
        st.executeUpdate(sql2)
      }

      // 0-12000 || 12000-18000 || 18000-25000
      if (min < 12000 && (max > 18000 && max <= 25000)) {
        val sql = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,0,12000,getdate());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,12000,18000,getdate());"
        st.executeUpdate(sql2)
        val sql3 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',3,18000,25000,getdate());"
        st.executeUpdate(sql3)
      }

      // 0-12000 || 12000-18000 || 18000-25000 || 25000-34000
      if (min < 12000 && (max > 25000 && max <= 34000)) {
        val sql = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,0,12000,getdate());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,12000,18000,getdate());"
        st.executeUpdate(sql2)
        val sql3 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',3,18000,25000,getdate());"
        st.executeUpdate(sql3)
        val sql4 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',4,25000,34000,getdate());"
        st.executeUpdate(sql4)
      }

      // 0-12000 || 12000-18000 || 18000-25000 || 25000-34000 || 34000-50000
      if (min < 12000 && (max > 34000 && max <= 50000)) {
        val sql = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,0,12000,getdate());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,12000,18000,getdate());"
        st.executeUpdate(sql2)
        val sql3 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',3,18000,25000,getdate());"
        st.executeUpdate(sql3)
        val sql4 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',4,25000,34000,getdate());"
        st.executeUpdate(sql4)
        val sql5 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',5,34000,50000,getdate());"
        st.executeUpdate(sql5)
      }

      // 0-12000 || 12000-18000 || 18000-25000 || 25000-34000 || 34000-50000 || 50000-
      if (min < 12000 && max > 50000) {
        val sql = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,0,12000,getdate());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,12000,18000,getdate());"
        st.executeUpdate(sql2)
        val sql3 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',3,18000,25000,getdate());"
        st.executeUpdate(sql3)
        val sql4 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',4,25000,34000,getdate());"
        st.executeUpdate(sql4)
        val sql5 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',5,34000,50000,getdate());"
        st.executeUpdate(sql5)
        val sql6 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',6,50000,200000,getdate());"
        st.executeUpdate(sql6)
      }

      // 12000-18000 || 18000-25000
      if ((min >= 12000 && min < 18000) && (max > 18000 && max <= 25000)) {
        val sql = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,12000,18000,getdate());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,18000,25000,getdate());"
        st.executeUpdate(sql2)
      }

      // 12000-18000 || 18000-25000 || 25000-34000
      if ((min >= 12000 && min < 18000) && (max > 25000 && max <= 34000)) {
        val sql = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,12000,18000,getdate());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,18000,25000,getdate());"
        st.executeUpdate(sql2)
        val sql3 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',3,25000,34000,getdate());"
        st.executeUpdate(sql3)
      }

      // 12000-18000 || 18000-25000 || 25000-34000 || 34000-50000
      if ((min >= 12000 && min < 18000) && (max > 34000 && max <= 50000)) {
        val sql = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,12000,18000,getdate());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,18000,25000,getdate());"
        st.executeUpdate(sql2)
        val sql3 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',3,25000,34000,getdate());"
        st.executeUpdate(sql3)
        val sql4 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',4,34000,50000,getdate());"
        st.executeUpdate(sql4)
      }

      // 12000-18000 || 18000-25000 || 25000-34000 || 34000-50000 || 50000-
      if ((min >= 12000 && min < 18000) && (max > 50000)) {
        val sql = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,12000,18000,getdate());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,18000,25000,getdate());"
        st.executeUpdate(sql2)
        val sql3 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',3,25000,34000,getdate());"
        st.executeUpdate(sql3)
        val sql4 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',4,34000,50000,getdate());"
        st.executeUpdate(sql4)
        val sql5 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',5,50000,200000,getdate());"
        st.executeUpdate(sql5)
      }

      // 18000-25000 || 25000-34000
      if ((min >= 18000 && min < 25000) && (max > 25000 && max <= 34000)) {
        val sql = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,18000,25000,getdate());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,25000,34000,getdate());"
        st.executeUpdate(sql2)
      }

      // 18000-25000 || 25000-34000 || 34000-50000
      if ((min >= 18000 && min < 25000) && (max > 34000 && max <= 50000)) {
        val sql = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,18000,25000,getdate());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,25000,34000,getdate());"
        st.executeUpdate(sql2)
        val sql3 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',3,34000,50000,getdate());"
        st.executeUpdate(sql3)
      }

      // 18000-25000 || 25000-34000 || 34000-50000 || 50000-100000
      if ((min >= 18000 && min < 25000) && (max > 50000)) {
        val sql = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,18000,25000,getdate());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,25000,34000,getdate());"
        st.executeUpdate(sql2)
        val sql3 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',3,34000,50000,getdate());"
        st.executeUpdate(sql3)
        val sql4 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',4,50000,200000,getdate());"
        st.executeUpdate(sql4)
      }

      // 25000-34000 || 34000-50000
      if ((min >= 25000 && min < 34000) && (max > 34000 && max <= 50000)) {
        val sql = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,25000,34000,getdate());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,34000,50000,getdate());"
        st.executeUpdate(sql2)
      }

      // 25000-34000 || 34000-50000 || 50000-
      if ((min >= 25000 && min < 34000) && (max > 50000)) {
        val sql = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,25000,34000,getdate());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,34000,50000,getdate());"
        st.executeUpdate(sql2)
        val sql3 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',3,50000,200000,getdate());"
        st.executeUpdate(sql3)
      }

      // 34000-50000 || 50000-
      if ((min >= 34000 && min < 50000) && (max > 50000)) {
        val sql = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,34000,50000,getdate());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,50000,200000,getdate());"
        st.executeUpdate(sql2)
      }
    }

  }

  /**
    * 根据婚礼id，获取酒店ID，省市区信息，婚期，预算上下限。
    *
    * @param wedding_id
    */
  def getNeedDataByWeddingID(wedding_id: String): Array[String] = {
    val res = JavaHBaseUtils.getRow("v2_rp_tb_user_wedding", wedding_id)
    if (!res.isEmpty) {
      val wedding_date = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("wedding_date")))
      var budget_min = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("budget_min")))
      var budget_max = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("budget_max")))
      if (budget_min != null) {
        budget_min = (budget_min.toInt / 100).toString
      } else {
        budget_min = "0"
      }
      if (budget_max != null) {
        budget_max = (budget_max.toInt / 100).toString
      } else {
        budget_max = "0"
      }
      val hotel_id = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("hotel_id")))
      var province: String = null
      var city: String = null
      var zone: String = null
      var name: String = null
      if (hotel_id != null && hotel_id.length > 0) {
        val hotelRes = JavaHBaseUtils.getRow("v2_rp_tb_hotel", hotel_id)
        province = Bytes.toString(hotelRes.getValue(Bytes.toBytes("info"), Bytes.toBytes("province")))
        city = Bytes.toString(hotelRes.getValue(Bytes.toBytes("info"), Bytes.toBytes("city")))
        zone = Bytes.toString(hotelRes.getValue(Bytes.toBytes("info"), Bytes.toBytes("zone")))
        name = Bytes.toString(hotelRes.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")))
      }
      if (budget_max.toInt < budget_min.toInt) { // 数据异常，最大值 < 最小值
        Array(province, city, zone, name, wedding_date, budget_max, budget_min, hotel_id)
      } else {
        Array(province, city, zone, name, wedding_date, budget_min, budget_max, hotel_id)
      }
    } else {
      Array("")
    }
  }

  // 方法重载，使用新传入的hotel_id
  def getNeedDataByWeddingID(wedding_id: String, hotel_id: String): Array[String] = {
    val res = JavaHBaseUtils.getRow("v2_rp_tb_user_wedding", wedding_id)
    if (res != null) {
      val wedding_date = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("wedding_date")))
      var budget_min = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("budget_min")))
      var budget_max = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("budget_max")))
      if (budget_min != null) {
        budget_min = (budget_min.toInt / 100).toString
      } else {
        budget_min = "0"
      }
      if (budget_max != null) {
        budget_max = (budget_max.toInt / 100).toString
      } else {
        budget_max = "0"
      }
      val hotelRes = JavaHBaseUtils.getRow("v2_rp_tb_hotel", hotel_id)
      val province = Bytes.toString(hotelRes.getValue(Bytes.toBytes("info"), Bytes.toBytes("province")))
      val city = Bytes.toString(hotelRes.getValue(Bytes.toBytes("info"), Bytes.toBytes("city")))
      val zone = Bytes.toString(hotelRes.getValue(Bytes.toBytes("info"), Bytes.toBytes("zone")))
      val name = Bytes.toString(hotelRes.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")))

      if (budget_max.toInt < budget_min.toInt) { // 数据异常，最大值 < 最小值
        Array(province, city, zone, name, wedding_date, budget_max, budget_min, hotel_id)
      } else {
        Array(province, city, zone, name, wedding_date, budget_min, budget_max, hotel_id)
      }
    } else {
      Array("")
    }
  }

  def isNotTestData(wedding_id: String): Boolean = {
    val brokerArr = Array("70a0d230-2b6a-4f32-8afc-a4379bbfca41",
      "864a6295-828b-4e22-9989-bfc2962efc4d",
      "aada4b53-d257-453f-848a-27869bad753a",
      "b411ec25-c2d2-11e7-864b-7cd30ab79bd4",
      "e7ed2c90-dc48-4595-a1bf-3725be1b1a68",
      "55d760af-ccbb-4c27-94b7-d555f4542361", // 07.11后加
      "bc4a7dc9-a1c6-460c-b370-ec3ab7d871a9") // 测试数据
    val statusArr = Array("0", "4", "5")
    val ceshi = "测试"
    val result: Result = JavaHBaseUtils.getRow("v2_rp_tb_user_wedding", wedding_id)
    val status = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("status")))
    val broker_id = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("broker_id")))
    val hotel = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("hotel")))
    val contacts_name = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("contacts_name")))
    if (brokerArr.contains(broker_id) || statusArr.contains(status) || hotel.contains(ceshi) || contacts_name.contains(ceshi)) {
      return false
    } else {
      return true
    }
  }


  /**
    * 统计已预订和可预订策划师信息，并写入到MySQL
    *
    * @since 06/17/2020 by hiwes
    */
  def readAndWriteDemandSupplyData2MySQL(): Unit = {
    val start = System.currentTimeMillis()
    //    // 1.清空原来的数据表
    //    truncateTableInHBase()
    //    val step1 = System.currentTimeMillis()
    //    println("______1.清空数据表用时: " + (step1 - start) + " ms.")
    //
    //    // 2.读取MySQL数据库，对tb_user_wedding_team_member表进行统计，得到已预订策划师并存入tb_planner_demand
    //    savePlannerDemandData2HBase()
    //    val step2 = System.currentTimeMillis()
    //    println("______2.写入已预订策划师数据到HBase: " + (step2 - step1) + " ms.")
    //
    //    // 3.读取v2_rp_tb_planner_recom_record，针对utime在20190819之前的数据进行统计，得到可预订策划师历史数据并存入tb_planner_supply；
    //    saveHistoryRecordData2HBase()
    //    val step3 = System.currentTimeMillis()
    //    println("______3.写入历史可预订策划师数据到HBase: " + (step3 - step2) + " ms.")
    //
    //    // 4.读取HBase数据库，对recom_planner_result表进行统计，得到可预订策划师并存入tb_planner_supply
    //    saveNowRecordData2HBase()
    //    val step4 = System.currentTimeMillis()
    //    println("______4.写入可预订策划师数据到HBase: " + (step4 - step3) + " ms.")

    // 5.读取tb_planner_supply，写入SQLServer
    savePlannnerSupply2MySQL()
    val step5 = System.currentTimeMillis()
    //    println("______5.写入可预订策划师数据到mysql: " + (step5 - step4) + " ms.")
    println("______5.写入可预订策划师数据到mysql: " + (step5 - start) + " ms.")

    // 6.读取tb_planner_demand，写入SQLServer
    savePlannnerDemand2MySQL()
    val step6 = System.currentTimeMillis()
    println("______6.写入已预订策划师数据到mysql: " + (step6 - step5) + " ms.")

  }

  /**
    * 5.存储可预订策划师数据到MySQL
    */
  def savePlannnerSupply2MySQL(): Unit = {
    // todo scan读取HBase数据tb_planner_supply，然后逐条进行写入到sqlserver
    val scanner = JavaHBaseUtils.getScanner(JavaBangBangProperties.TBPLANNERSUPPLY)
    val conn = BangBangDataMySQLUtils.getConnection
    var res = scanner.next()
    while (res != null) {
      val wedding_id = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("wedding_id")))
      val planner_id = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("planner_id")))
      val province = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("province")))
      val city = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("city")))
      val zone = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("zone")))
      val hotel_name = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("hotel_name")))
      val wedding_date = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("wedding_date")))
      var budget_min = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("budget_min")))
      var budget_max = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("budget_max")))
      val can_num = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("can_num")))
      // todo
      if (can_num.equals("0")) println(Bytes.toString(res.getRow))

      val st: Statement = conn.createStatement();
      if (budget_max.toInt < budget_min.toInt) {
        val next = budget_min
        budget_min = budget_max
        budget_max = next
      }
      val sql = s"insert into tb_planner_supply(wedding_id,planner_id,province,city,zone,hotel_name,wedding_date,budget_min,budget_max,can_num,itime) VALUES ('$wedding_id','$planner_id','$province','$city','$zone','$hotel_name','$wedding_date',$budget_min,$budget_max,$can_num,now());"
      st.executeUpdate(sql)
      // 调用副表存储方法
      savePlannnerSupplyAttach2MySQL(wedding_id, budget_min, budget_max, st)
      BangBangDataMySQLUtils.closeStatement(st)
      res = scanner.next()
    }
    BangBangDataMySQLUtils.closeConnection(conn)
  }

  /**
    * 5.存储supply副表
    */
  def savePlannnerSupplyAttach2MySQL(wedding_id: String, budget_min: String, budget_max: String, st: Statement): Unit = {
    var min = 0
    var max = 0
    try {
      min = budget_min.toInt
      max = budget_max.toInt
    } catch {
      case e: Exception => e.printStackTrace()
    }

    /**
      * 1.左右在区间边界上,不包含50000以上。
      */
    if ((min == 0 && max == 0) || (min == 0 && max == 12000) || (min == 12000 && max == 18000) || (min == 18000 && max == 25000) || (min == 25000 && max == 34000) || (min == 34000 && max == 50000)) {
      val sql = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,${min.toString},${max.toString},now());"
      st.executeUpdate(sql)
    } else {
      /**
        * 2.左右在区间边界中。
        */
      // 0-12000
      if ((min >= 0 && min <= 12000) && (max >= 0 && max <= 12000)) {
        val sql = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,0,12000,now());"
        st.executeUpdate(sql)
      }
      // 12000-18000
      if ((min >= 12000 && min <= 18000) && (max >= 12000 && max <= 18000)) {
        val sql = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,12000,18000,now());"
        st.executeUpdate(sql)
      }
      // 18000-25000
      if ((min >= 18000 && min <= 25000) && (max >= 18000 && max <= 25000)) {
        val sql = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,18000,25000,now());"
        st.executeUpdate(sql)
      }
      // 25000-34000
      if ((min >= 25000 && min <= 34000) && (max >= 25000 && max <= 34000)) {
        val sql = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,25000,34000,now());"
        st.executeUpdate(sql)
      }
      // 34000-50000
      if ((min >= 34000 && min <= 50000) && (max >= 34000 && max <= 50000)) {
        val sql = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,34000,50000,now());"
        st.executeUpdate(sql)
      }
      // 34000-50000
      if (min >= 50000 && max >= 50000) {
        val sql = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,50000,200000,now());"
        st.executeUpdate(sql)
      }

      /**
        * 3.跨越区间。
        */
      // 0-12000 || 12000-18000
      if (min < 12000 && (max > 12000 && max <= 18000)) {
        val sql = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,0,12000,now());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,12000,18000,now());"
        st.executeUpdate(sql2)
      }

      // 0-12000 || 12000-18000 || 18000-25000
      if (min < 12000 && (max > 18000 && max <= 25000)) {
        val sql = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,0,12000,now());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,12000,18000,now());"
        st.executeUpdate(sql2)
        val sql3 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',3,18000,25000,now());"
        st.executeUpdate(sql3)
      }

      // 0-12000 || 12000-18000 || 18000-25000 || 25000-34000
      if (min < 12000 && (max > 25000 && max <= 34000)) {
        val sql = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,0,12000,now());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,12000,18000,now());"
        st.executeUpdate(sql2)
        val sql3 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',3,18000,25000,now());"
        st.executeUpdate(sql3)
        val sql4 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',4,25000,34000,now());"
        st.executeUpdate(sql4)
      }

      // 0-12000 || 12000-18000 || 18000-25000 || 25000-34000 || 34000-50000
      if (min < 12000 && (max > 34000 && max <= 50000)) {
        val sql = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,0,12000,now());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,12000,18000,now());"
        st.executeUpdate(sql2)
        val sql3 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',3,18000,25000,now());"
        st.executeUpdate(sql3)
        val sql4 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',4,25000,34000,now());"
        st.executeUpdate(sql4)
        val sql5 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',5,34000,50000,now());"
        st.executeUpdate(sql5)
      }

      // 0-12000 || 12000-18000 || 18000-25000 || 25000-34000 || 34000-50000 || 50000-
      if (min < 12000 && max > 50000) {
        val sql = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,0,12000,now());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,12000,18000,now());"
        st.executeUpdate(sql2)
        val sql3 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',3,18000,25000,now());"
        st.executeUpdate(sql3)
        val sql4 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',4,25000,34000,now());"
        st.executeUpdate(sql4)
        val sql5 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',5,34000,50000,now());"
        st.executeUpdate(sql5)
        val sql6 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',6,50000,200000,now());"
        st.executeUpdate(sql6)
      }

      // 12000-18000 || 18000-25000
      if ((min >= 12000 && min < 18000) && (max > 18000 && max <= 25000)) {
        val sql = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,12000,18000,now());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,18000,25000,now());"
        st.executeUpdate(sql2)
      }

      // 12000-18000 || 18000-25000 || 25000-34000
      if ((min >= 12000 && min < 18000) && (max > 25000 && max <= 34000)) {
        val sql = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,12000,18000,now());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,18000,25000,now());"
        st.executeUpdate(sql2)
        val sql3 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',3,25000,34000,now());"
        st.executeUpdate(sql3)
      }

      // 12000-18000 || 18000-25000 || 25000-34000 || 34000-50000
      if ((min >= 12000 && min < 18000) && (max > 34000 && max <= 50000)) {
        val sql = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,12000,18000,now());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,18000,25000,now());"
        st.executeUpdate(sql2)
        val sql3 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',3,25000,34000,now());"
        st.executeUpdate(sql3)
        val sql4 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',4,34000,50000,now());"
        st.executeUpdate(sql4)
      }

      // 12000-18000 || 18000-25000 || 25000-34000 || 34000-50000 || 50000-
      if ((min >= 12000 && min < 18000) && (max > 50000)) {
        val sql = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,12000,18000,now());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,18000,25000,now());"
        st.executeUpdate(sql2)
        val sql3 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',3,25000,34000,now());"
        st.executeUpdate(sql3)
        val sql4 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',4,34000,50000,now());"
        st.executeUpdate(sql4)
        val sql5 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',5,50000,200000,now());"
        st.executeUpdate(sql5)
      }

      // 18000-25000 || 25000-34000
      if ((min >= 18000 && min < 25000) && (max > 25000 && max <= 34000)) {
        val sql = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,18000,25000,now());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,25000,34000,now());"
        st.executeUpdate(sql2)
      }

      // 18000-25000 || 25000-34000 || 34000-50000
      if ((min >= 18000 && min < 25000) && (max > 34000 && max <= 50000)) {
        val sql = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,18000,25000,now());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,25000,34000,now());"
        st.executeUpdate(sql2)
        val sql3 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',3,34000,50000,now());"
        st.executeUpdate(sql3)
      }

      // 18000-25000 || 25000-34000 || 34000-50000 || 50000-100000
      if ((min >= 18000 && min < 25000) && (max > 50000)) {
        val sql = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,18000,25000,now());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,25000,34000,now());"
        st.executeUpdate(sql2)
        val sql3 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',3,34000,50000,now());"
        st.executeUpdate(sql3)
        val sql4 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',4,50000,200000,now());"
        st.executeUpdate(sql4)
      }

      // 25000-34000 || 34000-50000
      if ((min >= 25000 && min < 34000) && (max > 34000 && max <= 50000)) {
        val sql = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,25000,34000,now());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,34000,50000,now());"
        st.executeUpdate(sql2)
      }

      // 25000-34000 || 34000-50000 || 50000-
      if ((min >= 25000 && min < 34000) && (max > 50000)) {
        val sql = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,25000,34000,now());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,34000,50000,now());"
        st.executeUpdate(sql2)
        val sql3 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',3,50000,200000,now());"
        st.executeUpdate(sql3)
      }

      // 34000-50000 || 50000-
      if ((min >= 34000 && min < 50000) && (max > 50000)) {
        val sql = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,34000,50000,now());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_supply_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,50000,200000,now());"
        st.executeUpdate(sql2)
      }
    }
  }

  /**
    * 6.存储已预订策划师数据到SQLServer
    */
  def savePlannnerDemand2MySQL(): Unit = {
    val ymd: String = JavaDateUtils.getDateOnlyYMD
    // todo scan读取HBase数据tb_planner_demand，然后逐条进行写入到sqlserver
    val scanner = JavaHBaseUtils.getScanner(JavaBangBangProperties.TBPLANNERDEMAND)
    val conn = BangBangDataMySQLUtils.getConnection
    var res = scanner.next()
    while (res != null) {
      val wedding_id = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("wedding_id")))
      val planner_id = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("planner_id")))
      val province = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("province")))
      val city = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("city")))
      val zone = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("zone")))
      val hotel_name = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("hotel_name")))
      val wedding_date = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("wedding_date")))
      var budget_min = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("budget_min")))
      var budget_max = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("budget_max")))
      val done_num = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("done_num")))
      val st: Statement = conn.createStatement()

      if (budget_max.toInt < budget_min.toInt) {
        val next = budget_min
        budget_min = budget_max
        budget_max = next
      }
      val sql = s"insert into tb_planner_demand(wedding_id,planner_id,province,city,zone,hotel_name,wedding_date,budget_min,budget_max,done_num,itime) VALUES ('$wedding_id','$planner_id','$province','$city','$zone','$hotel_name','$wedding_date',$budget_min,$budget_max,$done_num,now());"
      st.executeUpdate(sql)
      // 调用副表存储方法
      savePlannnerDemandAttach2MySQL(wedding_id, budget_min, budget_max, st)

      // TODO 查询此条数据在tb_planner_supply中是否存在，如果不存在，则写入
      val query = s"select * from tb_planner_supply where wedding_id = '$wedding_id' and wedding_date = '$wedding_date' and DateDiff(itime,'$ymd')=0;"
      val end: ResultSet = st.executeQuery(query)
      var bool = false
      if (end.next()) {
        bool = true
      }
      if (!bool) { // bool不等于true，则往supply写数据。
        val sql = s"insert into tb_planner_supply(wedding_id,planner_id,province,city,zone,hotel_name,wedding_date,budget_min,budget_max,can_num,itime) VALUES ('$wedding_id','$planner_id','$province','$city','$zone','$hotel_name','$wedding_date',$budget_min,$budget_max,0,now());"
        st.executeUpdate(sql)
        // 调用副表存储方法
        savePlannnerSupplyAttach2MySQL(wedding_id, budget_min, budget_max, st)
      }
      BangBangDataMySQLUtils.closeStatement(st)
      res = scanner.next()
    }
    BangBangDataMySQLUtils.closeConnection(conn)
  }

  /**
    * 6.存储demand副表
    */
  def savePlannnerDemandAttach2MySQL(wedding_id: String, budget_min: String, budget_max: String, st: Statement): Unit = {
    var min = 0
    var max = 0
    try {
      min = budget_min.toInt
      max = budget_max.toInt
    } catch {
      case e: Exception => e.printStackTrace()
    }

    /**
      * 1.左右在区间边界上,不包含50000以上。
      */
    if ((min == 0 && max == 0) || (min == 0 && max == 12000) || (min == 12000 && max == 18000) || (min == 18000 && max == 25000) || (min == 25000 && max == 34000) || (min == 34000 && max == 50000)) {
      val sql = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,${min.toString},${max.toString},now());"
      st.executeUpdate(sql)
    } else {
      /**
        * 2.左右在区间边界中。
        */
      // 0-12000
      if ((min >= 0 && min <= 12000) && (max >= 0 && max <= 12000)) {
        val sql = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,0,12000,now());"
        st.executeUpdate(sql)
      }
      // 12000-18000
      if ((min >= 12000 && min <= 18000) && (max >= 12000 && max <= 18000)) {
        val sql = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,12000,18000,now());"
        st.executeUpdate(sql)
      }
      // 18000-25000
      if ((min >= 18000 && min <= 25000) && (max >= 18000 && max <= 25000)) {
        val sql = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,18000,25000,now());"
        st.executeUpdate(sql)
      }
      // 25000-34000
      if ((min >= 25000 && min <= 34000) && (max >= 25000 && max <= 34000)) {
        val sql = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,25000,34000,now());"
        st.executeUpdate(sql)
      }
      // 34000-50000
      if ((min >= 34000 && min <= 50000) && (max >= 34000 && max <= 50000)) {
        val sql = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,34000,50000,now());"
        st.executeUpdate(sql)
      }
      // 34000-50000
      if (min >= 50000 && max >= 50000) {
        val sql = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,50000,200000,now());"
        st.executeUpdate(sql)
      }

      /**
        * 3.跨越区间。
        */
      // 0-12000 || 12000-18000
      if (min < 12000 && (max > 12000 && max <= 18000)) {
        val sql = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,0,12000,now());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,12000,18000,now());"
        st.executeUpdate(sql2)
      }

      // 0-12000 || 12000-18000 || 18000-25000
      if (min < 12000 && (max > 18000 && max <= 25000)) {
        val sql = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,0,12000,now());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,12000,18000,now());"
        st.executeUpdate(sql2)
        val sql3 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',3,18000,25000,now());"
        st.executeUpdate(sql3)
      }

      // 0-12000 || 12000-18000 || 18000-25000 || 25000-34000
      if (min < 12000 && (max > 25000 && max <= 34000)) {
        val sql = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,0,12000,now());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,12000,18000,now());"
        st.executeUpdate(sql2)
        val sql3 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',3,18000,25000,now());"
        st.executeUpdate(sql3)
        val sql4 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',4,25000,34000,now());"
        st.executeUpdate(sql4)
      }

      // 0-12000 || 12000-18000 || 18000-25000 || 25000-34000 || 34000-50000
      if (min < 12000 && (max > 34000 && max <= 50000)) {
        val sql = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,0,12000,now());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,12000,18000,now());"
        st.executeUpdate(sql2)
        val sql3 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',3,18000,25000,now());"
        st.executeUpdate(sql3)
        val sql4 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',4,25000,34000,now());"
        st.executeUpdate(sql4)
        val sql5 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',5,34000,50000,now());"
        st.executeUpdate(sql5)
      }

      // 0-12000 || 12000-18000 || 18000-25000 || 25000-34000 || 34000-50000 || 50000-
      if (min < 12000 && max > 50000) {
        val sql = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,0,12000,now());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,12000,18000,now());"
        st.executeUpdate(sql2)
        val sql3 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',3,18000,25000,now());"
        st.executeUpdate(sql3)
        val sql4 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',4,25000,34000,now());"
        st.executeUpdate(sql4)
        val sql5 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',5,34000,50000,now());"
        st.executeUpdate(sql5)
        val sql6 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',6,50000,200000,now());"
        st.executeUpdate(sql6)
      }

      // 12000-18000 || 18000-25000
      if ((min >= 12000 && min < 18000) && (max > 18000 && max <= 25000)) {
        val sql = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,12000,18000,now());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,18000,25000,now());"
        st.executeUpdate(sql2)
      }

      // 12000-18000 || 18000-25000 || 25000-34000
      if ((min >= 12000 && min < 18000) && (max > 25000 && max <= 34000)) {
        val sql = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,12000,18000,now());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,18000,25000,now());"
        st.executeUpdate(sql2)
        val sql3 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',3,25000,34000,now());"
        st.executeUpdate(sql3)
      }

      // 12000-18000 || 18000-25000 || 25000-34000 || 34000-50000
      if ((min >= 12000 && min < 18000) && (max > 34000 && max <= 50000)) {
        val sql = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,12000,18000,now());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,18000,25000,now());"
        st.executeUpdate(sql2)
        val sql3 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',3,25000,34000,now());"
        st.executeUpdate(sql3)
        val sql4 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',4,34000,50000,now());"
        st.executeUpdate(sql4)
      }

      // 12000-18000 || 18000-25000 || 25000-34000 || 34000-50000 || 50000-
      if ((min >= 12000 && min < 18000) && (max > 50000)) {
        val sql = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,12000,18000,now());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,18000,25000,now());"
        st.executeUpdate(sql2)
        val sql3 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',3,25000,34000,now());"
        st.executeUpdate(sql3)
        val sql4 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',4,34000,50000,now());"
        st.executeUpdate(sql4)
        val sql5 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',5,50000,200000,now());"
        st.executeUpdate(sql5)
      }

      // 18000-25000 || 25000-34000
      if ((min >= 18000 && min < 25000) && (max > 25000 && max <= 34000)) {
        val sql = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,18000,25000,now());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,25000,34000,now());"
        st.executeUpdate(sql2)
      }

      // 18000-25000 || 25000-34000 || 34000-50000
      if ((min >= 18000 && min < 25000) && (max > 34000 && max <= 50000)) {
        val sql = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,18000,25000,now());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,25000,34000,now());"
        st.executeUpdate(sql2)
        val sql3 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',3,34000,50000,now());"
        st.executeUpdate(sql3)
      }

      // 18000-25000 || 25000-34000 || 34000-50000 || 50000-100000
      if ((min >= 18000 && min < 25000) && (max > 50000)) {
        val sql = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,18000,25000,now());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,25000,34000,now());"
        st.executeUpdate(sql2)
        val sql3 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',3,34000,50000,now());"
        st.executeUpdate(sql3)
        val sql4 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',4,50000,200000,now());"
        st.executeUpdate(sql4)
      }

      // 25000-34000 || 34000-50000
      if ((min >= 25000 && min < 34000) && (max > 34000 && max <= 50000)) {
        val sql = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,25000,34000,now());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,34000,50000,now());"
        st.executeUpdate(sql2)
      }

      // 25000-34000 || 34000-50000 || 50000-
      if ((min >= 25000 && min < 34000) && (max > 50000)) {
        val sql = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,25000,34000,now());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,34000,50000,now());"
        st.executeUpdate(sql2)
        val sql3 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',3,50000,200000,now());"
        st.executeUpdate(sql3)
      }

      // 34000-50000 || 50000-
      if ((min >= 34000 && min < 50000) && (max > 50000)) {
        val sql = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',1,34000,50000,now());"
        st.executeUpdate(sql)
        val sql2 = s"insert into tb_planner_demand_attach(wedding_id,number,budget_min,budget_max,itime) VALUES ('$wedding_id',2,50000,200000,now());"
        st.executeUpdate(sql2)
      }
    }

  }

  def main(args: Array[String]): Unit = {
    val original = System.currentTimeMillis()
    readAndWriteDemandSupplyData2SQLServer()
    val start = System.currentTimeMillis()
    readAndWriteDemandSupplyData2MySQL
    val stop = System.currentTimeMillis()
    println("总共存入sqlserver用时: " + (start - original) / 1000 + "s.")
    println("总共存入mysql用时: " + (stop - start) / 1000 + "s.")
  }

}
