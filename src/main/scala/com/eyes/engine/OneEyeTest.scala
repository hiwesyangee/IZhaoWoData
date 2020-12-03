package com.eyes.engine

import java.sql.Connection

import com.izhaowo.cores.utils.{JavaHBaseUtils, JavaSQLServerConn, JavaSparkUtils}
import com.realleader.properties.JavaRealLeaderProperties
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame

object OneEyeTest {

  val spark = JavaSparkUtils.getInstance().getSparkSession

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hbase").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.kafka").setLevel(Level.WARN)

    val start = System.currentTimeMillis()
    dataTransfer2
    val stop = System.currentTimeMillis()
    println("用时: " + (stop - start) / 1000 + "s.")
  }

  /**
    * 整体流程入口。
    */
  def dataTransfer(): Unit = {
    val s1 = System.currentTimeMillis()
    val allNeedDf = getEyesOriginalDataFromMySQL // 1. 获取原始数据并缓存
    allNeedDf.cache()
    val s2 = System.currentTimeMillis()
    val smVector: Vector[SmallMarketInfo] = getEyesOriginalSmallMarketFromMySQL // 2.获取原始小市场
    //    val smVector: Vector[SmallMarketInfo] = Vector(SmallMarketInfo("57290e8c-adab-48c6-ae2f-96c397a8aa28", 2, "西蜀森林酒店(西南1门)")) // 2.获取原始小市场
    val s3 = System.currentTimeMillis()
    val lastVector = filterDataFromOriginalData(allNeedDf, smVector) // 3.进行数据过滤
    println("3vector.size=" + lastVector.size)
    val conn = JavaSQLServerConn.getConnection // 4.进行SQLServer连接
    val s4 = System.currentTimeMillis()
    saveData2SQLServer(lastVector, conn) // 5.进行数据写入
    val s5 = System.currentTimeMillis()
    JavaSQLServerConn.closeConnection(conn) // 6.关闭SQLServer连接
    allNeedDf.unpersist()
    println("1.time = " + (s2 - s1) + "ms.")
    println("2.time = " + (s3 - s2) + "ms.")
    println("3.time = " + (s4 - s3) + "ms.")
    println("4.time = " + (s5 - s4) + "ms.")
  }

  /**
    * 1.读取原始指标数据
    *
    * @return
    */
  def getEyesOriginalDataFromMySQL(): DataFrame = {
    val tbLowBudgetTypeDF = spark.read.format("jdbc")
      .option("url", JavaRealLeaderProperties.MYSQLURL)
      .option("dbtable", "tb_low_budget_type")
      .option("user", JavaRealLeaderProperties.MYSQLUSER)
      .option("password", JavaRealLeaderProperties.MYSQLPASSWORD)
      .load()
      .withColumnRenamed("到店签单率", "to_store_rate")
      .withColumnRenamed("策划师文字评价率", "text_rating_rate")
    tbLowBudgetTypeDF.createOrReplaceTempView("bbb")

    val dfTotalDF = spark.read.format("jdbc")
      .option("url", JavaRealLeaderProperties.MYSQLURL)
      .option("dbtable", "df_total")
      .option("user", JavaRealLeaderProperties.MYSQLUSER)
      .option("password", JavaRealLeaderProperties.MYSQLPASSWORD)
      .load()
    dfTotalDF.createOrReplaceTempView("ddd")
    val allNeedDf: DataFrame = spark.sql(s"select bbb.worker_id,ddd.section,bbb.case_dot,bbb.reorder_rate,bbb.communication_level,bbb.design_sense,bbb.case_rate,bbb.all_score_final,bbb.number,bbb.to_store_rate,bbb.text_rating_rate,ddd.display_amount,bbb.sot from bbb left join ddd on bbb.worker_id = ddd.worker_id").distinct()
    allNeedDf
  }

  case class SmallMarketInfo(hotel_id: String, section: Int, hotel_name: String)

  /**
    * 2.读取原始小市场数据
    */
  def getEyesOriginalSmallMarketFromMySQL(): Vector[SmallMarketInfo] = {
    val hotelSectionDF = spark.read.format("jdbc")
      .option("url", JavaRealLeaderProperties.MYSQLURL)
      .option("dbtable", "hotel_cannum_section")
      .option("user", JavaRealLeaderProperties.MYSQLUSER)
      .option("password", JavaRealLeaderProperties.MYSQLPASSWORD)
      .load()

    val resultDf = hotelSectionDF.filter(row => {
      row.getAs[Long]("can_num") > 0 &&
        row.getAs[String]("hotel_id").equals("59b8d93d-6914-4446-a10f-e97898045766")
    }).select("hotel_id", "section").distinct()

    val resultVec = resultDf.collect().toVector

    var vector: Vector[SmallMarketInfo] = Vector[SmallMarketInfo]()
    resultVec.foreach(row => {
      val hotel_id = row.getAs[String]("hotel_id")
      val section = row.getAs[String]("section")
      val hotel_name = JavaHBaseUtils.getValue("v2_rp_tb_hotel", hotel_id, "info", "name")
      if (hotel_name != null) {
        vector = vector ++ Vector[SmallMarketInfo](SmallMarketInfo(hotel_id, section.toInt, hotel_name))
      }
    })
    vector
  }

  case class EyesPlannerIndicators(hotel_id: String, section: Int, hotel_name: String, planner_id: String, case_dot: Double, reorder_rate: Double,
                                   communication_level: Double, design_sense: Double, case_rate: Double, all_score_final: Double,
                                   number: Double, text_rating_rate: Double, display_amount: Double, to_store_rate: Double)

  /**
    * 3.进行数据过滤和组装
    *
    * @param allNeedDf
    * @param smVector
    */
  def filterDataFromOriginalData(allNeedDf: DataFrame, smVector: Vector[SmallMarketInfo]): Vector[EyesPlannerIndicators] = {
    var vector = Vector[EyesPlannerIndicators]()
    try {
      // todo 1.遍历小市场，并生成对应的DataFrame
      for (sm: SmallMarketInfo <- smVector) {
        val hotel_id = sm.hotel_id
        val section = sm.section
        val hotel_name = sm.hotel_name

        // 生成每个小市场对应的策划师指标
        val needDf: DataFrame = allNeedDf.filter(row => {
          val sec = row.getAs[Long]("section").toInt
          sec == section
        }).filter(row => {
          val worker_id = row.getAs[String]("worker_id")
          val distance = JavaHBaseUtils.getValue("v2_rp_workers_hotel_distance", hotel_id + ":" + worker_id, "info", "distance")
          (distance != null && !distance.equals("") && (distance.toFloat / 1000) <= 20)
        })

        // 对每个小市场数据进行遍历
        needDf.collect().foreach(row => {
          val planner_id = row.getAs[String]("worker_id")
          val case_dot = row.getAs[Double]("case_dot")
          val reorder_rate = row.getAs[Double]("reorder_rate")
          val communication_level = row.getAs[Double]("communication_level")
          val design_sense = row.getAs[Double]("design_sense")
          val case_rate = row.getAs[Double]("case_rate")
          val all_score_final = row.getAs[Double]("all_score_final")
          val number = row.getAs[Double]("number")
          val text_rating_rate = row.getAs[Double]("text_rating_rate")
          val display_amount = row.getAs[Double]("display_amount")
          val to_store_rate = row.getAs[Double]("to_store_rate")
          vector = vector ++ Vector(EyesPlannerIndicators(hotel_id, section, hotel_name, planner_id, case_dot, reorder_rate, communication_level, design_sense, case_rate, all_score_final, number, text_rating_rate, display_amount, to_store_rate))
        })
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
    vector
  }

  /**
    * 4.进行数据存储
    *
    * @param resultVector
    * @param conn
    */
  def saveData2SQLServer(resultVector: Vector[EyesPlannerIndicators], conn: Connection): Unit = {
    var i = 0
    //设置批量处理的数量
    val batchSize = 5000
    val stmt = conn.prepareStatement(s"insert into daily_planner_indicators(hotel_id,section,hotel_name,planner_id,case_dot,reorder_rate,communication_level,design_sense,case_rate,all_score_final,number,text_rating_rate,display_amount,to_store_rate,itime) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,getdate())")
    conn.setAutoCommit(false)
    for (result <- resultVector) {
      i = i + 1;
      println("-------写入时：打印hotel_id：" + result.hotel_id)
      println("_______写入时，打印section：" + result.section)
      stmt.setString(1, result.hotel_id)
      stmt.setInt(2, result.section)
      stmt.setString(3, result.hotel_name)
      stmt.setString(4, result.planner_id)
      stmt.setFloat(5, result.case_dot.toFloat)
      stmt.setFloat(6, result.reorder_rate.toFloat)
      stmt.setFloat(7, result.communication_level.toFloat)
      stmt.setFloat(8, result.design_sense.toFloat)
      stmt.setFloat(9, result.case_rate.toFloat)
      stmt.setFloat(10, result.all_score_final.toFloat)
      stmt.setFloat(11, result.number.toFloat)
      stmt.setFloat(12, result.text_rating_rate.toFloat)
      stmt.setFloat(13, result.display_amount.toFloat)
      stmt.setFloat(14, result.to_store_rate.toFloat)
      stmt.addBatch()
      if (i % batchSize == 0) {
        stmt.executeBatch
        conn.commit
      }
    }
    if (i % batchSize != 0) {
      stmt.executeBatch
      conn.commit
    }
    JavaSQLServerConn.closeStatement(stmt)
  }

  /**
    * 直接进行数据过滤并进行数据写入
    *
    * @param allNeedDf
    * @param smVector
    */
  def filterAndSaveData(allNeedDf: DataFrame, smVector: Vector[SmallMarketInfo]): Unit = {
    var vector = Vector[EyesPlannerIndicators]()
    val conn = JavaSQLServerConn.getConnection // 4.进行SQLServer连接
    try {
      var size = 0
      // todo 1.遍历小市场，并生成对应的DataFrame
      for (sm: SmallMarketInfo <- smVector) {
        size = size + 1
        val hotel_id = sm.hotel_id
        val section = sm.section
        val hotel_name = sm.hotel_name

        // 生成每个小市场对应的策划师指标
        val needDf: DataFrame = allNeedDf.filter(row => {
          val sec = row.getAs[Long]("section").toInt
          sec == section
        }).filter(row => {
          val worker_id = row.getAs[String]("worker_id")
          val distance = JavaHBaseUtils.getValue("v2_rp_workers_hotel_distance", hotel_id + ":" + worker_id, "info", "distance")
          (distance != null && !distance.equals("") && (distance.toFloat / 1000) <= 20)
        })

        println("——————————存在一个距离小于等于20的策划师。")

        // 对每个小市场数据进行遍历
        needDf.collect().foreach(row => {
          val planner_id = row.getAs[String]("worker_id")
          val case_dot = row.getAs[Double]("case_dot")
          val reorder_rate = row.getAs[Double]("reorder_rate")
          val communication_level = row.getAs[Double]("communication_level")
          val design_sense = row.getAs[Double]("design_sense")
          val case_rate = row.getAs[Double]("case_rate")
          val all_score_final = row.getAs[Double]("all_score_final")
          val number = row.getAs[Double]("number")
          val text_rating_rate = row.getAs[Double]("text_rating_rate")
          val display_amount = row.getAs[Double]("display_amount")
          val to_store_rate = row.getAs[Double]("to_store_rate")
          vector = vector ++ Vector(EyesPlannerIndicators(hotel_id, section, hotel_name, planner_id, case_dot, reorder_rate, communication_level, design_sense, case_rate, all_score_final, number, text_rating_rate, display_amount, to_store_rate))
        })

        // todo 循环数量100个小市场提交一次
        if (size % 100 == 0) {
          saveData2SQLServer(vector, conn)
          println("_______写入一次。。。")
          vector = Vector[EyesPlannerIndicators]()
        }
      }
      // todo 剩余小市场提交一次
      saveData2SQLServer(vector, conn)
      println("_______写入一次。。。")
      vector = null
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (conn != null) {
        JavaSQLServerConn.closeConnection(conn) // 6.关闭SQLServer连接
      }
    }
  }

  /**
    * 整体流程入口2。
    */
  def dataTransfer2(): Unit = {
    val s1 = System.currentTimeMillis()
    val allNeedDf = getEyesOriginalDataFromMySQL // 1. 获取原始数据并缓存
    allNeedDf.cache()
    val s2 = System.currentTimeMillis()
    val smVector: Vector[SmallMarketInfo] = getEyesOriginalSmallMarketFromMySQL // 2.获取原始小市场
    //    val smVector: Vector[SmallMarketInfo] = Vector(SmallMarketInfo("57290e8c-adab-48c6-ae2f-96c397a8aa28", 2, "西蜀森林酒店(西南1门)")) // 2.获取原始小市场
    val s3 = System.currentTimeMillis()
    filterAndSaveData(allNeedDf, smVector) // 3.进行数据过滤并分批写入
    val s4 = System.currentTimeMillis()
    allNeedDf.unpersist()
    println(" 1.time = " + (s2 - s1) + "ms.")
    println("————打印allNeedDf.size = " + allNeedDf.count())
    println(" 2.time = " + (s3 - s2) + "ms.")
    println("————打印smVector.size = " + smVector.size)
    println(" 3.time = " + (s4 - s3) + "ms.")
    println("————打印smVector.size = " + smVector.size)
  }
}
