package com.realleaderAndyanjing.engine

import java.sql.{Connection, Statement}

import com.izhaowo.cores.utils.{JavaDateUtils, JavaHBaseUtils, JavaSQLServerConn, JavaSparkUtils}
import com.realleader.properties.JavaRealLeaderProperties
import com.yanjing.engine.{JavaSomeUtils, SomeUtils}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{HTable, Put, ResultScanner}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.collection.mutable

/**
  * 自定义RealLeader和YanJing Engine引擎类
  */
object RealLeaderAndYanJingEngine {
  val spark = JavaSparkUtils.getInstance().getSparkSession

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hbase").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.kafka").setLevel(Level.WARN)

    val start = System.currentTimeMillis()
    //    realLeaderDataStatistics
    //    yanjingDataStatistics
    eyesData

    val stop = System.currentTimeMillis()
    println("用时: " + (stop - start) / 1000 + "s.")
  }

  /**
    * RealLeader数据统计
    */
  def realLeaderDataStatistics(): Unit = {
    val start = System.currentTimeMillis()
    val originalDf = getOriginalDataFromKouDB()
    val need = originalDf.filter(row => {
      (row.getAs[String]("hotel_id").equals("c0aa9212-9aed-4801-800d-8c412e972c49")) &&
        (row.getAs[Int]("section") == 4)
    })

    need.show(false)
    println(need.count())

    val stop = System.currentTimeMillis()
    //    saveOriginalData2HBase(originalDf)
    val stop2 = System.currentTimeMillis()

    println("tim1 = " + (stop - start) + "ms.")
    println("tim2 = " + (stop2 - stop) + "ms.")
  }

  // 1-1 读取寇耀数据库3张表并进行数据关联
  def getOriginalDataFromKouDB(): DataFrame = {
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
    val allNeedDf = spark.sql(s"select bbb.worker_id,ddd.section,bbb.case_dot,bbb.reorder_rate,bbb.communication_level,bbb.design_sense,bbb.case_rate,bbb.all_score_final,bbb.number,bbb.to_store_rate,bbb.text_rating_rate,ddd.display_amount,bbb.sot from bbb left join ddd on bbb.worker_id = ddd.worker_id").distinct()
    allNeedDf.createOrReplaceTempView("aaa")
    val tbLowBudgetHotel1 = spark.read.format("jdbc")
      .option("url", JavaRealLeaderProperties.MYSQLURL)
      .option("dbtable", "tb_low_budget_hotel1")
      .option("user", JavaRealLeaderProperties.MYSQLUSER)
      .option("password", JavaRealLeaderProperties.MYSQLPASSWORD)
      .load()
    tbLowBudgetHotel1.createOrReplaceTempView("hhh")

    val resultDf = spark.sql(s"select hhh.hotel_id,hhh.section,hhh.worker_id,hhh.sot,hhh.distance,aaa.case_dot,aaa.reorder_rate,aaa.communication_level,aaa.design_sense,aaa.case_rate,aaa.all_score_final,aaa.number,aaa.text_rating_rate,aaa.display_amount,aaa.to_store_rate from aaa left join hhh on aaa.worker_id = hhh.worker_id").distinct()
    val endDf = resultDf.filter(row => {
      val hotel_id = row.getAs[String]("hotel_id")
      hotel_id.length == 36
    })
    endDf
  }

  // 1-2 将所有数据写入到HBase
  def saveOriginalData2HBase(originalDf: DataFrame): Unit = {
    val itime = JavaDateUtils.getDateYMDNew

    originalDf.foreachPartition(ite => {
      val myConf = HBaseConfiguration.create()
      myConf.set("hbase.zookeeper.quorum", "master")
      myConf.set("hbase.zookeeper.property.clientPort", "2181")
      myConf.set("hbase.defaults.for.version.skip", "true")
      val myTable = new HTable(myConf, TableName.valueOf("mv_tb_small_market_planner_indicator_old"))
      myTable.setAutoFlush(false, false) //关键点1
      myTable.setWriteBufferSize(5 * 1024 * 1024) //关键点2
      try {
        ite.foreach { row => {
          val hotel_id = row.getAs[String]("hotel_id") // 1
          val section = row.getAs[Int]("section").toString // 2
          val worker_id = row.getAs[String]("worker_id") // 3
          val hotel_name = JavaHBaseUtils.getValue("v2_rp_tb_hotel", hotel_id, "info", "name")
          //          val hotel_name = "testName"
          val sort = row.getAs[Double]("sot").toString // 5
          val distance = row.getAs[Long]("distance").toString // 6
          var case_dot = row.getAs[Double]("case_dot") // 7
          if (case_dot == null) case_dot = 0d
          var reorder_rate = row.getAs[Double]("reorder_rate") // 8
          if (reorder_rate == null) reorder_rate = 0d
          var communication_level = row.getAs[Double]("communication_level") // 9
          if (communication_level == null) communication_level = 0d
          var design_sense = row.getAs[Double]("design_sense") // 10
          if (design_sense == null) design_sense = 0d
          var case_rate = row.getAs[Double]("case_rate") // 11
          if (case_rate == null) case_rate = 0d
          var all_score_final = row.getAs[Double]("all_score_final") // 12
          if (all_score_final == null) all_score_final = 0d
          var number = row.getAs[Double]("number") // 13
          if (number == null) number = 0d
          var text_rating_rate = row.getAs[Double]("text_rating_rate") // 14
          if (text_rating_rate == null) text_rating_rate = 0d
          var display_amount = row.getAs[Double]("display_amount") // 15
          if (display_amount == null) display_amount = 0d
          var to_store_rate = row.getAs[Double]("to_store_rate") // 16
          if (to_store_rate == null) to_store_rate = 0d

          val rowkey = hotel_id + "=" + section + "=" + worker_id
          val put = new Put(Bytes.toBytes(rowkey))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("hotel_id"), Bytes.toBytes(hotel_id))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("section"), Bytes.toBytes(section))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("worker_id"), Bytes.toBytes(worker_id))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("hotel_name"), Bytes.toBytes(hotel_name))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("sort"), Bytes.toBytes(sort))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("distance"), Bytes.toBytes(distance))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("case_dot"), Bytes.toBytes(case_dot.toString))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("reorder_rate"), Bytes.toBytes(reorder_rate.toString))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("communication_level"), Bytes.toBytes(communication_level.toString))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("design_sense"), Bytes.toBytes(design_sense.toString))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("case_rate"), Bytes.toBytes(case_rate.toString))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("all_score_final"), Bytes.toBytes(all_score_final.toString))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("number"), Bytes.toBytes(number.toString))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("text_rating_rate"), Bytes.toBytes(text_rating_rate.toString))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("display_amount"), Bytes.toBytes(display_amount.toString))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("to_store_rate"), Bytes.toBytes(to_store_rate.toString))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("itime"), Bytes.toBytes(itime))
          myTable.put(put)
        }
        }
        myTable.flushCommits() //关键点3
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        if (myTable != null) {
          try {
            myTable.close()
          } catch {
            case e: Exception => e.printStackTrace()
          }
        }
        if (myConf != null) {
          try {
            myConf.clear()
          } catch {
            case e: Exception => e.printStackTrace()
          }
        }
      }
      //        ite.foreach(row => {
      //
      //          try {
      //            val hotel_id = row.getAs[String]("hotel_id") // 1
      //            val section = row.getAs[Int]("section").toString // 2
      //            val worker_id = row.getAs[String]("worker_id") // 3
      //             val hotel_name = JavaHBaseUtils.getValue("v2_rp_tb_hotel", hotel_id, "info", "name")
      //            val sort = row.getAs[Double]("sot").toString // 5
      //            val distance = row.getAs[Long]("distance").toString // 6
      //            var case_dot = row.getAs[Double]("case_dot") // 7
      //            if (case_dot == null) case_dot = 0d
      //            var reorder_rate = row.getAs[Double]("reorder_rate") // 8
      //            if (reorder_rate == null) reorder_rate = 0d
      //            var communication_level = row.getAs[Double]("communication_level") // 9
      //            if (communication_level == null) communication_level = 0d
      //            var design_sense = row.getAs[Double]("design_sense") // 10
      //            if (design_sense == null) design_sense = 0d
      //            var case_rate = row.getAs[Double]("case_rate") // 11
      //            if (case_rate == null) case_rate = 0d
      //            var all_score_final = row.getAs[Double]("all_score_final") // 12
      //            if (all_score_final == null) all_score_final = 0d
      //            var number = row.getAs[Double]("number") // 13
      //            if (number == null) number = 0d
      //            var text_rating_rate = row.getAs[Double]("text_rating_rate") // 14
      //            if (text_rating_rate == null) text_rating_rate = 0d
      //            var display_amount = row.getAs[Double]("display_amount") // 15
      //            if (display_amount == null) display_amount = 0d
      //            var to_store_rate = row.getAs[Double]("to_store_rate") // 16
      //            if (to_store_rate == null) to_store_rate = 0d
      //
      //            val rowkey = hotel_id + "=" + section + "=" + worker_id
      //            val put = new Put(Bytes.toBytes(rowkey))
      //            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("hotel_id"), Bytes.toBytes(hotel_id))
      //            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("section"), Bytes.toBytes(section))
      //            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("worker_id"), Bytes.toBytes(worker_id))
      //            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("hotel_name"), Bytes.toBytes(hotel_name))
      //            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("sort"), Bytes.toBytes(sort))
      //            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("distance"), Bytes.toBytes(distance))
      //            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("case_dot"), Bytes.toBytes(case_dot.toString))
      //            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("reorder_rate"), Bytes.toBytes(reorder_rate.toString))
      //            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("communication_level"), Bytes.toBytes(communication_level.toString))
      //            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("design_sense"), Bytes.toBytes(design_sense.toString))
      //            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("case_rate"), Bytes.toBytes(case_rate.toString))
      //            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("all_score_final"), Bytes.toBytes(all_score_final.toString))
      //            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("number"), Bytes.toBytes(number.toString))
      //            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("text_rating_rate"), Bytes.toBytes(text_rating_rate.toString))
      //            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("display_amount"), Bytes.toBytes(display_amount.toString))
      //            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("to_store_rate"), Bytes.toBytes(to_store_rate.toString))
      //            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("itime"), Bytes.toBytes(itime))
      //            val list = new util.ArrayList[Put]()
      //            list.add(put)
      //            JavaHBaseUtils.putRows("test", list)
      //
      //          } catch {
      //            case e: Exception => println("老数据写入HBase失败. ")
      //          }
      //        })

    })
  }

  /** yanjingDataStatistics // todo 弃用
    * Yanjing数据统计
    */
  def yanjingDataStatistics(): Unit = {
    // todo 针对不同小市场，进行数据统计和数据写入
    // 1.读取指定小市场的数据，批量扫描HBase表并进行数据采集
    //    val list: List[SmallMarketPlannerIndicators] = getHBaseData4Planner("a9fe4b14-5f45-47c2-bc35-4d36fcdaae05", "交子国际酒店", 2)
    //    val list = getHBaseData4AllSmallMarketCaseDot
    //    saveData2SQLServer(list)
    //    val conn = JavaSQLServerConn.getConnection
    //    val st: Statement = conn.createStatement()
    // 2.完成策划师数量统计并进行数据库写入

    //    dataStatisticsAndWrite2SQLServer(list, st, "a9fe4b14-5f45-47c2-bc35-4d36fcdaae05", "交子国际酒店", 2)
    //    JavaSQLServerConn.closeConnection(conn)

    val originalDf: DataFrame = getYanJingOriginalDataFromMySQL()
    originalDf.cache()
    println("数据查找结束")
    dataStatistics(originalDf) // 1.case_dot

    originalDf.unpersist()
  }

  case class SmallMarketPlannerIndicators(hotel_id: String, section: Int, hotel_name: String, case_dot: Double,
                                          reorder_rate: Double, communication_level: Double, design_sense: Double,
                                          all_score_final: Double, case_rate: Double, number: Double, text_rating_rate: Double,
                                          display_amount: Double, to_store_rate: Double, distance: Long, worker_id: String)

  // 2-1 从HBase中获取所有的该小市场的所有策划师指标 // todo 弃用
  def getHBaseData4Planner(hotel_id: String, hotel_name: String, section: Int): List[SmallMarketPlannerIndicators] = {
    var list = List[SmallMarketPlannerIndicators]()
    val start = hotel_id + "=" + section.toString + "=0"
    val stop = hotel_id + "=" + section.toString + "=zzzzzzzzzzz"
    try {
      val rs: ResultScanner = JavaHBaseUtils.getScanner("mv_tb_small_market_planner_indicator_old", start, stop)

      var result = rs.next()
      while (result != null) {
        val case_dot = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("case_dot"))).toDouble
        val reorder_rate = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("reorder_rate"))).toDouble
        val communication_level = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("communication_level"))).toDouble
        val design_sense = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("design_sense"))).toDouble
        val all_score_final = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("all_score_final"))).toDouble
        val case_rate = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("case_rate"))).toDouble
        val number = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("number"))).toDouble
        val text_rating_rate = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("text_rating_rate"))).toDouble
        val display_amount = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("display_amount"))).toDouble
        val to_store_rate = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("to_store_rate"))).toDouble
        val distance = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("distance"))).toLong
        val worker_id = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("worker_id")))

        // 基于距离策略的对象组装
        if (distance <= 20000) list = list.++(List[SmallMarketPlannerIndicators](new SmallMarketPlannerIndicators(hotel_id, section, hotel_name, case_dot, reorder_rate, communication_level, design_sense, all_score_final, case_rate, number, text_rating_rate, display_amount, to_store_rate, distance, worker_id)))
        result = rs.next()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
    list
  }

  // 2-2 从指标List中进行数量统计并进行数据库写入 // todo 弃用
  def dataStatisticsAndWrite2SQLServer(list: List[SmallMarketPlannerIndicators], st: Statement, hotel_id: String, hotel_name: String, section: Int): Unit = {
    try {
      val allInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
      val displayAmountInterval = SomeUtils.getSplitIntervalSetting(0, 5000, 100, "[]")

      // 1.
      var resetList: List[Double] = list.map(_.case_dot)
      var caseDotMap = SomeUtils.getRightIntervalBySetting(resetList, allInterval)
      caseDotMap.foreach(a => {
        val case_dot = JavaSomeUtils.get2Double(a._1.toDouble)
        val num = a._2
        val other = "Vj ≠ 1"
        val sql = s"insert into canNum_v1_case_dot(hotel_id,hotel_name,section,case_dot,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$case_dot,'$other',$num,getdate());"
        st.executeUpdate(sql)
      })
      caseDotMap = null

      // 2.
      resetList = list.map(_.reorder_rate)
      var reorderRateMap = SomeUtils.getRightIntervalBySetting(resetList, allInterval)
      reorderRateMap.foreach(a => {
        val case_dot = JavaSomeUtils.get2Double(a._1.toDouble)
        val num = a._2
        val other = "Vj ≠ 2"
        val sql = s"insert into canNum_v2_reorder_rate(hotel_id,hotel_name,section,reorder_rate,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$case_dot,'$other',$num,getdate());"
        st.executeUpdate(sql)
      })
      reorderRateMap = null

      // 3.
      resetList = list.map(_.communication_level)
      var communicationLevelMap = SomeUtils.getRightIntervalBySetting(resetList, allInterval)
      communicationLevelMap.foreach(a => {
        val communication_level = JavaSomeUtils.get2Double(a._1.toDouble)
        val num = a._2
        val other = "Vj ≠ 3"
        val sql = s"insert into canNum_v3_communication_level(hotel_id,hotel_name,section,communication_level,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$communication_level,'$other',$num,getdate());"
        st.executeUpdate(sql)
      })
      communicationLevelMap = null

      // 4.
      resetList = list.map(_.design_sense)
      var designSenseMap = SomeUtils.getRightIntervalBySetting(resetList, allInterval)
      designSenseMap.foreach(a => {
        val design_sense = JavaSomeUtils.get2Double(a._1.toDouble)
        val num = a._2
        val other = "Vj ≠ 4"
        val sql = s"insert into canNum_v4_design_sense(hotel_id,hotel_name,section,design_sense,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$design_sense,'$other',$num,getdate());"
        st.executeUpdate(sql)
      })
      designSenseMap = null

      // 5.
      resetList = list.map(_.case_rate)
      var caseRateMap = SomeUtils.getRightIntervalBySetting(resetList, allInterval)
      caseRateMap.foreach(a => {
        val case_rate = JavaSomeUtils.get2Double(a._1.toDouble)
        val num = a._2
        val other = "Vj ≠ 5"
        val sql = s"insert into canNum_v5_case_rate(hotel_id,hotel_name,section,case_rate,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$case_rate,'$other',$num,getdate());"
        st.executeUpdate(sql)
      })
      caseRateMap = null

      // 6.
      resetList = list.map(_.all_score_final)
      var allScoreFinalMap = SomeUtils.getRightIntervalBySetting(resetList, allInterval)
      allScoreFinalMap.foreach(a => {
        val all_score_final = JavaSomeUtils.get2Double(a._1.toDouble)
        val num = a._2
        val other = "Vj ≠ 6"
        val sql = s"insert into canNum_v6_all_score_final(hotel_id,hotel_name,section,all_score_final,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$all_score_final,'$other',$num,getdate());"
        st.executeUpdate(sql)
      })
      allScoreFinalMap = null

      // 7.
      resetList = list.map(_.number)
      var numberMap = SomeUtils.getRightIntervalBySetting(resetList, allInterval)
      numberMap.foreach(a => {
        val number = JavaSomeUtils.get2Double(a._1.toDouble)
        val num = a._2
        val other = "Vj ≠ 7"
        val sql = s"insert into canNum_v7_number(hotel_id,hotel_name,section,number,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$number,'$other',$num,getdate());"
        st.executeUpdate(sql)
      })
      numberMap = null

      // 8.
      resetList = list.map(_.text_rating_rate)
      var textRatingRateMap = SomeUtils.getRightIntervalBySetting(resetList, allInterval)

      textRatingRateMap.foreach(a => {
        val text_rating_rate = JavaSomeUtils.get2Double(a._1.toDouble)
        val num = a._2
        val other = "Vj ≠ 8"
        val sql = s"insert into canNum_v8_text_rating_rate(hotel_id,hotel_name,section,text_rating_rate,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$text_rating_rate,'$other',$num,getdate());"
        st.executeUpdate(sql)
      })
      textRatingRateMap = null

      // 9.
      resetList = list.map(_.display_amount)
      var displayAmountMap = SomeUtils.getRightIntervalBySetting(resetList, displayAmountInterval)

      displayAmountMap.foreach(a => {
        val display_amount = JavaSomeUtils.get2Double(a._1.toDouble)
        val num = a._2
        val other = "Vj ≠ 9"
        val sql = s"insert into canNum_v9_display_amount(hotel_id,hotel_name,section,display_amount,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$display_amount,'$other',$num,getdate());"
        st.executeUpdate(sql)
      })
      displayAmountMap = null

      // 10.
      resetList = list.map(_.to_store_rate)
      var toStoreRateMap = SomeUtils.getRightIntervalBySetting(resetList, allInterval)

      toStoreRateMap.foreach(a => {
        val to_store_rate = JavaSomeUtils.get2Double(a._1.toDouble)
        val num = a._2
        val other = "Vj ≠ 10"
        val sql = s"insert into canNum_v10_to_store_rate(hotel_id,hotel_name,section,to_store_rate,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$to_store_rate,'$other',$num,getdate());"
        st.executeUpdate(sql)
      })
      toStoreRateMap = null
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  // todo 1 将所有的小市场的1-case_dot都找出来进行统计 // todo 弃用
  def getHBaseData4AllSmallMarketCaseDot(): List[Tuple3[String, String, Map[String, Int]]] = {
    var resultList = List[Tuple3[String, String, Map[String, Int]]]()

    val allInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
    for (sp <- strategyPolyhedronList) {
      val row = sp._1
      val name = sp._2
      val start = row + "=0"
      val stop = row + "=zzzzzzzzzzzzzz"

      val rs = JavaHBaseUtils.getScanner("mv_tb_small_market_planner_indicator_old", start, stop)
      var result = rs.next()

      var caseDotList = List[Double]()
      while (result != null) {
        val case_dot = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("case_dot"))).toDouble
        caseDotList = caseDotList.++(List[Double](case_dot))
        // 基于距离策略的对象组装
        result = rs.next()
      }
      val caseDotMap: Map[String, Int] = SomeUtils.getRightIntervalBySetting(caseDotList, allInterval)
      resultList = resultList.++(List[Tuple3[String, String, Map[String, Int]]](Tuple3(row, name, caseDotMap)))
    }
    resultList
  }

  // todo 2 批量写入小市场的case_dot统计表。// todo 弃用
  def saveData2SQLServer(list: List[Tuple3[String, String, Map[String, Int]]]): Unit = {
    val conn = JavaSQLServerConn.getConnection
    var i = 0
    //设置批量处理的数量
    val batchSize = 5000
    val stmt = conn.prepareStatement("insert into canNum_v1_case_dot(hotel_id,hotel_name,section,case_dot,other_condition,can_num,itime) VALUES (?,?,?,?,?,?,getdate())")
    conn.setAutoCommit(false)
    for (inL <- list) {
      val hotel_id = inL._1.split("=")(0)
      val section = inL._1.split("=")(1).toInt
      val hotel_name = inL._2
      for (map <- inL._3) {
        i = i + 1;
        val case_dot = map._1.toFloat
        val number = map._2
        stmt.setString(1, hotel_id)
        stmt.setString(2, hotel_name)
        stmt.setInt(3, section)
        stmt.setFloat(4, case_dot)
        stmt.setString(5, "Vj≠1")
        stmt.setInt(6, number)
        stmt.addBatch()
        if (i % batchSize == 0) {
          stmt.executeBatch
          conn.commit
        }
      }
    }
    if (i % batchSize != 0) {
      stmt.executeBatch
      conn.commit
    }

    JavaSQLServerConn.closeStatement(stmt)
    JavaSQLServerConn.closeConnection(conn)
  }

  /**
    * 03-27 新增。眼睛数据重绘图
    */
  def getYanJingOriginalDataFromMySQL(): DataFrame = {
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
    val allNeedDf = spark.sql(s"select bbb.worker_id,ddd.section,bbb.case_dot,bbb.reorder_rate,bbb.communication_level,bbb.design_sense,bbb.case_rate,bbb.all_score_final,bbb.number,bbb.to_store_rate,bbb.text_rating_rate,ddd.display_amount,bbb.sot from bbb left join ddd on bbb.worker_id = ddd.worker_id").distinct()
    allNeedDf
  }

  // todo 弃用
  // 1.统计case_dot数据
  def dataStatistics(originalDf: DataFrame): Unit = {
    val conn = JavaSQLServerConn.getConnection
    val otherInterval: List[Double] = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
    val displayAmountInterval = SomeUtils.getSplitIntervalSetting(0, 5000, 100, "[]")

    var tup3CaseDotList: List[Tuple3[String, String, Map[String, Int]]] = List()
    var tup3ReorderRateList: List[Tuple3[String, String, Map[String, Int]]] = List()
    var tup3CommunicationLevelList: List[Tuple3[String, String, Map[String, Int]]] = List()
    var tup3DesignSenseList: List[Tuple3[String, String, Map[String, Int]]] = List()
    var tup3CaseRateList: List[Tuple3[String, String, Map[String, Int]]] = List()
    var tup3AllScoreFinalList: List[Tuple3[String, String, Map[String, Int]]] = List()
    var tup3NumberList: List[Tuple3[String, String, Map[String, Int]]] = List()
    var tup3TextRatingRateList: List[Tuple3[String, String, Map[String, Int]]] = List()
    var tup3DisplayAmountList: List[Tuple3[String, String, Map[String, Int]]] = List()
    var tup3ToStoreRateList: List[Tuple3[String, String, Map[String, Int]]] = List()
    for (list <- strategyPolyhedronList) {
      val hotel_id = list._1.split("=")(0)
      val section = list._1.split("=")(1).toInt
      val needDf: Dataset[Row] = originalDf.filter(row => {
        val sec = row.getAs[Long]("section").toInt
        sec == section
      }).filter(row => {
        val worker_id = row.getAs[String]("worker_id")
        val distance = JavaHBaseUtils.getValue("v2_rp_workers_hotel_distance", hotel_id + ":" + worker_id, "info", "distance")
        val distanceKM = (distance.toFloat / 1000)
        distanceKM <= 20
      })

      // 1.
      val caseDotList: List[Double] = needDf.select("case_dot").rdd.map(row => row.getAs[Double]("case_dot")).collect().toList
      val caseDotMap = SomeUtils.getRightIntervalBySetting(caseDotList, otherInterval)
      tup3CaseDotList = tup3CaseDotList.++(List[Tuple3[String, String, Map[String, Int]]](new Tuple3(list._1, list._2, caseDotMap)))
      // 2.
      val reorderRateList: List[Double] = needDf.select("reorder_rate").rdd.map(row => row.getAs[Double]("reorder_rate")).collect().toList
      val reorderRateMap = SomeUtils.getRightIntervalBySetting(reorderRateList, otherInterval)
      tup3ReorderRateList = tup3ReorderRateList.++(List[Tuple3[String, String, Map[String, Int]]](new Tuple3(list._1, list._2, reorderRateMap)))
      // 3.
      val communicationLevelList: List[Double] = needDf.select("communication_level").rdd.map(row => row.getAs[Double]("communication_level")).collect().toList
      val communicationLevelMap = SomeUtils.getRightIntervalBySetting(communicationLevelList, otherInterval)
      tup3CommunicationLevelList = tup3CommunicationLevelList.++(List[Tuple3[String, String, Map[String, Int]]](new Tuple3(list._1, list._2, communicationLevelMap)))
      // 4.
      val designSenseList: List[Double] = needDf.select("design_sense").rdd.map(row => row.getAs[Double]("design_sense")).collect().toList
      val designSenseMap = SomeUtils.getRightIntervalBySetting(designSenseList, otherInterval)
      tup3DesignSenseList = tup3DesignSenseList.++(List[Tuple3[String, String, Map[String, Int]]](new Tuple3(list._1, list._2, designSenseMap)))
      // 5.
      val caseRateList: List[Double] = needDf.select("case_rate").rdd.map(row => row.getAs[Double]("case_rate")).collect().toList
      val caseRateMap = SomeUtils.getRightIntervalBySetting(caseRateList, otherInterval)
      tup3CaseRateList = tup3CaseRateList.++(List[Tuple3[String, String, Map[String, Int]]](new Tuple3(list._1, list._2, caseRateMap)))
      // 6.
      val allScoreFinalList: List[Double] = needDf.select("all_score_final").rdd.map(row => row.getAs[Double]("all_score_final")).collect().toList
      val allScoreFinalMap = SomeUtils.getRightIntervalBySetting(allScoreFinalList, otherInterval)
      tup3AllScoreFinalList = tup3AllScoreFinalList.++(List[Tuple3[String, String, Map[String, Int]]](new Tuple3(list._1, list._2, allScoreFinalMap)))
      // 7.
      val numberList: List[Double] = needDf.select("number").rdd.map(row => row.getAs[Double]("number")).collect().toList
      val numberMap = SomeUtils.getRightIntervalBySetting(numberList, otherInterval)
      tup3NumberList = tup3NumberList.++(List[Tuple3[String, String, Map[String, Int]]](new Tuple3(list._1, list._2, numberMap)))
      // 8.
      val textRatingRateList: List[Double] = needDf.select("text_rating_rate").rdd.map(row => row.getAs[Double]("text_rating_rate")).collect().toList
      val textRatingRateMap = SomeUtils.getRightIntervalBySetting(textRatingRateList, otherInterval)
      tup3TextRatingRateList = tup3TextRatingRateList.++(List[Tuple3[String, String, Map[String, Int]]](new Tuple3(list._1, list._2, textRatingRateMap)))
      // 9.
      val displayAmountList: List[Double] = needDf.select("display_amount").rdd.map(row => row.getAs[Double]("display_amount")).collect().toList
      val displayAmountMap = SomeUtils.getRightIntervalBySetting(displayAmountList, displayAmountInterval)
      tup3DisplayAmountList = tup3DisplayAmountList.++(List[Tuple3[String, String, Map[String, Int]]](new Tuple3(list._1, list._2, displayAmountMap)))
      // 10.
      val toStoreRateList: List[Double] = needDf.select("to_store_rate").rdd.map(row => row.getAs[Double]("to_store_rate")).collect().toList
      val toStoreRateMap = SomeUtils.getRightIntervalBySetting(toStoreRateList, otherInterval)
      tup3ToStoreRateList = tup3ToStoreRateList.++(List[Tuple3[String, String, Map[String, Int]]](new Tuple3(list._1, list._2, toStoreRateMap)))
    }

    println("开始写入数据")
    saveData2SQLServerMatchName(tup3CaseDotList, "case_dot", conn)
    saveData2SQLServerMatchName(tup3ReorderRateList, "reorder_rate", conn)
    saveData2SQLServerMatchName(tup3CommunicationLevelList, "communication_level", conn)
    saveData2SQLServerMatchName(tup3DesignSenseList, "design_sense", conn)
    saveData2SQLServerMatchName(tup3CaseRateList, "case_rate", conn)
    saveData2SQLServerMatchName(tup3AllScoreFinalList, "all_score_final", conn)
    saveData2SQLServerMatchName(tup3NumberList, "number", conn)
    saveData2SQLServerMatchName(tup3TextRatingRateList, "text_rating_rate", conn)
    saveData2SQLServerMatchName(tup3DisplayAmountList, "display_amount", conn)
    saveData2SQLServerMatchName(tup3ToStoreRateList, "to_store_rate", conn)

    JavaSQLServerConn.closeConnection(conn)
  }

  // todo 2 批量写入小市场的case_dot统计表。 // todo 弃用
  def saveData2SQLServerMatchName(list: List[Tuple3[String, String, Map[String, Int]]], name: String, conn: Connection): Unit = {
    var i = 0
    //设置批量处理的数量
    val batchSize = 5000
    var tableName = ""
    var field = ""
    var other_condition = ""
    name match {
      case "case_dot" => tableName = "canNum_v1_case_dot"; field = "case_dot"; other_condition = "Vj≠1"
      case "reorder_rate" => tableName = "canNum_v2_reorder_rate"; field = "reorder_rate"; other_condition = "Vj≠2"
      case "communication_level" => tableName = "canNum_v3_communication_level"; field = "communication_level"; other_condition = "Vj≠3"
      case "design_sense" => tableName = "canNum_v4_design_sense"; field = "design_sense"; other_condition = "Vj≠4"
      case "case_rate" => tableName = "canNum_v5_case_rate"; field = "case_rate"; other_condition = "Vj≠5"
      case "all_score_final" => tableName = "canNum_v6_all_score_final"; field = "all_score_final"; other_condition = "Vj≠6"
      case "number" => tableName = "canNum_v7_number"; field = "number"; other_condition = "Vj≠7"
      case "text_rating_rate" => tableName = "canNum_v8_text_rating_rate"; field = "text_rating_rate"; other_condition = "Vj≠8"
      case "display_amount" => tableName = "canNum_v9_display_amount"; field = "display_amount"; other_condition = "Vj≠9"
      case "to_store_rate" => tableName = "canNum_v10_to_store_rate"; field = "to_store_rate"; other_condition = "Vj≠10"
      case _ =>
    }
    val stmt = conn.prepareStatement(s"insert into $tableName(hotel_id,hotel_name,section,$field,other_condition,can_num,itime) VALUES (?,?,?,?,?,?,getdate())")
    conn.setAutoCommit(false)
    for (inL <- list) {
      val hotel_id = inL._1.split("=")(0)
      val section = inL._1.split("=")(1).toInt
      val hotel_name = inL._2
      for (map <- inL._3) {
        i = i + 1;
        val inField = map._1.toFloat
        val number = map._2
        stmt.setString(1, hotel_id)
        stmt.setString(2, hotel_name)
        stmt.setInt(3, section)
        stmt.setFloat(4, inField)
        stmt.setString(5, other_condition)
        stmt.setInt(6, number)
        stmt.addBatch()
        if (i % batchSize == 0) {
          stmt.executeBatch
          conn.commit
        }
      }
    }
    if (i % batchSize != 0) {
      stmt.executeBatch
      conn.commit
    }

    JavaSQLServerConn.closeStatement(stmt)
    //    JavaSQLServerConn.closeConnection(conn)
  }

  //
  //  // 2.统计reorder_rate数据
  //  def dataStatistics4ReorderRate(originalDf: DataFrame, caseDotInterval: List[Double]): Unit = {
  //    var tup3List: List[Tuple3[String, String, Map[String, Int]]] = List()
  //    for (list <- strategyPolyhedronList) {
  //      val hotel_id = list._1.split("=")(0)
  //      val section = list._1.split("=")(1).toInt
  //      val needDf = originalDf.filter(row => {
  //        val sec = row.getAs[Long]("section").toInt
  //        sec == section
  //      }).filter(row => {
  //        val worker_id = row.getAs[String]("worker_id")
  //        val distance = JavaHBaseUtils.getValue("v2_rp_workers_hotel_distance", hotel_id + ":" + worker_id, "info", "distance")
  //        val distanceKM = (distance.toFloat / 1000)
  //        distanceKM <= 20
  //      })
  //
  //      val caseDotList: List[Double] = needDf.select("reorder_rate").rdd.map(row => row.getAs[Double]("reorder_rate")).collect().toList
  //      val caseDotMap = SomeUtils.getRightIntervalBySetting(caseDotList, caseDotInterval)
  //
  //      tup3List = tup3List.++(List[Tuple3[String, String, Map[String, Int]]](new Tuple3(list._1, list._2, caseDotMap)))
  //    }
  //    saveData2SQLServer(tup3List)
  //  }
  //
  //  // 3.统计communication_level数据
  //  def dataStatistics4CommunicationLevel(originalDf: DataFrame, caseDotInterval: List[Double]): Unit = {
  //    var tup3List: List[Tuple3[String, String, Map[String, Int]]] = List()
  //    for (list <- strategyPolyhedronList) {
  //      val hotel_id = list._1.split("=")(0)
  //      val section = list._1.split("=")(1).toInt
  //      val needDf = originalDf.filter(row => {
  //        val sec = row.getAs[Long]("section").toInt
  //        sec == section
  //      }).filter(row => {
  //        val worker_id = row.getAs[String]("worker_id")
  //        val distance = JavaHBaseUtils.getValue("v2_rp_workers_hotel_distance", hotel_id + ":" + worker_id, "info", "distance")
  //        val distanceKM = (distance.toFloat / 1000)
  //        distanceKM <= 20
  //      })
  //
  //      val caseDotList: List[Double] = needDf.select("case_dot").rdd.map(row => row.getAs[Double]("case_dot")).collect().toList
  //      val caseDotMap = SomeUtils.getRightIntervalBySetting(caseDotList, caseDotInterval)
  //
  //      tup3List = tup3List.++(List[Tuple3[String, String, Map[String, Int]]](new Tuple3(list._1, list._2, caseDotMap)))
  //    }
  //    saveData2SQLServer(tup3List)
  //  }
  //
  //  // 4.统计design_sense数据
  //  def dataStatistics4DesignSense(originalDf: DataFrame, caseDotInterval: List[Double]): Unit = {
  //    var tup3List: List[Tuple3[String, String, Map[String, Int]]] = List()
  //    for (list <- strategyPolyhedronList) {
  //      val hotel_id = list._1.split("=")(0)
  //      val section = list._1.split("=")(1).toInt
  //      val needDf = originalDf.filter(row => {
  //        val sec = row.getAs[Long]("section").toInt
  //        sec == section
  //      }).filter(row => {
  //        val worker_id = row.getAs[String]("worker_id")
  //        val distance = JavaHBaseUtils.getValue("v2_rp_workers_hotel_distance", hotel_id + ":" + worker_id, "info", "distance")
  //        val distanceKM = (distance.toFloat / 1000)
  //        distanceKM <= 20
  //      })
  //
  //      val caseDotList: List[Double] = needDf.select("case_dot").rdd.map(row => row.getAs[Double]("case_dot")).collect().toList
  //      val caseDotMap = SomeUtils.getRightIntervalBySetting(caseDotList, caseDotInterval)
  //
  //      tup3List = tup3List.++(List[Tuple3[String, String, Map[String, Int]]](new Tuple3(list._1, list._2, caseDotMap)))
  //    }
  //    saveData2SQLServer(tup3List)
  //  }
  //
  //  // 5.统计case_rate数据
  //  def dataStatistics4CasRate(originalDf: DataFrame, caseDotInterval: List[Double]): Unit = {
  //    var tup3List: List[Tuple3[String, String, Map[String, Int]]] = List()
  //    for (list <- strategyPolyhedronList) {
  //      val hotel_id = list._1.split("=")(0)
  //      val section = list._1.split("=")(1).toInt
  //      val needDf = originalDf.filter(row => {
  //        val sec = row.getAs[Long]("section").toInt
  //        sec == section
  //      }).filter(row => {
  //        val worker_id = row.getAs[String]("worker_id")
  //        val distance = JavaHBaseUtils.getValue("v2_rp_workers_hotel_distance", hotel_id + ":" + worker_id, "info", "distance")
  //        val distanceKM = (distance.toFloat / 1000)
  //        distanceKM <= 20
  //      })
  //
  //      val caseDotList: List[Double] = needDf.select("case_dot").rdd.map(row => row.getAs[Double]("case_dot")).collect().toList
  //      val caseDotMap = SomeUtils.getRightIntervalBySetting(caseDotList, caseDotInterval)
  //
  //      tup3List = tup3List.++(List[Tuple3[String, String, Map[String, Int]]](new Tuple3(list._1, list._2, caseDotMap)))
  //    }
  //    saveData2SQLServer(tup3List)
  //  }
  //
  //  // 6.统计all_score_final数据
  //  def dataStatistics4AllScoreFInal(originalDf: DataFrame, caseDotInterval: List[Double]): Unit = {
  //    var tup3List: List[Tuple3[String, String, Map[String, Int]]] = List()
  //    for (list <- strategyPolyhedronList) {
  //      val hotel_id = list._1.split("=")(0)
  //      val section = list._1.split("=")(1).toInt
  //      val needDf = originalDf.filter(row => {
  //        val sec = row.getAs[Long]("section").toInt
  //        sec == section
  //      }).filter(row => {
  //        val worker_id = row.getAs[String]("worker_id")
  //        val distance = JavaHBaseUtils.getValue("v2_rp_workers_hotel_distance", hotel_id + ":" + worker_id, "info", "distance")
  //        val distanceKM = (distance.toFloat / 1000)
  //        distanceKM <= 20
  //      })
  //
  //      val caseDotList: List[Double] = needDf.select("case_dot").rdd.map(row => row.getAs[Double]("case_dot")).collect().toList
  //      val caseDotMap = SomeUtils.getRightIntervalBySetting(caseDotList, caseDotInterval)
  //
  //      tup3List = tup3List.++(List[Tuple3[String, String, Map[String, Int]]](new Tuple3(list._1, list._2, caseDotMap)))
  //    }
  //    saveData2SQLServer(tup3List)
  //  }
  //
  //  // 7.统计number数据
  //  def dataStatistics4Number(originalDf: DataFrame, caseDotInterval: List[Double]): Unit = {
  //    var tup3List: List[Tuple3[String, String, Map[String, Int]]] = List()
  //    for (list <- strategyPolyhedronList) {
  //      val hotel_id = list._1.split("=")(0)
  //      val section = list._1.split("=")(1).toInt
  //      val needDf = originalDf.filter(row => {
  //        val sec = row.getAs[Long]("section").toInt
  //        sec == section
  //      }).filter(row => {
  //        val worker_id = row.getAs[String]("worker_id")
  //        val distance = JavaHBaseUtils.getValue("v2_rp_workers_hotel_distance", hotel_id + ":" + worker_id, "info", "distance")
  //        val distanceKM = (distance.toFloat / 1000)
  //        distanceKM <= 20
  //      })
  //
  //      val caseDotList: List[Double] = needDf.select("case_dot").rdd.map(row => row.getAs[Double]("case_dot")).collect().toList
  //      val caseDotMap = SomeUtils.getRightIntervalBySetting(caseDotList, caseDotInterval)
  //
  //      tup3List = tup3List.++(List[Tuple3[String, String, Map[String, Int]]](new Tuple3(list._1, list._2, caseDotMap)))
  //    }
  //    saveData2SQLServer(tup3List)
  //  }
  //
  //  // 8.统计to_store_rate数据
  //  def dataStatistics4ToStoreRate(originalDf: DataFrame, caseDotInterval: List[Double]): Unit = {
  //    var tup3List: List[Tuple3[String, String, Map[String, Int]]] = List()
  //    for (list <- strategyPolyhedronList) {
  //      val hotel_id = list._1.split("=")(0)
  //      val section = list._1.split("=")(1).toInt
  //      val needDf = originalDf.filter(row => {
  //        val sec = row.getAs[Long]("section").toInt
  //        sec == section
  //      }).filter(row => {
  //        val worker_id = row.getAs[String]("worker_id")
  //        val distance = JavaHBaseUtils.getValue("v2_rp_workers_hotel_distance", hotel_id + ":" + worker_id, "info", "distance")
  //        val distanceKM = (distance.toFloat / 1000)
  //        distanceKM <= 20
  //      })
  //
  //      val caseDotList: List[Double] = needDf.select("case_dot").rdd.map(row => row.getAs[Double]("case_dot")).collect().toList
  //      val caseDotMap = SomeUtils.getRightIntervalBySetting(caseDotList, caseDotInterval)
  //
  //      tup3List = tup3List.++(List[Tuple3[String, String, Map[String, Int]]](new Tuple3(list._1, list._2, caseDotMap)))
  //    }
  //    saveData2SQLServer(tup3List)
  //  }
  //
  //  // 9.统计display_amount数据
  //  def dataStatistics4DisplayAmount(originalDf: DataFrame, caseDotInterval: List[Double]): Unit = {
  //    var tup3List: List[Tuple3[String, String, Map[String, Int]]] = List()
  //    for (list <- strategyPolyhedronList) {
  //      val hotel_id = list._1.split("=")(0)
  //      val section = list._1.split("=")(1).toInt
  //      val needDf = originalDf.filter(row => {
  //        val sec = row.getAs[Long]("section").toInt
  //        sec == section
  //      }).filter(row => {
  //        val worker_id = row.getAs[String]("worker_id")
  //        val distance = JavaHBaseUtils.getValue("v2_rp_workers_hotel_distance", hotel_id + ":" + worker_id, "info", "distance")
  //        val distanceKM = (distance.toFloat / 1000)
  //        distanceKM <= 20
  //      })
  //
  //      val caseDotList: List[Double] = needDf.select("case_dot").rdd.map(row => row.getAs[Double]("case_dot")).collect().toList
  //      val caseDotMap = SomeUtils.getRightIntervalBySetting(caseDotList, caseDotInterval)
  //
  //      tup3List = tup3List.++(List[Tuple3[String, String, Map[String, Int]]](new Tuple3(list._1, list._2, caseDotMap)))
  //    }
  //    saveData2SQLServer(tup3List)
  //  }
  //
  //  // 10.统计text_rating_rate数据
  //  def dataStatistics4TextRatingRate(originalDf: DataFrame, caseDotInterval: List[Double]): Unit = {
  //    var tup3List: List[Tuple3[String, String, Map[String, Int]]] = List()
  //    for (list <- strategyPolyhedronList) {
  //      val hotel_id = list._1.split("=")(0)
  //      val section = list._1.split("=")(1).toInt
  //      val needDf = originalDf.filter(row => {
  //        val sec = row.getAs[Long]("section").toInt
  //        sec == section
  //      }).filter(row => {
  //        val worker_id = row.getAs[String]("worker_id")
  //        val distance = JavaHBaseUtils.getValue("v2_rp_workers_hotel_distance", hotel_id + ":" + worker_id, "info", "distance")
  //        val distanceKM = (distance.toFloat / 1000)
  //        distanceKM <= 20
  //      })
  //
  //      val caseDotList: List[Double] = needDf.select("case_dot").rdd.map(row => row.getAs[Double]("case_dot")).collect().toList
  //      val caseDotMap = SomeUtils.getRightIntervalBySetting(caseDotList, caseDotInterval)
  //
  //      tup3List = tup3List.++(List[Tuple3[String, String, Map[String, Int]]](new Tuple3(list._1, list._2, caseDotMap)))
  //    }
  //    saveData2SQLServer(tup3List)
  //  }

  /**
    * 03-30新增。
    */
  def eyesData(): Unit = {
    val originalDf: DataFrame = getYanJingOriginalDataFromMySQL()
    originalDf.cache()
    println("数据查找结束")
    //    val smInfoVector: Vector[SmallMarketInfo] = makeOriginalTuple()
    //    println("待写入小市场数量: " + smInfoVector.size)
    //    dataStatistics3(originalDf, smInfoVector)
    //    dataStatistics4(originalDf, smInfoVector)

    //     局部小市场数据
    println("待写入小市场数量: " + strategyPolyhedronVector.size)
    dataStatistics5(originalDf, strategyPolyhedronVector)
    originalDf.unpersist()
  }

  // todo 弃用
  def dataStatistics2(originalDf: DataFrame): Unit = {
    val conn = JavaSQLServerConn.getConnection
    val otherInterval: List[Double] = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
    val displayAmountInterval = SomeUtils.getSplitIntervalSetting(0, 5000, 100, "[]")

    var tup3CaseDotList: List[Tuple3[String, String, Map[Double, Int]]] = List()
    var tup3ReorderRateList: List[Tuple3[String, String, Map[Double, Int]]] = List()
    var tup3CommunicationLevelList: List[Tuple3[String, String, Map[Double, Int]]] = List()
    var tup3DesignSenseList: List[Tuple3[String, String, Map[Double, Int]]] = List()
    var tup3CaseRateList: List[Tuple3[String, String, Map[Double, Int]]] = List()
    var tup3AllScoreFinalList: List[Tuple3[String, String, Map[Double, Int]]] = List()
    var tup3NumberList: List[Tuple3[String, String, Map[Double, Int]]] = List()
    var tup3TextRatingRateList: List[Tuple3[String, String, Map[Double, Int]]] = List()
    var tup3DisplayAmountList: List[Tuple3[String, String, Map[Double, Int]]] = List()
    var tup3ToStoreRateList: List[Tuple3[String, String, Map[Double, Int]]] = List()
    for (list <- strategyPolyhedronList) {
      val hotel_id = list._1.split("=")(0)
      val section = list._1.split("=")(1).toInt
      val needDf: Dataset[Row] = originalDf.filter(row => {
        val sec = row.getAs[Long]("section").toInt
        sec == section
      }).filter(row => {
        val worker_id = row.getAs[String]("worker_id")
        val distance = JavaHBaseUtils.getValue("v2_rp_workers_hotel_distance", hotel_id + ":" + worker_id, "info", "distance")
        val distanceKM = (distance.toFloat / 1000)
        distanceKM <= 20
      })

      // 1.
      val caseDotList: List[Double] = needDf.select("case_dot").rdd.map(row => row.getAs[Double]("case_dot")).collect().toList
      val caseDotMap = SomeUtils.getRightIntervalBySetting2(caseDotList, otherInterval)
      tup3CaseDotList = tup3CaseDotList.++(List[Tuple3[String, String, Map[Double, Int]]](new Tuple3(list._1, list._2, caseDotMap)))
      // 2.
      val reorderRateList: List[Double] = needDf.select("reorder_rate").rdd.map(row => row.getAs[Double]("reorder_rate")).collect().toList
      val reorderRateMap = SomeUtils.getRightIntervalBySetting2(reorderRateList, otherInterval)
      tup3ReorderRateList = tup3ReorderRateList.++(List[Tuple3[String, String, Map[Double, Int]]](new Tuple3(list._1, list._2, reorderRateMap)))
      // 3.
      val communicationLevelList: List[Double] = needDf.select("communication_level").rdd.map(row => row.getAs[Double]("communication_level")).collect().toList
      val communicationLevelMap = SomeUtils.getRightIntervalBySetting2(communicationLevelList, otherInterval)
      tup3CommunicationLevelList = tup3CommunicationLevelList.++(List[Tuple3[String, String, Map[Double, Int]]](new Tuple3(list._1, list._2, communicationLevelMap)))
      // 4.
      val designSenseList: List[Double] = needDf.select("design_sense").rdd.map(row => row.getAs[Double]("design_sense")).collect().toList
      val designSenseMap = SomeUtils.getRightIntervalBySetting2(designSenseList, otherInterval)
      tup3DesignSenseList = tup3DesignSenseList.++(List[Tuple3[String, String, Map[Double, Int]]](new Tuple3(list._1, list._2, designSenseMap)))
      // 5.
      val caseRateList: List[Double] = needDf.select("case_rate").rdd.map(row => row.getAs[Double]("case_rate")).collect().toList
      val caseRateMap = SomeUtils.getRightIntervalBySetting2(caseRateList, otherInterval)
      tup3CaseRateList = tup3CaseRateList.++(List[Tuple3[String, String, Map[Double, Int]]](new Tuple3(list._1, list._2, caseRateMap)))
      // 6.
      val allScoreFinalList: List[Double] = needDf.select("all_score_final").rdd.map(row => row.getAs[Double]("all_score_final")).collect().toList
      val allScoreFinalMap = SomeUtils.getRightIntervalBySetting2(allScoreFinalList, otherInterval)
      tup3AllScoreFinalList = tup3AllScoreFinalList.++(List[Tuple3[String, String, Map[Double, Int]]](new Tuple3(list._1, list._2, allScoreFinalMap)))
      // 7.
      val numberList: List[Double] = needDf.select("number").rdd.map(row => row.getAs[Double]("number")).collect().toList
      val numberMap = SomeUtils.getRightIntervalBySetting2(numberList, otherInterval)
      tup3NumberList = tup3NumberList.++(List[Tuple3[String, String, Map[Double, Int]]](new Tuple3(list._1, list._2, numberMap)))
      // 8.
      val textRatingRateList: List[Double] = needDf.select("text_rating_rate").rdd.map(row => row.getAs[Double]("text_rating_rate")).collect().toList
      val textRatingRateMap = SomeUtils.getRightIntervalBySetting2(textRatingRateList, otherInterval)
      tup3TextRatingRateList = tup3TextRatingRateList.++(List[Tuple3[String, String, Map[Double, Int]]](new Tuple3(list._1, list._2, textRatingRateMap)))
      // 9.
      val displayAmountList: List[Double] = needDf.select("display_amount").rdd.map(row => row.getAs[Double]("display_amount")).collect().toList
      val displayAmountMap = SomeUtils.getRightIntervalBySetting3(displayAmountList, displayAmountInterval)
      tup3DisplayAmountList = tup3DisplayAmountList.++(List[Tuple3[String, String, Map[Double, Int]]](new Tuple3(list._1, list._2, displayAmountMap)))
      // 10.
      val toStoreRateList: List[Double] = needDf.select("to_store_rate").rdd.map(row => row.getAs[Double]("to_store_rate")).collect().toList
      val toStoreRateMap = SomeUtils.getRightIntervalBySetting2(toStoreRateList, otherInterval)
      tup3ToStoreRateList = tup3ToStoreRateList.++(List[Tuple3[String, String, Map[Double, Int]]](new Tuple3(list._1, list._2, toStoreRateMap)))
    }

    println("开始写入数据")

    val start = System.currentTimeMillis()
    saveData2SQLServerMatchName2(tup3CaseDotList, "case_dot", conn)
    saveData2SQLServerMatchName2(tup3ReorderRateList, "reorder_rate", conn)
    saveData2SQLServerMatchName2(tup3CommunicationLevelList, "communication_level", conn)
    saveData2SQLServerMatchName2(tup3DesignSenseList, "design_sense", conn)
    saveData2SQLServerMatchName2(tup3CaseRateList, "case_rate", conn)
    saveData2SQLServerMatchName2(tup3AllScoreFinalList, "all_score_final", conn)
    saveData2SQLServerMatchName2(tup3NumberList, "number", conn)
    saveData2SQLServerMatchName2(tup3TextRatingRateList, "text_rating_rate", conn)
    saveData2SQLServerMatchName2(tup3DisplayAmountList, "display_amount", conn)
    saveData2SQLServerMatchName2(tup3ToStoreRateList, "to_store_rate", conn)
    val stop = System.currentTimeMillis()

    println("=============")
    println("写入time = " + (stop - start) / 1000 + "s.")
    println("=============")

    JavaSQLServerConn.closeConnection(conn)
  }

  // todo 弃用
  def saveData2SQLServerMatchName2(list: List[Tuple3[String, String, Map[Double, Int]]], name: String, conn: Connection): Unit = {
    var i = 0
    //设置批量处理的数量
    val batchSize = 5000
    var tableName = ""
    var field = ""
    var other_condition = ""
    name match {
      case "case_dot" => tableName = "canNum_v1_case_dot"; field = "case_dot"; other_condition = "Vj≠1"
      case "reorder_rate" => tableName = "canNum_v2_reorder_rate"; field = "reorder_rate"; other_condition = "Vj≠2"
      case "communication_level" => tableName = "canNum_v3_communication_level"; field = "communication_level"; other_condition = "Vj≠3"
      case "design_sense" => tableName = "canNum_v4_design_sense"; field = "design_sense"; other_condition = "Vj≠4"
      case "case_rate" => tableName = "canNum_v5_case_rate"; field = "case_rate"; other_condition = "Vj≠5"
      case "all_score_final" => tableName = "canNum_v6_all_score_final"; field = "all_score_final"; other_condition = "Vj≠6"
      case "number" => tableName = "canNum_v7_number"; field = "number"; other_condition = "Vj≠7"
      case "text_rating_rate" => tableName = "canNum_v8_text_rating_rate"; field = "text_rating_rate"; other_condition = "Vj≠8"
      case "display_amount" => tableName = "canNum_v9_display_amount"; field = "display_amount"; other_condition = "Vj≠9"
      case "to_store_rate" => tableName = "canNum_v10_to_store_rate"; field = "to_store_rate"; other_condition = "Vj≠10"
      case _ =>
    }
    val stmt = conn.prepareStatement(s"insert into $tableName(hotel_id,hotel_name,section,$field,other_condition,can_num,itime) VALUES (?,?,?,?,?,?,getdate())")
    conn.setAutoCommit(false)
    for (inL <- list) {
      val hotel_id = inL._1.split("=")(0)
      val section = inL._1.split("=")(1).toInt
      val hotel_name = inL._2
      for (map <- inL._3) {
        i = i + 1;
        val inField = map._1.toFloat
        val number = map._2
        stmt.setString(1, hotel_id)
        stmt.setString(2, hotel_name)
        stmt.setInt(3, section)
        stmt.setFloat(4, inField)
        stmt.setString(5, other_condition)
        stmt.setInt(6, number)
        stmt.addBatch()
        if (i % batchSize == 0) {
          stmt.executeBatch
          conn.commit
        }
      }
    }
    if (i % batchSize != 0) {
      stmt.executeBatch
      conn.commit
    }

    JavaSQLServerConn.closeStatement(stmt)
    //    JavaSQLServerConn.closeConnection(conn)
  }

  // 策略多面体元组
  val strategyPolyhedronList = List[Tuple2[String, String]](
    // 第一波策略多面体酒店
    Tuple2("1fa69245-c5bc-4107-9e06-73ebbe0879c0=3", "世外桃源酒店"),
    //    // 第二波策略多面体酒店
    Tuple2("43c7e2bf-4c7a-4af1-94bc-8ed553a23ddb=1", "正熙雅居酒店"),
    Tuple2("f59f8c98-5270-4c45-804b-3bcca78efa00=1", "艺朗酒店"),
    Tuple2("18b2bd26-a06b-4abd-a8a5-b4f1d3f5a8f5=2", "厚院庄园"),
    Tuple2("7d477695-a2e2-4010-9e64-0b420245596b=2", "亿臣国际酒店"),
    Tuple2("917ead54-441e-444c-8927-8257f07b824d=3", "简阳城市名人酒店·宴会厅"),
    Tuple2("e1520dbe-8fb0-430c-b2e4-7931309541de=3", "诺亚方舟(东南门)"),
    Tuple2("8a19bcfb-909d-4388-9d1f-e59ef9b989c1=4", "南昌万达文华酒店"),
    Tuple2("01bb057e-5b65-4a1f-b211-ce8692aa85d7=4", "巴国布衣(神仙树店)"),
    Tuple2("427b89cc-795d-4899-8fea-4a14d8f4bd01=5", "春生"),
    Tuple2("d280ab29-ee5f-4a3c-a7a4-01468167d5a0=5", "重庆金陵大饭店"),

    // 第三波策略多面体酒店
    Tuple2("dd99abe5-d2f2-4ae0-8c22-579251ed1d7d=1", "成都金韵酒店"),
    Tuple2("a335c367-4d9a-4d6b-90c3-fc369703f147=1", "成都大鼎戴斯大酒店"),
    Tuple2("97c6330d-26f1-4c67-ae4f-5bbddfb79f9e=2", "世茂成都茂御酒店"),
    Tuple2("a9fe4b14-5f45-47c2-bc35-4d36fcdaae05=2", "交子国际酒店"),
    Tuple2("7f7ded13-8cf2-44c3-aed0-491fea28a77c=3", "天邑国际酒店"),
    Tuple2("d3ede457-bdee-43d5-aac8-15b57a72db7c=3", "华府金座"),
    Tuple2("0346b938-2a7b-4b70-bbc4-1bd500547497=4", "林道假日酒店"),
    Tuple2("16786afa-f9bc-4e74-848f-bc0fc79e6bf3=4", "艾克美雅阁大酒店"),
    Tuple2("462aee34-67b5-4851-aa2e-002aafc2a22c=5", "西苑半岛"),
    Tuple2("be621581-25d8-4dbd-847b-da7c72dbfefc=5", "怡东国际酒店"),

    // 第四波策略多面体酒店
    Tuple2("ef0c39be-9675-49dc-a32e-23a31f5db258=1", "成都希尔顿酒店"),
    Tuple2("8072ab05-5e85-4276-9ace-ac2851f7a103=1", "蓉南国际酒店"),
    Tuple2("a335c367-4d9a-4d6b-90c3-fc369703f147=2", "成都大鼎戴斯大酒店"),
    Tuple2("ed8da82f-5aec-40a8-9b24-7ba3c289c0b8=2", "南堂馆(天府三街店)"),
    Tuple2("5849ad9a-f16d-417a-a58f-ec51e971cb47=3", "航宸国际酒店"),
    Tuple2("8ede682c-8144-4eb7-8050-1b76e5181210=3", "路易·珀泰夏堡"),
    Tuple2("9ee77461-0e82-418e-ba27-4bfa7cf2f242=4", "瑞升·芭富丽大酒店"),
    Tuple2("792d3a3a-9a15-4846-bd84-b3c480de08ea=4", "成都新东方千禧大酒店"),
    Tuple2("d3484e83-4408-41eb-923b-e009672afe55=5", "成都首座万丽酒店"),
    Tuple2("a835f3df-4a27-4c2f-8456-fbab4ccea61b=5", "成都百悦希尔顿逸林酒店"),

    // 第五波策略多面体酒店
    Tuple2("8bda29bb-c63e-4631-a465-e928ccf7525e=1", "峨眉山大酒店"),
    Tuple2("e812e6b3-d881-45e2-b5b3-e168bca74ff7=1", "尚成十八步岛酒店"),
    Tuple2("a67ad81c-61b3-467a-a162-c7aedf177b46=2", "金领·莲花大酒店"),
    Tuple2("852d1616-5c9c-4ca3-8b27-54fa5362f191=2", "南昌瑞颐大酒店(东门)"),
    Tuple2("fa409e82-ac70-497c-9d3d-88345e5712ff=3", "圣瑞云台营山宴会中心"),
    Tuple2("7c8f6e40-bd80-42ee-9289-a57acd840801=3", "今日东坡"),
    Tuple2("f2c3aadf-5530-4d22-921d-ddb206543dbc=4", "滨江大会堂"),
    Tuple2("8bda29bb-c63e-4631-a465-e928ccf7525e=4", "峨眉山大酒店	"),
    Tuple2("fa409e82-ac70-497c-9d3d-88345e5712ff=5", "圣瑞云台营山宴会中心"),
    Tuple2("852d1616-5c9c-4ca3-8b27-54fa5362f191=5", "南昌瑞颐大酒店(东门)"),

    // 第六波策略多面体酒店
    Tuple2("4fb48ad2-fcaa-41bd-b148-fde3ddf2bbbd=1", "宽亭酒楼"),
    Tuple2("a335c367-4d9a-4d6b-90c3-fc369703f147=1", "成都大鼎戴斯大酒店"),
    Tuple2("3e7e916c-2631-4d4f-862a-5edb80859fc5=2", "成都龙之梦大酒店"),
    Tuple2("8f0e43e3-42c3-4d5f-821f-b6250332d85d=2", "安泰安蓉大酒店"),
    Tuple2("7992b693-5d0b-4c64-9463-e1f22c50805e=3", "第一江南酒店"),
    Tuple2("fc2e2086-bd35-4580-a5aa-91257b4116fb=3", "天和缘"),
    Tuple2("eccee365-a536-4d6f-be71-64401f7c59c8=4", "绿洲大酒店"),
    Tuple2("d7a2d62d-73ab-43c0-aa7e-a28d21a01046=4", "林恩国际酒店	"),
    Tuple2("64e6c112-1e4d-4d3d-b3a7-a49344840302=5", "成都棕榈泉费尔蒙酒店"),
    Tuple2("e7bd0545-82ce-4c1a-a0c2-0b6ed6307d8c=5", "成都首座万豪酒店"),

    // 第7波策略多面体酒店
    // section1
    Tuple2("4a3b30ac-73a9-4c5c-81f4-94be28fbeeec=1", "不二山房"),
    Tuple2("57290e8c-adab-48c6-ae2f-96c397a8aa28=1", "西蜀森林酒店(西南1门)"),
    Tuple2("5b426558-5264-4749-87ac-cc697ba0cc0a=1", "红杏酒家(明珠店)"),
    Tuple2("cc4ee06f-bfbf-4768-a179-c1fda8ef4808=1", "大蓉和·卓锦酒楼(鹭岛路店)"),
    Tuple2("98127048-a928-405f-af1e-1501e9be5192=1", "巴国布衣紫荆店-宴会厅"),
    Tuple2("a82f0f18-daf8-4f8d-93c1-e26de1b65eb7=1", "成都合江亭翰文大酒店"),
    Tuple2("3e7e916c-2631-4d4f-862a-5edb80859fc5=1", "成都龙之梦大酒店"),
    Tuple2("2d8043cd-00a0-40a0-b939-53663c409853=1", "泰合索菲特大饭店"),
    Tuple2("454f80e9-4ca9-48e9-b0c4-1eac095bcda0=1", "月圆霖"),
    Tuple2("5fd54d5a-403a-4c75-9da4-c4e687ef07cf=1", "迎宾一号"),
    // section2
    Tuple2("01d3d0fc-a47e-47dc-bf9f-8c1f5492ad24=2", "郦湾国际酒店"),
    Tuple2("392359e6-b6eb-4098-bad8-9fc4c43110df=2", "寅生国际酒店"),
    Tuple2("02d6ccf1-46e0-4d28-83dd-769bfa8ec4b0=2", "大蓉和(一品天下旗舰店)"),
    Tuple2("e22c5f92-70a8-4a81-a6ff-cb3c7a7d87db=2", "林海山庄(环港路)"),
    Tuple2("3e47de5d-e076-454d-b0f8-d555064a96c3=2", "川西人居"),
    Tuple2("710629f0-31e1-45fd-903c-d670055fb327=2", "西蜀人家"),
    Tuple2("29672fa4-4640-4a2f-b0a8-f868d54e67a7=2", "漫花庄园"),
    Tuple2("c461e101-23db-4cf0-8824-9fdfb442271c=2", "成都牧山沁园酒店"),
    Tuple2("d9105038-f307-431e-bb51-4c25e5335d45=2", "洁惠花园饭店"),
    Tuple2("6af69637-105e-4647-b592-937a8742e208=2", "老房子(毗河店)"),
    // section3
    Tuple2("bd64ef3a-40ee-42fc-8289-fc57e2f177a4=3", "映月湖酒店"),
    Tuple2("057e09d5-9c3d-48c2-8d85-1338ef133625=3", "川投国际酒店"),
    Tuple2("da844c8f-c413-4733-8ad8-b3f644a93a98=3", "友豪·罗曼大酒店"),
    Tuple2("2dd8454a-16cb-434f-aff7-6eba8454c341=3", "鹿归国际酒店"),
    Tuple2("11dcc500-2cef-44d0-bfcd-a70014090031=3", "桂湖国际大酒店"),
    Tuple2("3cb310bd-c2bf-4c83-82ab-40280f232e24=3", "喜鹊先生花园餐厅"),
    Tuple2("c058f54d-8c4e-4fa7-978e-412f0536c816=3", "菁华园"),
    Tuple2("787eaa99-7092-4593-9db6-39353f1e38cd=3", "顺兴老(世纪城店)"),
    Tuple2("4032eb26-2b39-4fea-bb9b-fc4491b0b171=3", "蒋排骨顺湖园"),
    Tuple2("90c25b75-9ac0-4c8e-a4bb-5c898753859a=3", "喜馆精品酒店"),
    // section4
    Tuple2("8996068b-0ceb-46e1-a366-d70282b5eb1d=4", "智汇堂枫泽大酒店"),
    Tuple2("c3dc2c80-4571-4ecc-a5f0-9b380cb8a163=4", "中胜大酒店"),
    Tuple2("29672fa4-4640-4a2f-b0a8-f868d54e67a7=4", "漫花庄园"),
    Tuple2("057e09d5-9c3d-48c2-8d85-1338ef133625=4", "川投国际酒店"),
    Tuple2("c6234b4f-aa8e-4949-be1a-075e639f2157=4", "锦亦缘新派川菜"),
    Tuple2("16c3174a-0a36-46b9-bbf7-7fde885932fb=4", "嘉莱特精典国际酒店"),
    Tuple2("af155b48-3149-45af-8625-16c42d5570e3=4", "明宇豪雅饭店(科华店)"),
    Tuple2("724ee82d-57ba-4e70-913a-d868508322a5=4", "老房子华粹元年食府(天府三街店)"),
    Tuple2("9ff71e0b-b881-4718-9319-f87f5ce82f40=4", "重庆澳维酒店(澳维酒店港式茶餐厅)"),
    Tuple2("3e3e86d6-3b2a-4446-8ae0-bdc4ec158489=4", "香城竹韵(斑竹园店)"),
    // section5
    Tuple2("97c6330d-26f1-4c67-ae4f-5bbddfb79f9e=5", "世茂成都茂御酒店"),
    Tuple2("5b967168-f07f-4f09-b7c0-55ada1d9dec1=5", "明宇丽雅悦酒店"),
    Tuple2("ebb0bdaf-214c-4633-b121-4ebd2da5fc85=5", "星宸航都国际酒店"),
    Tuple2("8936bf11-c356-4115-9f5f-c7b6c828b46b=5", "成都凯宾斯基饭店"),
    Tuple2("a9fe4b14-5f45-47c2-bc35-4d36fcdaae05=5", "交子国际酒店"),
    Tuple2("1fa69245-c5bc-4107-9e06-73ebbe0879c0=5", "世外桃源酒店"),
    Tuple2("724ee82d-57ba-4e70-913a-d868508322a5=5", "老房子华粹元年食府(天府三街店)"),
    Tuple2("7ccfdc8f-7237-4d8f-a002-f6f235880334=5", "和淦·香城竹韵"),
    Tuple2("1158cc09-daa5-4942-a84d-5b2e02238a3c=5", "家园国际酒店"),
    Tuple2("b50fcf50-9cfb-4c48-8115-9cc47e89a521=5", "成都雅居乐豪生大酒店"),

    // 第8波策略多面体酒店
    // section1
    Tuple2("20c2a7e2-f124-495a-9569-9ba2e3914d37=1", "大蓉和拉德方斯(精品店)"),
    Tuple2("37a6faab-3a18-4e89-b68d-cdc756a88da6=1", "阿斯牛牛·凉山菜(新会展店)"),
    Tuple2("d3892737-7236-4c7e-9318-7dfd12b53d63=1", "贵府酒楼(蜀辉路)"),
    Tuple2("8996068b-0ceb-46e1-a366-d70282b5eb1d=1", "智汇堂枫泽大酒店"),
    Tuple2("749d1dff-f0fb-4780-b183-d89c2b1519c1=1", "上层名人酒店"),
    Tuple2("c8258e5b-9e19-4573-b63d-6cf59016762c=1", "文杏酒楼(一品天下店)"),
    Tuple2("c94f43f4-fbb9-4ab6-8668-bc8678f333b7=1", "蔚然花海"),
    Tuple2("09f8e3bd-0053-44c0-85ff-3939690ecb09=1", "望江宾馆"),
    Tuple2("02ebfc9f-9180-4a0d-a74f-2e1c13e2fad0=1", "锦峰大酒店"),
    Tuple2("724ee82d-57ba-4e70-913a-d868508322a5=1", "老房子华粹元年食府(天府三街店)"),
    // section2
    Tuple2("9c4eb1af-4d9f-4bbf-b823-ce2fb08827c9=2", "红高粱海鲜量贩酒楼(红光店)"),
    Tuple2("889004a8-70bd-4b99-b669-81167105560d=2", "智谷云尚丽呈华廷酒店"),
    Tuple2("7992b693-5d0b-4c64-9463-e1f22c50805e=2", "第一江南酒店"),
    Tuple2("3a1baa7f-17f7-4a66-946a-2bf0061c1f20=2", "红杏酒家(羊西店)"),
    Tuple2("dc4bb742-6e35-4b9a-9631-58bb035de764=2", "广都国际酒店"),
    Tuple2("fd985b07-db6a-4203-a8ef-b4389773b695=2", "水香雅舍(三圣乡店)"),
    Tuple2("92905055-163e-4081-a074-727fb00b5cc6=2", "红杏酒家(万达广场金牛店)"),
    Tuple2("d6928f86-cdc8-47bb-aa8e-117b1e8a008b=2", "呈祥·东馆"),
    Tuple2("ec523414-19bc-499e-9090-64460d76bc77=2", "杰恩酒店(旗舰店)"),
    Tuple2("c6ff560b-9819-4321-88a6-5b034b62ce89=2", "红杏酒家·宴会厅(锦华店)"),
    Tuple2("e58f3c73-d4a3-4103-a49a-508d2dadd2c5=2", "红杏酒家(金府店)"),
    // section3
    Tuple2("1158cc09-daa5-4942-a84d-5b2e02238a3c=3", "家园国际酒店"),
    Tuple2("3cb310bd-c2bf-4c83-82ab-40280f232e24=3", "喜鹊先生花园餐厅"),
    Tuple2("ccf94653-eaa9-4e07-97ae-9d7da264134e=3", "新皇城大酒店"),
    Tuple2("c6234b4f-aa8e-4949-be1a-075e639f2157=3", "锦亦缘新派川菜"),
    Tuple2("f6597d6f-9125-4367-b685-2fb3e5510c0a=3", "友豪锦江酒店"),
    Tuple2("8936bf11-c356-4115-9f5f-c7b6c828b46b=3", "成都凯宾斯基饭店"),
    Tuple2("33d34c3c-a184-48d8-bb30-dd78925d27e3=3", "闲亭(峨影店)"),
    Tuple2("5b967168-f07f-4f09-b7c0-55ada1d9dec1=3", "明宇丽雅悦酒店"),
    Tuple2("b96db01c-ecd6-4f9a-8cd0-23ff4e0d7512=3", "应龙湾澜岸酒店"),
    Tuple2("b32317c0-2613-4a75-aeee-6ac4dc726f83=3", "玉瑞酒店"),
    // section4
    Tuple2("5900742b-4290-4bc3-ae8d-4db7b1115557=4", "泰清锦宴"),
    Tuple2("ae5d93e3-481a-42fe-9643-c0387321d9ae=4", "博瑞花园酒店"),
    Tuple2("c0aa9212-9aed-4801-800d-8c412e972c49=4", "首席·1956"),
    Tuple2("72fd3bdd-69d6-4dca-b7cf-d48644ac8e58=4", "明宇尚雅饭店"),
    Tuple2("3e0921fb-4957-4eeb-9640-334455227c6e=4", "西苑半岛酒店"),
    Tuple2("f59f8c98-5270-4c45-804b-3bcca78efa00=4", "艺朗酒店"),
    Tuple2("0346b938-2a7b-4b70-bbc4-1bd500547497=4", "林道假日酒店"),
    Tuple2("d889a8d4-c60a-405c-a94d-b783c223560c=4", "明宇豪雅·怡品堂(东大街店)"),
    Tuple2("16c90a2c-fd6c-4166-95c6-f1cff3c178e5=4", "岷江新濠酒店宴会厅"),
    Tuple2("dd99abe5-d2f2-4ae0-8c22-579251ed1d7d=4", "成都金韵酒店"),
    // section5
    Tuple2("09f8e3bd-0053-44c0-85ff-3939690ecb09=5", "望江宾馆"),
    Tuple2("39b08285-d287-46ba-abca-70acb0c58898=5", "南昌万达嘉华酒店"),
    Tuple2("9d4e835b-5865-4627-bfda-be56199cbcb2=5", "禧悦酒店"),
    Tuple2("a0234282-d242-45e3-a5a7-55653be2d667=5", "南昌力高皇冠假日酒店"),
    Tuple2("d6a014e2-de76-4285-bb27-b31b1cf339c1=5", "豪雅东方花园酒店"),
    Tuple2("8a19bcfb-909d-4388-9d1f-e59ef9b989c1=5", "南昌万达文华酒店"),
    Tuple2("ebb0bdaf-214c-4633-b121-4ebd2da5fc85=5", "星宸航都国际酒店"),
    Tuple2("1158cc09-daa5-4942-a84d-5b2e02238a3c=5", "家园国际酒店"),
    Tuple2("8fe4d72d-8c16-422c-80aa-2c70453e9ca1=5", "南昌喜来登酒店"),
    Tuple2("f6597d6f-9125-4367-b685-2fb3e5510c0a=5", "友豪锦江酒店"),

    // 第9波策略多面体酒店
    // section1
    Tuple2("b2b26408-4b8c-49e4-98a3-122ce40d9888=1", "保利国际高尔夫花园"),
    Tuple2("b2b26408-4b8c-49e4-98a3-122ce40d9888=1", "保利国际高尔夫花园"),
    Tuple2("d3b26bad-787c-4e1e-92f2-1620c1e1733f=1", "南昌喜来登酒店-大宴会厅"),
    Tuple2("d6a014e2-de76-4285-bb27-b31b1cf339c1=1", "豪雅东方花园酒店"),
    Tuple2("c3d64db1-995f-46ef-a32a-e4acad7469d7=1", "东方豪景花园酒店"),
    Tuple2("020f5fef-d0b5-48da-8e03-dee0c61adeea=1", "银桦半岛酒店"),
    Tuple2("39b08285-d287-46ba-abca-70acb0c58898=1", "南昌万达嘉华酒店"),
    Tuple2("965f750b-bcda-4a51-aedd-94563652c0fe=1", "成都茂业JW万豪酒店"),
    Tuple2("20c2a7e2-f124-495a-9569-9ba2e3914d37=1", "大蓉和拉德方斯(精品店)"),
    Tuple2("403bc1b9-d66d-4d6a-872c-c9fe736de3f0=1", "岷山饭店"),
    Tuple2("d72ca7a2-74d6-4780-9a5b-51ba1284fccf=1", "诺亚方舟(羊犀店)"),
    // section2
    Tuple2("c4dfa2b8-a627-46d7-b139-e0d40d952110=2", "彭州信一雅阁酒店"),
    Tuple2("85d91ae3-dda3-4d20-bfcb-2e4de544879c=2", "东篱翠湖"),
    Tuple2("c86348e9-cfe5-4ccd-961e-bcc7e1b7240f=2", "成飞宾馆"),
    Tuple2("51f8208c-2d64-42fa-8ead-705a4fae8f26=2", "刘家花园"),
    Tuple2("4e496ec5-9234-4d2c-a448-a1f82ab1d915=2", "赣江宾馆"),
    Tuple2("ea98d4eb-f3a5-4685-8984-cad046b720b1=2", "欢聚一堂"),
    Tuple2("9fc5ac91-e027-4025-8a5e-7b024670acad=2", "西蜀森林酒店"),
    Tuple2("749d1dff-f0fb-4780-b183-d89c2b1519c1=2", "上层名人酒店"),
    Tuple2("c0e20114-bf80-426d-8817-a40e6c5853dc=2", "俏巴渝(爱琴海购物公园店)"),
    Tuple2("4f0ce7c0-7c40-4ae8-acb9-90ea1a47b6ff=2", "新华国际酒店"),
    // section3
    Tuple2("c4dfa2b8-a627-46d7-b139-e0d40d952110=3", "彭州信一雅阁酒店"),
    Tuple2("462aee34-67b5-4851-aa2e-002aafc2a22c=3", "西苑半岛"),
    Tuple2("43264973-17ad-4ce4-ac2b-15e2a02b7642=3", "金河宾馆"),
    Tuple2("85d91ae3-dda3-4d20-bfcb-2e4de544879c=3", "东篱翠湖"),
    Tuple2("b22d46ba-a7f4-4da6-9977-0a314778c9e3=3", "南昌江景假日酒店"),
    Tuple2("4a3b30ac-73a9-4c5c-81f4-94be28fbeeec=3", "不二山房"),
    Tuple2("852d1616-5c9c-4ca3-8b27-54fa5362f191=3", "南昌瑞颐大酒店(东门)"),
    Tuple2("9d4e835b-5865-4627-bfda-be56199cbcb2=3", "禧悦酒店"),
    Tuple2("a909a7fb-2b8d-4643-a507-6896ea9faf0e=3", "西藏饭店"),
    Tuple2("f102bd3f-f02f-4452-8e17-38a409a7b257=3", "金阳尚城酒店"),
    // section4
    Tuple2("e812e6b3-d881-45e2-b5b3-e168bca74ff7=4", "尚成十八步岛酒店"),
    Tuple2("c5b139d3-57d9-4095-b422-81452d82d158=4", "南昌香格里拉大酒店"),
    Tuple2("c0aa9212-9aed-4801-800d-8c412e972c49=4", "首席·1956"),
    Tuple2("f6def674-452b-4a16-9030-33d88daf6756=4", "星辰航都国际酒店销售中心"),
    Tuple2("92bd6809-7105-4952-8de8-77bae6c896eb=4", "南昌绿地华邑酒店"),
    Tuple2("d764c9de-22bc-4c67-a262-3e2ac2b39cb7=4", "成都空港大酒店"),
    Tuple2("87395129-729f-46ec-8e22-7322b129c54a=4", "南昌凯美开元名都大酒店"),
    Tuple2("c0b134fb-493d-4b07-bed4-c48df9def143=4", "席锦酒家"),
    Tuple2("9ee77461-0e82-418e-ba27-4bfa7cf2f242=4", "瑞升·芭富丽大酒店"),
    Tuple2("0346b938-2a7b-4b70-bbc4-1bd500547497=4", "林道假日酒店"),
    // section5
    Tuple2("f12fe4aa-84bc-45bb-b1e0-68a7367b6828=5", "麓山国际社区"))

  val strategyPolyhedronVector = Vector[SmallMarketInfo](
    //    // 第一波策略多面体酒店
    //    SmallMarketInfo("1fa69245-c5bc-4107-9e06-73ebbe0879c0=3", "世外桃源酒店"),
    //    //    // 第二波策略多面体酒店
    //    SmallMarketInfo("43c7e2bf-4c7a-4af1-94bc-8ed553a23ddb=1", "正熙雅居酒店"),
    //    SmallMarketInfo("f59f8c98-5270-4c45-804b-3bcca78efa00=1", "艺朗酒店"),
    //    SmallMarketInfo("18b2bd26-a06b-4abd-a8a5-b4f1d3f5a8f5=2", "厚院庄园"),
    //    SmallMarketInfo("7d477695-a2e2-4010-9e64-0b420245596b=2", "亿臣国际酒店"),
    //    SmallMarketInfo("917ead54-441e-444c-8927-8257f07b824d=3", "简阳城市名人酒店·宴会厅"),
    //    SmallMarketInfo("e1520dbe-8fb0-430c-b2e4-7931309541de=3", "诺亚方舟(东南门)"),
    //    SmallMarketInfo("8a19bcfb-909d-4388-9d1f-e59ef9b989c1=4", "南昌万达文华酒店"),
    //    SmallMarketInfo("01bb057e-5b65-4a1f-b211-ce8692aa85d7=4", "巴国布衣(神仙树店)"),
    //    SmallMarketInfo("427b89cc-795d-4899-8fea-4a14d8f4bd01=5", "春生"),
    //    SmallMarketInfo("d280ab29-ee5f-4a3c-a7a4-01468167d5a0=5", "重庆金陵大饭店"),
    //
    //    // 第三波策略多面体酒店
    //    SmallMarketInfo("dd99abe5-d2f2-4ae0-8c22-579251ed1d7d=1", "成都金韵酒店"),
    //    SmallMarketInfo("a335c367-4d9a-4d6b-90c3-fc369703f147=1", "成都大鼎戴斯大酒店"),
    //    SmallMarketInfo("97c6330d-26f1-4c67-ae4f-5bbddfb79f9e=2", "世茂成都茂御酒店"),
    //    SmallMarketInfo("a9fe4b14-5f45-47c2-bc35-4d36fcdaae05=2", "交子国际酒店"),
    //    SmallMarketInfo("7f7ded13-8cf2-44c3-aed0-491fea28a77c=3", "天邑国际酒店"),
    //    SmallMarketInfo("d3ede457-bdee-43d5-aac8-15b57a72db7c=3", "华府金座"),
    //    SmallMarketInfo("0346b938-2a7b-4b70-bbc4-1bd500547497=4", "林道假日酒店"),
    //    SmallMarketInfo("16786afa-f9bc-4e74-848f-bc0fc79e6bf3=4", "艾克美雅阁大酒店"),
    //    SmallMarketInfo("462aee34-67b5-4851-aa2e-002aafc2a22c=5", "西苑半岛"),
    //    SmallMarketInfo("be621581-25d8-4dbd-847b-da7c72dbfefc=5", "怡东国际酒店"),
    //
    //    // 第四波策略多面体酒店
    //    SmallMarketInfo("ef0c39be-9675-49dc-a32e-23a31f5db258=1", "成都希尔顿酒店"),
    //    SmallMarketInfo("8072ab05-5e85-4276-9ace-ac2851f7a103=1", "蓉南国际酒店"),
    //    SmallMarketInfo("a335c367-4d9a-4d6b-90c3-fc369703f147=2", "成都大鼎戴斯大酒店"),
    //    SmallMarketInfo("ed8da82f-5aec-40a8-9b24-7ba3c289c0b8=2", "南堂馆(天府三街店)"),
    //    SmallMarketInfo("5849ad9a-f16d-417a-a58f-ec51e971cb47=3", "航宸国际酒店"),
    //    SmallMarketInfo("8ede682c-8144-4eb7-8050-1b76e5181210=3", "路易·珀泰夏堡"),
    //    SmallMarketInfo("9ee77461-0e82-418e-ba27-4bfa7cf2f242=4", "瑞升·芭富丽大酒店"),
    //    SmallMarketInfo("792d3a3a-9a15-4846-bd84-b3c480de08ea=4", "成都新东方千禧大酒店"),
    //    SmallMarketInfo("d3484e83-4408-41eb-923b-e009672afe55=5", "成都首座万丽酒店"),
    //    SmallMarketInfo("a835f3df-4a27-4c2f-8456-fbab4ccea61b=5", "成都百悦希尔顿逸林酒店"),
    //
    //    // 第五波策略多面体酒店
    //    SmallMarketInfo("8bda29bb-c63e-4631-a465-e928ccf7525e=1", "峨眉山大酒店"),
    //    SmallMarketInfo("e812e6b3-d881-45e2-b5b3-e168bca74ff7=1", "尚成十八步岛酒店"),
    //    SmallMarketInfo("a67ad81c-61b3-467a-a162-c7aedf177b46=2", "金领·莲花大酒店"),
    //    SmallMarketInfo("852d1616-5c9c-4ca3-8b27-54fa5362f191=2", "南昌瑞颐大酒店(东门)"),
    //    SmallMarketInfo("fa409e82-ac70-497c-9d3d-88345e5712ff=3", "圣瑞云台营山宴会中心"),
    //    SmallMarketInfo("7c8f6e40-bd80-42ee-9289-a57acd840801=3", "今日东坡"),
    //    SmallMarketInfo("f2c3aadf-5530-4d22-921d-ddb206543dbc=4", "滨江大会堂"),
    //    SmallMarketInfo("8bda29bb-c63e-4631-a465-e928ccf7525e=4", "峨眉山大酒店	"),
    //    SmallMarketInfo("fa409e82-ac70-497c-9d3d-88345e5712ff=5", "圣瑞云台营山宴会中心"),
    //    SmallMarketInfo("852d1616-5c9c-4ca3-8b27-54fa5362f191=5", "南昌瑞颐大酒店(东门)"),
    //
    //    // 第六波策略多面体酒店
    //    SmallMarketInfo("4fb48ad2-fcaa-41bd-b148-fde3ddf2bbbd=1", "宽亭酒楼"),
    //    SmallMarketInfo("a335c367-4d9a-4d6b-90c3-fc369703f147=1", "成都大鼎戴斯大酒店"),
    //    SmallMarketInfo("3e7e916c-2631-4d4f-862a-5edb80859fc5=2", "成都龙之梦大酒店"),
    //    SmallMarketInfo("8f0e43e3-42c3-4d5f-821f-b6250332d85d=2", "安泰安蓉大酒店"),
    //    SmallMarketInfo("7992b693-5d0b-4c64-9463-e1f22c50805e=3", "第一江南酒店"),
    //    SmallMarketInfo("fc2e2086-bd35-4580-a5aa-91257b4116fb=3", "天和缘"),
    //    SmallMarketInfo("eccee365-a536-4d6f-be71-64401f7c59c8=4", "绿洲大酒店"),
    //    SmallMarketInfo("d7a2d62d-73ab-43c0-aa7e-a28d21a01046=4", "林恩国际酒店	"),
    //    SmallMarketInfo("64e6c112-1e4d-4d3d-b3a7-a49344840302=5", "成都棕榈泉费尔蒙酒店"),
    //    SmallMarketInfo("e7bd0545-82ce-4c1a-a0c2-0b6ed6307d8c=5", "成都首座万豪酒店"),
    //
    //    // 第7波策略多面体酒店
    //    // section1
    //    SmallMarketInfo("4a3b30ac-73a9-4c5c-81f4-94be28fbeeec=1", "不二山房"),
    //    SmallMarketInfo("57290e8c-adab-48c6-ae2f-96c397a8aa28=1", "西蜀森林酒店(西南1门)"),
    //    SmallMarketInfo("5b426558-5264-4749-87ac-cc697ba0cc0a=1", "红杏酒家(明珠店)"),
    //    SmallMarketInfo("cc4ee06f-bfbf-4768-a179-c1fda8ef4808=1", "大蓉和·卓锦酒楼(鹭岛路店)"),
    //    SmallMarketInfo("98127048-a928-405f-af1e-1501e9be5192=1", "巴国布衣紫荆店-宴会厅"),
    //    SmallMarketInfo("a82f0f18-daf8-4f8d-93c1-e26de1b65eb7=1", "成都合江亭翰文大酒店"),
    //    SmallMarketInfo("3e7e916c-2631-4d4f-862a-5edb80859fc5=1", "成都龙之梦大酒店"),
    //    SmallMarketInfo("2d8043cd-00a0-40a0-b939-53663c409853=1", "泰合索菲特大饭店"),
    //    SmallMarketInfo("454f80e9-4ca9-48e9-b0c4-1eac095bcda0=1", "月圆霖"),
    //    SmallMarketInfo("5fd54d5a-403a-4c75-9da4-c4e687ef07cf=1", "迎宾一号"),
    //    // section2
    //    SmallMarketInfo("01d3d0fc-a47e-47dc-bf9f-8c1f5492ad24=2", "郦湾国际酒店"),
    //    SmallMarketInfo("392359e6-b6eb-4098-bad8-9fc4c43110df=2", "寅生国际酒店"),
    //    SmallMarketInfo("02d6ccf1-46e0-4d28-83dd-769bfa8ec4b0=2", "大蓉和(一品天下旗舰店)"),
    //    SmallMarketInfo("e22c5f92-70a8-4a81-a6ff-cb3c7a7d87db=2", "林海山庄(环港路)"),
    //    SmallMarketInfo("3e47de5d-e076-454d-b0f8-d555064a96c3=2", "川西人居"),
    //    SmallMarketInfo("710629f0-31e1-45fd-903c-d670055fb327=2", "西蜀人家"),
    //    SmallMarketInfo("29672fa4-4640-4a2f-b0a8-f868d54e67a7=2", "漫花庄园"),
    //    SmallMarketInfo("c461e101-23db-4cf0-8824-9fdfb442271c=2", "成都牧山沁园酒店"),
    //    SmallMarketInfo("d9105038-f307-431e-bb51-4c25e5335d45=2", "洁惠花园饭店"),
    //    SmallMarketInfo("6af69637-105e-4647-b592-937a8742e208=2", "老房子(毗河店)"),
    //    // section3
    //    SmallMarketInfo("bd64ef3a-40ee-42fc-8289-fc57e2f177a4=3", "映月湖酒店"),
    //    SmallMarketInfo("057e09d5-9c3d-48c2-8d85-1338ef133625=3", "川投国际酒店"),
    //    SmallMarketInfo("da844c8f-c413-4733-8ad8-b3f644a93a98=3", "友豪·罗曼大酒店"),
    //    SmallMarketInfo("2dd8454a-16cb-434f-aff7-6eba8454c341=3", "鹿归国际酒店"),
    //    SmallMarketInfo("11dcc500-2cef-44d0-bfcd-a70014090031=3", "桂湖国际大酒店"),
    //    SmallMarketInfo("3cb310bd-c2bf-4c83-82ab-40280f232e24=3", "喜鹊先生花园餐厅"),
    //    SmallMarketInfo("c058f54d-8c4e-4fa7-978e-412f0536c816=3", "菁华园"),
    //    SmallMarketInfo("787eaa99-7092-4593-9db6-39353f1e38cd=3", "顺兴老(世纪城店)"),
    //    SmallMarketInfo("4032eb26-2b39-4fea-bb9b-fc4491b0b171=3", "蒋排骨顺湖园"),
    //    SmallMarketInfo("90c25b75-9ac0-4c8e-a4bb-5c898753859a=3", "喜馆精品酒店"),
    //    // section4
    //    SmallMarketInfo("8996068b-0ceb-46e1-a366-d70282b5eb1d=4", "智汇堂枫泽大酒店"),
    //    SmallMarketInfo("c3dc2c80-4571-4ecc-a5f0-9b380cb8a163=4", "中胜大酒店"),
    //    SmallMarketInfo("29672fa4-4640-4a2f-b0a8-f868d54e67a7=4", "漫花庄园"),
    //    SmallMarketInfo("057e09d5-9c3d-48c2-8d85-1338ef133625=4", "川投国际酒店"),
    //    SmallMarketInfo("c6234b4f-aa8e-4949-be1a-075e639f2157=4", "锦亦缘新派川菜"),
    //    SmallMarketInfo("16c3174a-0a36-46b9-bbf7-7fde885932fb=4", "嘉莱特精典国际酒店"),
    //    SmallMarketInfo("af155b48-3149-45af-8625-16c42d5570e3=4", "明宇豪雅饭店(科华店)"),
    //    SmallMarketInfo("724ee82d-57ba-4e70-913a-d868508322a5=4", "老房子华粹元年食府(天府三街店)"),
    //    SmallMarketInfo("9ff71e0b-b881-4718-9319-f87f5ce82f40=4", "重庆澳维酒店(澳维酒店港式茶餐厅)"),
    //    SmallMarketInfo("3e3e86d6-3b2a-4446-8ae0-bdc4ec158489=4", "香城竹韵(斑竹园店)"),
    //    // section5
    //    SmallMarketInfo("97c6330d-26f1-4c67-ae4f-5bbddfb79f9e=5", "世茂成都茂御酒店"),
    //    SmallMarketInfo("5b967168-f07f-4f09-b7c0-55ada1d9dec1=5", "明宇丽雅悦酒店"),
    //    SmallMarketInfo("ebb0bdaf-214c-4633-b121-4ebd2da5fc85=5", "星宸航都国际酒店"),
    //    SmallMarketInfo("8936bf11-c356-4115-9f5f-c7b6c828b46b=5", "成都凯宾斯基饭店"),
    //    SmallMarketInfo("a9fe4b14-5f45-47c2-bc35-4d36fcdaae05=5", "交子国际酒店"),
    //    SmallMarketInfo("1fa69245-c5bc-4107-9e06-73ebbe0879c0=5", "世外桃源酒店"),
    //    SmallMarketInfo("724ee82d-57ba-4e70-913a-d868508322a5=5", "老房子华粹元年食府(天府三街店)"),
    //    SmallMarketInfo("7ccfdc8f-7237-4d8f-a002-f6f235880334=5", "和淦·香城竹韵"),
    //    SmallMarketInfo("1158cc09-daa5-4942-a84d-5b2e02238a3c=5", "家园国际酒店"),
    //    SmallMarketInfo("b50fcf50-9cfb-4c48-8115-9cc47e89a521=5", "成都雅居乐豪生大酒店"),
    //
    //    // 第8波策略多面体酒店
    //    // section1
    //    SmallMarketInfo("20c2a7e2-f124-495a-9569-9ba2e3914d37=1", "大蓉和拉德方斯(精品店)"),
    //    SmallMarketInfo("37a6faab-3a18-4e89-b68d-cdc756a88da6=1", "阿斯牛牛·凉山菜(新会展店)"),
    //    SmallMarketInfo("d3892737-7236-4c7e-9318-7dfd12b53d63=1", "贵府酒楼(蜀辉路)"),
    //    SmallMarketInfo("8996068b-0ceb-46e1-a366-d70282b5eb1d=1", "智汇堂枫泽大酒店"),
    //    SmallMarketInfo("749d1dff-f0fb-4780-b183-d89c2b1519c1=1", "上层名人酒店"),
    //    SmallMarketInfo("c8258e5b-9e19-4573-b63d-6cf59016762c=1", "文杏酒楼(一品天下店)"),
    //    SmallMarketInfo("c94f43f4-fbb9-4ab6-8668-bc8678f333b7=1", "蔚然花海"),
    //    SmallMarketInfo("09f8e3bd-0053-44c0-85ff-3939690ecb09=1", "望江宾馆"),
    //    SmallMarketInfo("02ebfc9f-9180-4a0d-a74f-2e1c13e2fad0=1", "锦峰大酒店"),
    //    SmallMarketInfo("724ee82d-57ba-4e70-913a-d868508322a5=1", "老房子华粹元年食府(天府三街店)"),
    //    // section2
    //    SmallMarketInfo("9c4eb1af-4d9f-4bbf-b823-ce2fb08827c9=2", "红高粱海鲜量贩酒楼(红光店)"),
    //    SmallMarketInfo("889004a8-70bd-4b99-b669-81167105560d=2", "智谷云尚丽呈华廷酒店"),
    //    SmallMarketInfo("7992b693-5d0b-4c64-9463-e1f22c50805e=2", "第一江南酒店"),
    //    SmallMarketInfo("3a1baa7f-17f7-4a66-946a-2bf0061c1f20=2", "红杏酒家(羊西店)"),
    //    SmallMarketInfo("dc4bb742-6e35-4b9a-9631-58bb035de764=2", "广都国际酒店"),
    //    SmallMarketInfo("fd985b07-db6a-4203-a8ef-b4389773b695=2", "水香雅舍(三圣乡店)"),
    //    SmallMarketInfo("92905055-163e-4081-a074-727fb00b5cc6=2", "红杏酒家(万达广场金牛店)"),
    //    SmallMarketInfo("d6928f86-cdc8-47bb-aa8e-117b1e8a008b=2", "呈祥·东馆"),
    //    SmallMarketInfo("ec523414-19bc-499e-9090-64460d76bc77=2", "杰恩酒店(旗舰店)"),
    //    SmallMarketInfo("c6ff560b-9819-4321-88a6-5b034b62ce89=2", "红杏酒家·宴会厅(锦华店)"),
    //    SmallMarketInfo("e58f3c73-d4a3-4103-a49a-508d2dadd2c5=2", "红杏酒家(金府店)"),
    //    // section3
    //    SmallMarketInfo("1158cc09-daa5-4942-a84d-5b2e02238a3c=3", "家园国际酒店"),
    //    SmallMarketInfo("3cb310bd-c2bf-4c83-82ab-40280f232e24=3", "喜鹊先生花园餐厅"),
    //    SmallMarketInfo("ccf94653-eaa9-4e07-97ae-9d7da264134e=3", "新皇城大酒店"),
    //    SmallMarketInfo("c6234b4f-aa8e-4949-be1a-075e639f2157=3", "锦亦缘新派川菜"),
    //    SmallMarketInfo("f6597d6f-9125-4367-b685-2fb3e5510c0a=3", "友豪锦江酒店"),
    //    SmallMarketInfo("8936bf11-c356-4115-9f5f-c7b6c828b46b=3", "成都凯宾斯基饭店"),
    //    SmallMarketInfo("33d34c3c-a184-48d8-bb30-dd78925d27e3=3", "闲亭(峨影店)"),
    //    SmallMarketInfo("5b967168-f07f-4f09-b7c0-55ada1d9dec1=3", "明宇丽雅悦酒店"),
    //    SmallMarketInfo("b96db01c-ecd6-4f9a-8cd0-23ff4e0d7512=3", "应龙湾澜岸酒店"),
    //    SmallMarketInfo("b32317c0-2613-4a75-aeee-6ac4dc726f83=3", "玉瑞酒店"),
    //    // section4
    //    SmallMarketInfo("5900742b-4290-4bc3-ae8d-4db7b1115557=4", "泰清锦宴"),
    //    SmallMarketInfo("ae5d93e3-481a-42fe-9643-c0387321d9ae=4", "博瑞花园酒店"),
    //    SmallMarketInfo("c0aa9212-9aed-4801-800d-8c412e972c49=4", "首席·1956"),
    //    SmallMarketInfo("72fd3bdd-69d6-4dca-b7cf-d48644ac8e58=4", "明宇尚雅饭店"),
    //    SmallMarketInfo("3e0921fb-4957-4eeb-9640-334455227c6e=4", "西苑半岛酒店"),
    //    SmallMarketInfo("f59f8c98-5270-4c45-804b-3bcca78efa00=4", "艺朗酒店"),
    //    SmallMarketInfo("0346b938-2a7b-4b70-bbc4-1bd500547497=4", "林道假日酒店"),
    //    SmallMarketInfo("d889a8d4-c60a-405c-a94d-b783c223560c=4", "明宇豪雅·怡品堂(东大街店)"),
    //    SmallMarketInfo("16c90a2c-fd6c-4166-95c6-f1cff3c178e5=4", "岷江新濠酒店宴会厅"),
    //    SmallMarketInfo("dd99abe5-d2f2-4ae0-8c22-579251ed1d7d=4", "成都金韵酒店"),
    //    // section5
    //    SmallMarketInfo("09f8e3bd-0053-44c0-85ff-3939690ecb09=5", "望江宾馆"),
    //    SmallMarketInfo("39b08285-d287-46ba-abca-70acb0c58898=5", "南昌万达嘉华酒店"),
    //    SmallMarketInfo("9d4e835b-5865-4627-bfda-be56199cbcb2=5", "禧悦酒店"),
    //    SmallMarketInfo("a0234282-d242-45e3-a5a7-55653be2d667=5", "南昌力高皇冠假日酒店"),
    //    SmallMarketInfo("d6a014e2-de76-4285-bb27-b31b1cf339c1=5", "豪雅东方花园酒店"),
    //    SmallMarketInfo("8a19bcfb-909d-4388-9d1f-e59ef9b989c1=5", "南昌万达文华酒店"),
    //    SmallMarketInfo("ebb0bdaf-214c-4633-b121-4ebd2da5fc85=5", "星宸航都国际酒店"),
    //    SmallMarketInfo("1158cc09-daa5-4942-a84d-5b2e02238a3c=5", "家园国际酒店"),
    //    SmallMarketInfo("8fe4d72d-8c16-422c-80aa-2c70453e9ca1=5", "南昌喜来登酒店"),
    //    SmallMarketInfo("f6597d6f-9125-4367-b685-2fb3e5510c0a=5", "友豪锦江酒店"),
    //
    //    // 第9波策略多面体酒店
    //    // section1
    //    SmallMarketInfo("b2b26408-4b8c-49e4-98a3-122ce40d9888=1", "保利国际高尔夫花园"),
    //    SmallMarketInfo("b2b26408-4b8c-49e4-98a3-122ce40d9888=1", "保利国际高尔夫花园"),
    //    SmallMarketInfo("d3b26bad-787c-4e1e-92f2-1620c1e1733f=1", "南昌喜来登酒店-大宴会厅"),
    //    SmallMarketInfo("d6a014e2-de76-4285-bb27-b31b1cf339c1=1", "豪雅东方花园酒店"),
    //    SmallMarketInfo("c3d64db1-995f-46ef-a32a-e4acad7469d7=1", "东方豪景花园酒店"),
    //    SmallMarketInfo("020f5fef-d0b5-48da-8e03-dee0c61adeea=1", "银桦半岛酒店"),
    //    SmallMarketInfo("39b08285-d287-46ba-abca-70acb0c58898=1", "南昌万达嘉华酒店"),
    //    SmallMarketInfo("965f750b-bcda-4a51-aedd-94563652c0fe=1", "成都茂业JW万豪酒店"),
    //    SmallMarketInfo("20c2a7e2-f124-495a-9569-9ba2e3914d37=1", "大蓉和拉德方斯(精品店)"),
    //    SmallMarketInfo("403bc1b9-d66d-4d6a-872c-c9fe736de3f0=1", "岷山饭店"),
    //    SmallMarketInfo("d72ca7a2-74d6-4780-9a5b-51ba1284fccf=1", "诺亚方舟(羊犀店)"),
    //    // section2
    //    SmallMarketInfo("c4dfa2b8-a627-46d7-b139-e0d40d952110=2", "彭州信一雅阁酒店"),
    //    SmallMarketInfo("85d91ae3-dda3-4d20-bfcb-2e4de544879c=2", "东篱翠湖"),
    //    SmallMarketInfo("c86348e9-cfe5-4ccd-961e-bcc7e1b7240f=2", "成飞宾馆"),
    //    SmallMarketInfo("51f8208c-2d64-42fa-8ead-705a4fae8f26=2", "刘家花园"),
    //    SmallMarketInfo("4e496ec5-9234-4d2c-a448-a1f82ab1d915=2", "赣江宾馆"),
    //    SmallMarketInfo("ea98d4eb-f3a5-4685-8984-cad046b720b1=2", "欢聚一堂"),
    //    SmallMarketInfo("9fc5ac91-e027-4025-8a5e-7b024670acad=2", "西蜀森林酒店"),
    //    SmallMarketInfo("749d1dff-f0fb-4780-b183-d89c2b1519c1=2", "上层名人酒店"),
    //    SmallMarketInfo("c0e20114-bf80-426d-8817-a40e6c5853dc=2", "俏巴渝(爱琴海购物公园店)"),
    //    SmallMarketInfo("4f0ce7c0-7c40-4ae8-acb9-90ea1a47b6ff=2", "新华国际酒店"),
    //    // section3
    //    SmallMarketInfo("c4dfa2b8-a627-46d7-b139-e0d40d952110=3", "彭州信一雅阁酒店"),
    //    SmallMarketInfo("462aee34-67b5-4851-aa2e-002aafc2a22c=3", "西苑半岛"),
    //    SmallMarketInfo("43264973-17ad-4ce4-ac2b-15e2a02b7642=3", "金河宾馆"),
    //    SmallMarketInfo("85d91ae3-dda3-4d20-bfcb-2e4de544879c=3", "东篱翠湖"),
    //    SmallMarketInfo("b22d46ba-a7f4-4da6-9977-0a314778c9e3=3", "南昌江景假日酒店"),
    //    SmallMarketInfo("4a3b30ac-73a9-4c5c-81f4-94be28fbeeec=3", "不二山房"),
    //    SmallMarketInfo("852d1616-5c9c-4ca3-8b27-54fa5362f191=3", "南昌瑞颐大酒店(东门)"),
    //    SmallMarketInfo("9d4e835b-5865-4627-bfda-be56199cbcb2=3", "禧悦酒店"),
    //    SmallMarketInfo("a909a7fb-2b8d-4643-a507-6896ea9faf0e=3", "西藏饭店"),
    //    SmallMarketInfo("f102bd3f-f02f-4452-8e17-38a409a7b257=3", "金阳尚城酒店"),
    //    // section4
    //    SmallMarketInfo("e812e6b3-d881-45e2-b5b3-e168bca74ff7=4", "尚成十八步岛酒店"),
    //    SmallMarketInfo("c5b139d3-57d9-4095-b422-81452d82d158=4", "南昌香格里拉大酒店"),
    //    SmallMarketInfo("c0aa9212-9aed-4801-800d-8c412e972c49=4", "首席·1956"),
    //    SmallMarketInfo("f6def674-452b-4a16-9030-33d88daf6756=4", "星辰航都国际酒店销售中心"),
    //    SmallMarketInfo("92bd6809-7105-4952-8de8-77bae6c896eb=4", "南昌绿地华邑酒店"),
    //    SmallMarketInfo("d764c9de-22bc-4c67-a262-3e2ac2b39cb7=4", "成都空港大酒店"),
    //    SmallMarketInfo("87395129-729f-46ec-8e22-7322b129c54a=4", "南昌凯美开元名都大酒店"),
    //    SmallMarketInfo("c0b134fb-493d-4b07-bed4-c48df9def143=4", "席锦酒家"),
    //    SmallMarketInfo("9ee77461-0e82-418e-ba27-4bfa7cf2f242=4", "瑞升·芭富丽大酒店"),
    //    SmallMarketInfo("0346b938-2a7b-4b70-bbc4-1bd500547497=4", "林道假日酒店"),
    //    // section5
    //    SmallMarketInfo("f12fe4aa-84bc-45bb-b1e0-68a7367b6828=5", "麓山国际社区"),
    //
    //    // 追加
    //    SmallMarketInfo("b3cc4f38-4c52-4274-a83f-39f6a0515d7a=2", "成都世纪城天堂洲际大饭店"),
    //    SmallMarketInfo("57290e8c-adab-48c6-ae2f-96c397a8aa28=4", "西蜀森林酒店(西南1门)"),
    //    SmallMarketInfo("1c8e755c-2a4e-4b3f-8e38-441294b74693=2", "墨宴"),
    //    SmallMarketInfo("ebb0bdaf-214c-4633-b121-4ebd2da5fc85=4", "星宸航都国际酒店"),
    //    SmallMarketInfo("4d1fe867-ec24-4cca-bed2-d82c2e9120b2=2", "金丰翔精品湘菜酒楼"),
    //    SmallMarketInfo("ed8da82f-5aec-40a8-9b24-7ba3c289c0b8=3", "南堂馆(天府三街店)"),
    //    SmallMarketInfo("e812e6b3-d881-45e2-b5b3-e168bca74ff7=3", "尚成十八步岛酒店"),
    //    SmallMarketInfo("a335c367-4d9a-4d6b-90c3-fc369703f147=5", "成都大鼎戴斯大酒店"),
    //    SmallMarketInfo("3cb310bd-c2bf-4c83-82ab-40280f232e24=2", "喜鹊先生花园餐厅"),
    //    SmallMarketInfo("0cec375e-39e5-433c-86f2-92b55db63cfc=1", "成都天河智选假日酒店(犀浦合能橙中心店)"),
    //    SmallMarketInfo("0cec375e-39e5-433c-86f2-92b55db63cfc=2", "成都天河智选假日酒店(犀浦合能橙中心店)"),
    //    SmallMarketInfo("ebb0bdaf-214c-4633-b121-4ebd2da5fc85=3", "星宸航都国际酒店")
    RealLeaderAndYanJingEngine.SmallMarketInfo("d08b5174-a560-4438-a1ba-9cfe2df650f3=4", "柏萃·白居度假酒店")
  )

  /**
    * 03-31 新增。针对眼睛数据统计逻辑进行修改，主要修改底层数据结构为Vector和mutable.HashMap
    *
    * @param hotel_id
    * @param hotel_name
    * @param section
    * @param map
    */
  case class YanJingDataObj(hotel_id: String, hotel_name: String, section: Int, map: mutable.HashMap[Double, Int])

  /**
    * 针对小批量小市场，统一进行提交
    *
    * @param originalDf
    * @param smInfoVector
    */
  def dataStatistics3(originalDf: DataFrame, smInfoVector: Vector[SmallMarketInfo]): Unit = {
    val conn = JavaSQLServerConn.getConnection
    val otherInterval: Vector[Double] = SomeUtils.getSplitIntervalSettingVector(-1, 1, 200, "[]")
    val displayAmountInterval: Vector[Double] = SomeUtils.getSplitIntervalSettingVector(0, 5000, 100, "[]")

    var tup3CaseDotVector: Vector[YanJingDataObj] = Vector()
    var tup3ReorderRateVector: Vector[YanJingDataObj] = Vector()
    var tup3CommunicationLevelVector: Vector[YanJingDataObj] = Vector()
    var tup3DesignSenseVector: Vector[YanJingDataObj] = Vector()
    var tup3CaseRateVector: Vector[YanJingDataObj] = Vector()
    var tup3AllScoreFinalVector: Vector[YanJingDataObj] = Vector()
    var tup3NumberVector: Vector[YanJingDataObj] = Vector()
    var tup3TextRatingRateVector: Vector[YanJingDataObj] = Vector()
    var tup3DisplayAmountVector: Vector[YanJingDataObj] = Vector()
    var tup3ToStoreRateVector: Vector[YanJingDataObj] = Vector()

    for (list <- smInfoVector) { // 直接进行小市场信息的Vector遍历，来自外部传输
      val hotel_id = list.idAndSection.split("=")(0)
      val section = list.idAndSection.split("=")(1).toInt
      val hotel_name = list.name
      //      val hotel_id = list._1.split("=")(0)
      //      val section = list._1.split("=")(1).toInt
      //      val hotel_name = list._2
      val needDf: Dataset[Row] = originalDf.filter(row => {
        val sec = row.getAs[Long]("section").toInt
        sec == section
      }).filter(row => {
        val worker_id = row.getAs[String]("worker_id")
        val distance = JavaHBaseUtils.getValue("v2_rp_workers_hotel_distance", hotel_id + ":" + worker_id, "info", "distance")
        (distance != null && !distance.equals("") && (distance.toFloat / 1000) <= 20)
      })

      // 直接对一个DF进行数据统计，取代原来的分条统计
      var caseDotVector = Vector[Double]()
      var reorderRateVector = Vector[Double]()
      var communicationLevelVector = Vector[Double]()
      var designSenseVector = Vector[Double]()
      var caseRateVector = Vector[Double]()
      var allScoreFinalVector = Vector[Double]()
      var numberVector = Vector[Double]()
      var textRatingRateVector = Vector[Double]()
      var displayAmountVector = Vector[Double]()
      var toStoreRateVector = Vector[Double]()
      needDf.collect().foreach(row => {
        caseDotVector = caseDotVector ++ Vector(row.getAs[Double]("case_dot"))
        reorderRateVector = reorderRateVector ++ Vector(row.getAs[Double]("reorder_rate"))
        communicationLevelVector = communicationLevelVector ++ Vector(row.getAs[Double]("communication_level"))
        designSenseVector = designSenseVector ++ Vector(row.getAs[Double]("design_sense"))
        caseRateVector = caseRateVector ++ Vector(row.getAs[Double]("case_rate"))
        allScoreFinalVector = allScoreFinalVector ++ Vector(row.getAs[Double]("all_score_final"))
        numberVector = numberVector ++ Vector(row.getAs[Double]("number"))
        textRatingRateVector = textRatingRateVector ++ Vector(row.getAs[Double]("text_rating_rate"))
        displayAmountVector = displayAmountVector ++ Vector(row.getAs[Double]("display_amount"))
        toStoreRateVector = toStoreRateVector ++ Vector(row.getAs[Double]("to_store_rate"))
      })
      val caseDotMap = SomeUtils.getRightIntervalBySetting22(caseDotVector, otherInterval)
      tup3CaseDotVector = tup3CaseDotVector ++ Vector[YanJingDataObj](YanJingDataObj(hotel_id, hotel_name, section, caseDotMap))
      val reorderRateMap = SomeUtils.getRightIntervalBySetting22(reorderRateVector, otherInterval)
      tup3ReorderRateVector = tup3ReorderRateVector ++ Vector[YanJingDataObj](YanJingDataObj(hotel_id, hotel_name, section, reorderRateMap))
      val communicationLevelMap = SomeUtils.getRightIntervalBySetting22(communicationLevelVector, otherInterval)
      tup3CommunicationLevelVector = tup3CommunicationLevelVector ++ Vector[YanJingDataObj](YanJingDataObj(hotel_id, hotel_name, section, communicationLevelMap))
      val designSenseMap = SomeUtils.getRightIntervalBySetting22(designSenseVector, otherInterval)
      tup3DesignSenseVector = tup3DesignSenseVector ++ Vector[YanJingDataObj](YanJingDataObj(hotel_id, hotel_name, section, designSenseMap))
      val caseRateMap = SomeUtils.getRightIntervalBySetting22(caseRateVector, otherInterval)
      tup3CaseRateVector = tup3CaseRateVector ++ Vector[YanJingDataObj](YanJingDataObj(hotel_id, hotel_name, section, caseRateMap))
      val allScoreFinalMap = SomeUtils.getRightIntervalBySetting22(allScoreFinalVector, otherInterval)
      tup3AllScoreFinalVector = tup3AllScoreFinalVector ++ Vector[YanJingDataObj](YanJingDataObj(hotel_id, hotel_name, section, allScoreFinalMap))
      val numberMap = SomeUtils.getRightIntervalBySetting22(numberVector, otherInterval)
      tup3NumberVector = tup3NumberVector ++ Vector[YanJingDataObj](YanJingDataObj(hotel_id, hotel_name, section, numberMap))
      val textRatingRateMap = SomeUtils.getRightIntervalBySetting22(textRatingRateVector, otherInterval)
      tup3TextRatingRateVector = tup3TextRatingRateVector ++ Vector[YanJingDataObj](YanJingDataObj(hotel_id, hotel_name, section, textRatingRateMap))
      val displayAmountMap = SomeUtils.getRightIntervalBySetting33(displayAmountVector, displayAmountInterval)
      tup3DisplayAmountVector = tup3DisplayAmountVector ++ Vector[YanJingDataObj](YanJingDataObj(hotel_id, hotel_name, section, displayAmountMap))
      val toStoreRateMap = SomeUtils.getRightIntervalBySetting22(toStoreRateVector, otherInterval)
      tup3ToStoreRateVector = tup3ToStoreRateVector ++ Vector[YanJingDataObj](YanJingDataObj(hotel_id, hotel_name, section, toStoreRateMap))

    }

    println("开始写入数据")
    val start = System.currentTimeMillis()
    saveData2SQLServerMatchName3(tup3CaseDotVector, "case_dot", conn)
    saveData2SQLServerMatchName3(tup3ReorderRateVector, "reorder_rate", conn)
    saveData2SQLServerMatchName3(tup3CommunicationLevelVector, "communication_level", conn)
    saveData2SQLServerMatchName3(tup3DesignSenseVector, "design_sense", conn)
    saveData2SQLServerMatchName3(tup3CaseRateVector, "case_rate", conn)
    saveData2SQLServerMatchName3(tup3AllScoreFinalVector, "all_score_final", conn)
    saveData2SQLServerMatchName3(tup3NumberVector, "number", conn)
    saveData2SQLServerMatchName3(tup3TextRatingRateVector, "text_rating_rate", conn)
    saveData2SQLServerMatchName3(tup3DisplayAmountVector, "display_amount", conn)
    saveData2SQLServerMatchName3(tup3ToStoreRateVector, "to_store_rate", conn)
    val stop = System.currentTimeMillis()
    println("=============")
    println("写入time = " + (stop - start) / 1000 + "s.")
    println("=============")
    JavaSQLServerConn.closeConnection(conn)
  }

  /**
    * 50条一次提交
    *
    * @param originalDf
    */
  def dataStatistics4(originalDf: DataFrame, smInfoVector: Vector[SmallMarketInfo]): Unit = {
    val conn = JavaSQLServerConn.getConnection
    val otherInterval: Vector[Double] = SomeUtils.getSplitIntervalSettingVector(-1, 1, 200, "[]")
    val displayAmountInterval: Vector[Double] = SomeUtils.getSplitIntervalSettingVector(0, 5000, 100, "[]")

    var tup3CaseDotVector: Vector[YanJingDataObj] = Vector()
    var tup3ReorderRateVector: Vector[YanJingDataObj] = Vector()
    var tup3CommunicationLevelVector: Vector[YanJingDataObj] = Vector()
    var tup3DesignSenseVector: Vector[YanJingDataObj] = Vector()
    var tup3CaseRateVector: Vector[YanJingDataObj] = Vector()
    var tup3AllScoreFinalVector: Vector[YanJingDataObj] = Vector()
    var tup3NumberVector: Vector[YanJingDataObj] = Vector()
    var tup3TextRatingRateVector: Vector[YanJingDataObj] = Vector()
    var tup3DisplayAmountVector: Vector[YanJingDataObj] = Vector()
    var tup3ToStoreRateVector: Vector[YanJingDataObj] = Vector()

    var size = 0
    for (list <- smInfoVector) { // 直接进行小市场信息的Vector遍历，来自外部传输
      size = size + 1
      val hotel_id = list.idAndSection.split("=")(0)
      val section = list.idAndSection.split("=")(1).toInt
      val hotel_name = list.name
      val needDf: Dataset[Row] = originalDf.filter(row => {
        val sec = row.getAs[Long]("section").toInt
        sec == section
      }).filter(row => {
        val worker_id = row.getAs[String]("worker_id")
        val distance = JavaHBaseUtils.getValue("v2_rp_workers_hotel_distance", hotel_id + ":" + worker_id, "info", "distance")
        (distance != null && !distance.equals("") && (distance.toFloat / 1000) <= 20)
      })

      // 直接对一个DF进行数据统计，取代原来的分条统计
      var caseDotVector = Vector[Double]()
      var reorderRateVector = Vector[Double]()
      var communicationLevelVector = Vector[Double]()
      var designSenseVector = Vector[Double]()
      var caseRateVector = Vector[Double]()
      var allScoreFinalVector = Vector[Double]()
      var numberVector = Vector[Double]()
      var textRatingRateVector = Vector[Double]()
      var displayAmountVector = Vector[Double]()
      var toStoreRateVector = Vector[Double]()
      needDf.collect().foreach(row => {
        caseDotVector = caseDotVector ++ Vector(row.getAs[Double]("case_dot"))
        reorderRateVector = reorderRateVector ++ Vector(row.getAs[Double]("reorder_rate"))
        communicationLevelVector = communicationLevelVector ++ Vector(row.getAs[Double]("communication_level"))
        designSenseVector = designSenseVector ++ Vector(row.getAs[Double]("design_sense"))
        caseRateVector = caseRateVector ++ Vector(row.getAs[Double]("case_rate"))
        allScoreFinalVector = allScoreFinalVector ++ Vector(row.getAs[Double]("all_score_final"))
        numberVector = numberVector ++ Vector(row.getAs[Double]("number"))
        textRatingRateVector = textRatingRateVector ++ Vector(row.getAs[Double]("text_rating_rate"))
        displayAmountVector = displayAmountVector ++ Vector(row.getAs[Double]("display_amount"))
        toStoreRateVector = toStoreRateVector ++ Vector(row.getAs[Double]("to_store_rate"))
      })
      val caseDotMap = SomeUtils.getRightIntervalBySetting22(caseDotVector, otherInterval)
      tup3CaseDotVector = tup3CaseDotVector ++ Vector[YanJingDataObj](YanJingDataObj(hotel_id, hotel_name, section, caseDotMap))
      val reorderRateMap = SomeUtils.getRightIntervalBySetting22(reorderRateVector, otherInterval)
      tup3ReorderRateVector = tup3ReorderRateVector ++ Vector[YanJingDataObj](YanJingDataObj(hotel_id, hotel_name, section, reorderRateMap))
      val communicationLevelMap = SomeUtils.getRightIntervalBySetting22(communicationLevelVector, otherInterval)
      tup3CommunicationLevelVector = tup3CommunicationLevelVector ++ Vector[YanJingDataObj](YanJingDataObj(hotel_id, hotel_name, section, communicationLevelMap))
      val designSenseMap = SomeUtils.getRightIntervalBySetting22(designSenseVector, otherInterval)
      tup3DesignSenseVector = tup3DesignSenseVector ++ Vector[YanJingDataObj](YanJingDataObj(hotel_id, hotel_name, section, designSenseMap))
      val caseRateMap = SomeUtils.getRightIntervalBySetting22(caseRateVector, otherInterval)
      tup3CaseRateVector = tup3CaseRateVector ++ Vector[YanJingDataObj](YanJingDataObj(hotel_id, hotel_name, section, caseRateMap))
      val allScoreFinalMap = SomeUtils.getRightIntervalBySetting22(allScoreFinalVector, otherInterval)
      tup3AllScoreFinalVector = tup3AllScoreFinalVector ++ Vector[YanJingDataObj](YanJingDataObj(hotel_id, hotel_name, section, allScoreFinalMap))
      val numberMap = SomeUtils.getRightIntervalBySetting22(numberVector, otherInterval)
      tup3NumberVector = tup3NumberVector ++ Vector[YanJingDataObj](YanJingDataObj(hotel_id, hotel_name, section, numberMap))
      val textRatingRateMap = SomeUtils.getRightIntervalBySetting22(textRatingRateVector, otherInterval)
      tup3TextRatingRateVector = tup3TextRatingRateVector ++ Vector[YanJingDataObj](YanJingDataObj(hotel_id, hotel_name, section, textRatingRateMap))
      val displayAmountMap = SomeUtils.getRightIntervalBySetting33(displayAmountVector, displayAmountInterval)
      tup3DisplayAmountVector = tup3DisplayAmountVector ++ Vector[YanJingDataObj](YanJingDataObj(hotel_id, hotel_name, section, displayAmountMap))
      val toStoreRateMap = SomeUtils.getRightIntervalBySetting22(toStoreRateVector, otherInterval)
      tup3ToStoreRateVector = tup3ToStoreRateVector ++ Vector[YanJingDataObj](YanJingDataObj(hotel_id, hotel_name, section, toStoreRateMap))

      if (size % 50 == 0) {
        println("==============开始局部写入数据: ")
        saveData2SQLServerMatchName3(tup3CaseDotVector, "case_dot", conn)
        saveData2SQLServerMatchName3(tup3ReorderRateVector, "reorder_rate", conn)
        saveData2SQLServerMatchName3(tup3CommunicationLevelVector, "communication_level", conn)
        saveData2SQLServerMatchName3(tup3DesignSenseVector, "design_sense", conn)
        saveData2SQLServerMatchName3(tup3CaseRateVector, "case_rate", conn)
        saveData2SQLServerMatchName3(tup3AllScoreFinalVector, "all_score_final", conn)
        saveData2SQLServerMatchName3(tup3NumberVector, "number", conn)
        saveData2SQLServerMatchName3(tup3TextRatingRateVector, "text_rating_rate", conn)
        saveData2SQLServerMatchName3(tup3DisplayAmountVector, "display_amount", conn)
        saveData2SQLServerMatchName3(tup3ToStoreRateVector, "to_store_rate", conn)

        caseDotVector = caseDotVector.drop(caseDotVector.size)
        reorderRateVector = reorderRateVector.drop(reorderRateVector.size)
        communicationLevelVector = communicationLevelVector.drop(communicationLevelVector.size)
        designSenseVector = designSenseVector.drop(designSenseVector.size)
        caseRateVector = caseRateVector.drop(caseRateVector.size)
        allScoreFinalVector = allScoreFinalVector.drop(allScoreFinalVector.size)
        numberVector = numberVector.drop(numberVector.size)
        textRatingRateVector = textRatingRateVector.drop(textRatingRateVector.size)
        displayAmountVector = displayAmountVector.drop(displayAmountVector.size)
        toStoreRateVector = toStoreRateVector.drop(toStoreRateVector.size)
        println("写入50个小市场完成。")
      }
    }

    saveData2SQLServerMatchName3(tup3CaseDotVector, "case_dot", conn)
    saveData2SQLServerMatchName3(tup3ReorderRateVector, "reorder_rate", conn)
    saveData2SQLServerMatchName3(tup3CommunicationLevelVector, "communication_level", conn)
    saveData2SQLServerMatchName3(tup3DesignSenseVector, "design_sense", conn)
    saveData2SQLServerMatchName3(tup3CaseRateVector, "case_rate", conn)
    saveData2SQLServerMatchName3(tup3AllScoreFinalVector, "all_score_final", conn)
    saveData2SQLServerMatchName3(tup3NumberVector, "number", conn)
    saveData2SQLServerMatchName3(tup3TextRatingRateVector, "text_rating_rate", conn)
    saveData2SQLServerMatchName3(tup3DisplayAmountVector, "display_amount", conn)
    saveData2SQLServerMatchName3(tup3ToStoreRateVector, "to_store_rate", conn)

    JavaSQLServerConn.closeConnection(conn)
  }

  /**
    * 04-03 Test.
    *
    */
  def dataStatisticsTest(originalDf: DataFrame, smInfoVector: Vector[SmallMarketInfo]): Unit = {
    val conn = JavaSQLServerConn.getConnection
    val otherInterval: Vector[Double] = SomeUtils.getSplitIntervalSettingVector(-1, 1, 200, "[]")
    val displayAmountInterval: Vector[Double] = SomeUtils.getSplitIntervalSettingVector(0, 5000, 100, "[]")

    var tup3CaseDotVector: Vector[YanJingDataObj] = Vector()
    var tup3ReorderRateVector: Vector[YanJingDataObj] = Vector()
    var tup3CommunicationLevelVector: Vector[YanJingDataObj] = Vector()
    var tup3DesignSenseVector: Vector[YanJingDataObj] = Vector()
    var tup3CaseRateVector: Vector[YanJingDataObj] = Vector()
    var tup3AllScoreFinalVector: Vector[YanJingDataObj] = Vector()
    var tup3NumberVector: Vector[YanJingDataObj] = Vector()
    var tup3TextRatingRateVector: Vector[YanJingDataObj] = Vector()
    var tup3DisplayAmountVector: Vector[YanJingDataObj] = Vector()
    var tup3ToStoreRateVector: Vector[YanJingDataObj] = Vector()

    val testVector = smInfoVector.take(22)

    var size = 0
    for (list <- testVector) { // 直接进行小市场信息的Vector遍历，来自外部传输
      size = size + 1
      val hotel_id = list.idAndSection.split("=")(0)
      val section = list.idAndSection.split("=")(1).toInt
      val hotel_name = list.name
      val needDf: Dataset[Row] = originalDf.filter(row => {
        val sec = row.getAs[Long]("section").toInt
        sec == section
      }).filter(row => {
        val worker_id = row.getAs[String]("worker_id")
        val distance = JavaHBaseUtils.getValue("v2_rp_workers_hotel_distance", hotel_id + ":" + worker_id, "info", "distance")
        (distance != null && !distance.equals("") && (distance.toFloat / 1000) <= 20)
      }).cache()

      // 直接对一个DF进行数据统计，取代原来的分条统计
      var caseDotVector = Vector[Double]()
      var reorderRateVector = Vector[Double]()
      var communicationLevelVector = Vector[Double]()
      var designSenseVector = Vector[Double]()
      var caseRateVector = Vector[Double]()
      var allScoreFinalVector = Vector[Double]()
      var numberVector = Vector[Double]()
      var textRatingRateVector = Vector[Double]()
      var displayAmountVector = Vector[Double]()
      var toStoreRateVector = Vector[Double]()
      needDf.collect().foreach(row => {
        caseDotVector = caseDotVector ++ Vector(row.getAs[Double]("case_dot"))
        reorderRateVector = reorderRateVector ++ Vector(row.getAs[Double]("reorder_rate"))
        communicationLevelVector = communicationLevelVector ++ Vector(row.getAs[Double]("communication_level"))
        designSenseVector = designSenseVector ++ Vector(row.getAs[Double]("design_sense"))
        caseRateVector = caseRateVector ++ Vector(row.getAs[Double]("case_rate"))
        allScoreFinalVector = allScoreFinalVector ++ Vector(row.getAs[Double]("all_score_final"))
        numberVector = numberVector ++ Vector(row.getAs[Double]("number"))
        textRatingRateVector = textRatingRateVector ++ Vector(row.getAs[Double]("text_rating_rate"))
        displayAmountVector = displayAmountVector ++ Vector(row.getAs[Double]("display_amount"))
        toStoreRateVector = toStoreRateVector ++ Vector(row.getAs[Double]("to_store_rate"))
      })
      val caseDotMap = SomeUtils.getRightIntervalBySetting22(caseDotVector, otherInterval)
      tup3CaseDotVector = tup3CaseDotVector ++ Vector[YanJingDataObj](YanJingDataObj(hotel_id, hotel_name, section, caseDotMap))
      val reorderRateMap = SomeUtils.getRightIntervalBySetting22(reorderRateVector, otherInterval)
      tup3ReorderRateVector = tup3ReorderRateVector ++ Vector[YanJingDataObj](YanJingDataObj(hotel_id, hotel_name, section, reorderRateMap))
      val communicationLevelMap = SomeUtils.getRightIntervalBySetting22(communicationLevelVector, otherInterval)
      tup3CommunicationLevelVector = tup3CommunicationLevelVector ++ Vector[YanJingDataObj](YanJingDataObj(hotel_id, hotel_name, section, communicationLevelMap))
      val designSenseMap = SomeUtils.getRightIntervalBySetting22(designSenseVector, otherInterval)
      tup3DesignSenseVector = tup3DesignSenseVector ++ Vector[YanJingDataObj](YanJingDataObj(hotel_id, hotel_name, section, designSenseMap))
      val caseRateMap = SomeUtils.getRightIntervalBySetting22(caseRateVector, otherInterval)
      tup3CaseRateVector = tup3CaseRateVector ++ Vector[YanJingDataObj](YanJingDataObj(hotel_id, hotel_name, section, caseRateMap))
      val allScoreFinalMap = SomeUtils.getRightIntervalBySetting22(allScoreFinalVector, otherInterval)
      tup3AllScoreFinalVector = tup3AllScoreFinalVector ++ Vector[YanJingDataObj](YanJingDataObj(hotel_id, hotel_name, section, allScoreFinalMap))
      val numberMap = SomeUtils.getRightIntervalBySetting22(numberVector, otherInterval)
      tup3NumberVector = tup3NumberVector ++ Vector[YanJingDataObj](YanJingDataObj(hotel_id, hotel_name, section, numberMap))
      val textRatingRateMap = SomeUtils.getRightIntervalBySetting22(textRatingRateVector, otherInterval)
      tup3TextRatingRateVector = tup3TextRatingRateVector ++ Vector[YanJingDataObj](YanJingDataObj(hotel_id, hotel_name, section, textRatingRateMap))
      val displayAmountMap = SomeUtils.getRightIntervalBySetting33(displayAmountVector, displayAmountInterval)
      tup3DisplayAmountVector = tup3DisplayAmountVector ++ Vector[YanJingDataObj](YanJingDataObj(hotel_id, hotel_name, section, displayAmountMap))
      val toStoreRateMap = SomeUtils.getRightIntervalBySetting22(toStoreRateVector, otherInterval)
      tup3ToStoreRateVector = tup3ToStoreRateVector ++ Vector[YanJingDataObj](YanJingDataObj(hotel_id, hotel_name, section, toStoreRateMap))

      if (size % 5 == 0) {
        println("==============开始局部写入数据: ")
        saveData2SQLServerMatchName3(tup3CaseDotVector, "case_dot", conn)
        saveData2SQLServerMatchName3(tup3ReorderRateVector, "reorder_rate", conn)
        saveData2SQLServerMatchName3(tup3CommunicationLevelVector, "communication_level", conn)
        saveData2SQLServerMatchName3(tup3DesignSenseVector, "design_sense", conn)
        saveData2SQLServerMatchName3(tup3CaseRateVector, "case_rate", conn)
        saveData2SQLServerMatchName3(tup3AllScoreFinalVector, "all_score_final", conn)
        saveData2SQLServerMatchName3(tup3NumberVector, "number", conn)
        saveData2SQLServerMatchName3(tup3TextRatingRateVector, "text_rating_rate", conn)
        saveData2SQLServerMatchName3(tup3DisplayAmountVector, "display_amount", conn)
        saveData2SQLServerMatchName3(tup3ToStoreRateVector, "to_store_rate", conn)

        tup3CaseDotVector = tup3CaseDotVector.drop(tup3CaseDotVector.size)
        tup3ReorderRateVector = tup3ReorderRateVector.drop(tup3ReorderRateVector.size)
        tup3CommunicationLevelVector = tup3CommunicationLevelVector.drop(tup3CommunicationLevelVector.size)
        tup3DesignSenseVector = tup3DesignSenseVector.drop(tup3DesignSenseVector.size)
        tup3CaseRateVector = tup3CaseRateVector.drop(tup3CaseRateVector.size)
        tup3AllScoreFinalVector = tup3AllScoreFinalVector.drop(tup3AllScoreFinalVector.size)
        tup3NumberVector = tup3NumberVector.drop(tup3NumberVector.size)
        tup3TextRatingRateVector = tup3TextRatingRateVector.drop(tup3TextRatingRateVector.size)
        tup3DisplayAmountVector = tup3DisplayAmountVector.drop(tup3DisplayAmountVector.size)
        tup3ToStoreRateVector = tup3ToStoreRateVector.drop(tup3ToStoreRateVector.size)
        println("写入5个小市场完成。")
      }
    }

    println("==============开始尾部写入数据: ")

    saveData2SQLServerMatchName3(tup3CaseDotVector, "case_dot", conn)
    saveData2SQLServerMatchName3(tup3ReorderRateVector, "reorder_rate", conn)
    saveData2SQLServerMatchName3(tup3CommunicationLevelVector, "communication_level", conn)
    saveData2SQLServerMatchName3(tup3DesignSenseVector, "design_sense", conn)
    saveData2SQLServerMatchName3(tup3CaseRateVector, "case_rate", conn)
    saveData2SQLServerMatchName3(tup3AllScoreFinalVector, "all_score_final", conn)
    saveData2SQLServerMatchName3(tup3NumberVector, "number", conn)
    saveData2SQLServerMatchName3(tup3TextRatingRateVector, "text_rating_rate", conn)
    saveData2SQLServerMatchName3(tup3DisplayAmountVector, "display_amount", conn)
    saveData2SQLServerMatchName3(tup3ToStoreRateVector, "to_store_rate", conn)

    JavaSQLServerConn.closeConnection(conn)
  }

  /**
    * 04-07 新增
    *
    * @param originalDf
    * @param smInfoVector
    */
  def dataStatistics5(originalDf: DataFrame, smInfoVector: Vector[SmallMarketInfo]): Unit = {
    val conn = JavaSQLServerConn.getConnection
    val otherInterval: Vector[Double] = SomeUtils.getSplitIntervalSettingVector(-1, 1, 200, "[]")
    val displayAmountInterval: Vector[Double] = SomeUtils.getSplitIntervalSettingVector(0, 5000, 100, "[]")

    var tup3CaseDotVector: Vector[YanJingDataObj] = Vector()
    var tup3ReorderRateVector: Vector[YanJingDataObj] = Vector()
    var tup3CommunicationLevelVector: Vector[YanJingDataObj] = Vector()
    var tup3DesignSenseVector: Vector[YanJingDataObj] = Vector()
    var tup3CaseRateVector: Vector[YanJingDataObj] = Vector()
    var tup3AllScoreFinalVector: Vector[YanJingDataObj] = Vector()
    var tup3NumberVector: Vector[YanJingDataObj] = Vector()
    var tup3TextRatingRateVector: Vector[YanJingDataObj] = Vector()
    var tup3DisplayAmountVector: Vector[YanJingDataObj] = Vector()
    var tup3ToStoreRateVector: Vector[YanJingDataObj] = Vector()

    var size = 0
    for (list <- smInfoVector) { // 直接进行小市场信息的Vector遍历，来自外部传输 ,针对每个小市场
      size = size + 1
      val hotel_id = list.idAndSection.split("=")(0)
      val section = list.idAndSection.split("=")(1).toInt
      val hotel_name = list.name
      val needDf: Dataset[Row] = originalDf.filter(row => {
        val sec = row.getAs[Long]("section").toInt
        sec == section
      }).filter(row => {
        val worker_id = row.getAs[String]("worker_id")
        val distance = JavaHBaseUtils.getValue("v2_rp_workers_hotel_distance", hotel_id + ":" + worker_id, "info", "distance")
        (distance != null && !distance.equals("") && (distance.toFloat / 1000) <= 20)
      })

      // 直接对一个DF进行数据统计，取代原来的分条统计
      var caseDotVector = Vector[Double]()
      var reorderRateVector = Vector[Double]()
      var communicationLevelVector = Vector[Double]()
      var designSenseVector = Vector[Double]()
      var caseRateVector = Vector[Double]()
      var allScoreFinalVector = Vector[Double]()
      var numberVector = Vector[Double]()
      var textRatingRateVector = Vector[Double]()
      var displayAmountVector = Vector[Double]()
      var toStoreRateVector = Vector[Double]()
      needDf.collect().foreach(row => {
        caseDotVector = caseDotVector ++ Vector(row.getAs[Double]("case_dot"))
        reorderRateVector = reorderRateVector ++ Vector(row.getAs[Double]("reorder_rate"))
        communicationLevelVector = communicationLevelVector ++ Vector(row.getAs[Double]("communication_level"))
        designSenseVector = designSenseVector ++ Vector(row.getAs[Double]("design_sense"))
        caseRateVector = caseRateVector ++ Vector(row.getAs[Double]("case_rate"))
        allScoreFinalVector = allScoreFinalVector ++ Vector(row.getAs[Double]("all_score_final"))
        numberVector = numberVector ++ Vector(row.getAs[Double]("number"))
        textRatingRateVector = textRatingRateVector ++ Vector(row.getAs[Double]("text_rating_rate"))
        displayAmountVector = displayAmountVector ++ Vector(row.getAs[Double]("display_amount"))
        toStoreRateVector = toStoreRateVector ++ Vector(row.getAs[Double]("to_store_rate"))
      })
      val caseDotMap = SomeUtils.getRightIntervalBySetting22(caseDotVector, otherInterval)
      tup3CaseDotVector = tup3CaseDotVector ++ Vector[YanJingDataObj](YanJingDataObj(hotel_id, hotel_name, section, caseDotMap))
      val reorderRateMap = SomeUtils.getRightIntervalBySetting22(reorderRateVector, otherInterval)
      tup3ReorderRateVector = tup3ReorderRateVector ++ Vector[YanJingDataObj](YanJingDataObj(hotel_id, hotel_name, section, reorderRateMap))
      val communicationLevelMap = SomeUtils.getRightIntervalBySetting22(communicationLevelVector, otherInterval)
      tup3CommunicationLevelVector = tup3CommunicationLevelVector ++ Vector[YanJingDataObj](YanJingDataObj(hotel_id, hotel_name, section, communicationLevelMap))
      val designSenseMap = SomeUtils.getRightIntervalBySetting22(designSenseVector, otherInterval)
      tup3DesignSenseVector = tup3DesignSenseVector ++ Vector[YanJingDataObj](YanJingDataObj(hotel_id, hotel_name, section, designSenseMap))
      val caseRateMap = SomeUtils.getRightIntervalBySetting22(caseRateVector, otherInterval)
      tup3CaseRateVector = tup3CaseRateVector ++ Vector[YanJingDataObj](YanJingDataObj(hotel_id, hotel_name, section, caseRateMap))
      val allScoreFinalMap = SomeUtils.getRightIntervalBySetting22(allScoreFinalVector, otherInterval)
      tup3AllScoreFinalVector = tup3AllScoreFinalVector ++ Vector[YanJingDataObj](YanJingDataObj(hotel_id, hotel_name, section, allScoreFinalMap))
      val numberMap = SomeUtils.getRightIntervalBySetting22(numberVector, otherInterval)
      tup3NumberVector = tup3NumberVector ++ Vector[YanJingDataObj](YanJingDataObj(hotel_id, hotel_name, section, numberMap))
      val textRatingRateMap = SomeUtils.getRightIntervalBySetting22(textRatingRateVector, otherInterval)
      tup3TextRatingRateVector = tup3TextRatingRateVector ++ Vector[YanJingDataObj](YanJingDataObj(hotel_id, hotel_name, section, textRatingRateMap))
      val displayAmountMap = SomeUtils.getRightIntervalBySetting33(displayAmountVector, displayAmountInterval)
      tup3DisplayAmountVector = tup3DisplayAmountVector ++ Vector[YanJingDataObj](YanJingDataObj(hotel_id, hotel_name, section, displayAmountMap))
      val toStoreRateMap = SomeUtils.getRightIntervalBySetting22(toStoreRateVector, otherInterval)
      tup3ToStoreRateVector = tup3ToStoreRateVector ++ Vector[YanJingDataObj](YanJingDataObj(hotel_id, hotel_name, section, toStoreRateMap))

      // 进行数据提交
      if (size % 50 == 0) {
        println("==============开始局部写入数据: ")
        saveData2SQLServerMatchName3(tup3CaseDotVector, "case_dot", conn)
        saveData2SQLServerMatchName3(tup3ReorderRateVector, "reorder_rate", conn)
        saveData2SQLServerMatchName3(tup3CommunicationLevelVector, "communication_level", conn)
        saveData2SQLServerMatchName3(tup3DesignSenseVector, "design_sense", conn)
        saveData2SQLServerMatchName3(tup3CaseRateVector, "case_rate", conn)
        saveData2SQLServerMatchName3(tup3AllScoreFinalVector, "all_score_final", conn)
        saveData2SQLServerMatchName3(tup3NumberVector, "number", conn)
        saveData2SQLServerMatchName3(tup3TextRatingRateVector, "text_rating_rate", conn)
        saveData2SQLServerMatchName3(tup3DisplayAmountVector, "display_amount", conn)
        saveData2SQLServerMatchName3(tup3ToStoreRateVector, "to_store_rate", conn)

        tup3CaseDotVector = Vector[YanJingDataObj]()
        tup3ReorderRateVector = Vector[YanJingDataObj]()
        tup3CommunicationLevelVector = Vector[YanJingDataObj]()
        tup3DesignSenseVector = Vector[YanJingDataObj]()
        tup3CaseRateVector = Vector[YanJingDataObj]()
        tup3AllScoreFinalVector = Vector[YanJingDataObj]()
        tup3NumberVector = Vector[YanJingDataObj]()
        tup3TextRatingRateVector = Vector[YanJingDataObj]()
        tup3DisplayAmountVector = Vector[YanJingDataObj]()
        tup3ToStoreRateVector = Vector[YanJingDataObj]()
        println("写入50个小市场完成。")
      }
    }

    saveData2SQLServerMatchName3(tup3CaseDotVector, "case_dot", conn)
    saveData2SQLServerMatchName3(tup3ReorderRateVector, "reorder_rate", conn)
    saveData2SQLServerMatchName3(tup3CommunicationLevelVector, "communication_level", conn)
    saveData2SQLServerMatchName3(tup3DesignSenseVector, "design_sense", conn)
    saveData2SQLServerMatchName3(tup3CaseRateVector, "case_rate", conn)
    saveData2SQLServerMatchName3(tup3AllScoreFinalVector, "all_score_final", conn)
    saveData2SQLServerMatchName3(tup3NumberVector, "number", conn)
    saveData2SQLServerMatchName3(tup3TextRatingRateVector, "text_rating_rate", conn)
    saveData2SQLServerMatchName3(tup3DisplayAmountVector, "display_amount", conn)
    saveData2SQLServerMatchName3(tup3ToStoreRateVector, "to_store_rate", conn)

    JavaSQLServerConn.closeConnection(conn)
  }

  /**
    * 批量写入小市场的case_dot统计表。
    *
    * @param vector
    * @param name
    * @param conn
    */
  def saveData2SQLServerMatchName3(vector: Vector[YanJingDataObj], name: String, conn: Connection): Unit = {
    var i = 0
    //设置批量处理的数量
    val batchSize = 5000
    var tableName = ""
    var field = ""
    var other_condition = ""
    name match {
      case "case_dot" => tableName = "canNum_v1_case_dot"; field = "case_dot"; other_condition = "Vj≠1"
      case "reorder_rate" => tableName = "canNum_v2_reorder_rate"; field = "reorder_rate"; other_condition = "Vj≠2"
      case "communication_level" => tableName = "canNum_v3_communication_level"; field = "communication_level"; other_condition = "Vj≠3"
      case "design_sense" => tableName = "canNum_v4_design_sense"; field = "design_sense"; other_condition = "Vj≠4"
      case "case_rate" => tableName = "canNum_v5_case_rate"; field = "case_rate"; other_condition = "Vj≠5"
      case "all_score_final" => tableName = "canNum_v6_all_score_final"; field = "all_score_final"; other_condition = "Vj≠6"
      case "number" => tableName = "canNum_v7_number"; field = "number"; other_condition = "Vj≠7"
      case "text_rating_rate" => tableName = "canNum_v8_text_rating_rate"; field = "text_rating_rate"; other_condition = "Vj≠8"
      case "display_amount" => tableName = "canNum_v9_display_amount"; field = "display_amount"; other_condition = "Vj≠9"
      case "to_store_rate" => tableName = "canNum_v10_to_store_rate"; field = "to_store_rate"; other_condition = "Vj≠10"
      case _ =>
    }
    val stmt = conn.prepareStatement(s"insert into $tableName(hotel_id,hotel_name,section,$field,other_condition,can_num,itime) VALUES (?,?,?,?,?,?,getdate())")
    conn.setAutoCommit(false)
    for (inL <- vector) {
      val hotel_id = inL.hotel_id
      val section = inL.section
      val hotel_name = inL.hotel_name
      for (map <- inL.map) {
        i = i + 1;
        val inField = map._1.toFloat
        val number = map._2
        stmt.setString(1, hotel_id)
        stmt.setString(2, hotel_name)
        stmt.setInt(3, section)
        stmt.setFloat(4, inField)
        stmt.setString(5, other_condition)
        stmt.setInt(6, number)
        stmt.addBatch()
        if (i % batchSize == 0) {
          stmt.executeBatch
          conn.commit
        }
      }
    }
    if (i % batchSize != 0) {
      stmt.executeBatch
      conn.commit
    }

    JavaSQLServerConn.closeStatement(stmt)
    //    JavaSQLServerConn.closeConnection(conn)
  }

  case class SmallMarketInfo(idAndSection: String, name: String)

  /**
    * 04-01新增。返回每天小市场信息Vector。
    */
  def makeOriginalTuple(): Vector[SmallMarketInfo] = {
    val hotelSectionDF = spark.read.format("jdbc")
      .option("url", JavaRealLeaderProperties.MYSQLURL)
      .option("dbtable", "hotel_cannum_section")
      .option("user", JavaRealLeaderProperties.MYSQLUSER)
      .option("password", JavaRealLeaderProperties.MYSQLPASSWORD)
      .load()

    val resultDf = hotelSectionDF.filter(row => {
      row.getAs[Long]("can_num") > 0
    }).select("hotel_id", "section").distinct()

    val resultVec = resultDf.collect().toVector

    var vector: Vector[SmallMarketInfo] = Vector[SmallMarketInfo]()
    resultVec.foreach(row => {
      val hotel_id = row.getAs[String]("hotel_id")
      val section = row.getAs[String]("section")
      val hotel_name = JavaHBaseUtils.getValue("v2_rp_tb_hotel", hotel_id, "info", "name")
      //      val hotel_name = "testName"
      if (hotel_name != null) {
        vector = vector ++ Vector[SmallMarketInfo](SmallMarketInfo(hotel_id + "=" + section, hotel_name))
      }
    })
    vector
  }

}
