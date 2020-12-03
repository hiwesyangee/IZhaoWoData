package com.realleader.engine

import java.sql.Timestamp
import java.util

import com.izhaowo.cores.utils.{JavaDateUtils, JavaHBaseUtils, JavaSparkUtils, MyUtils}
import com.realleader.properties.JavaRealLeaderProperties
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes

/**
  * 小市场策划师指标数据读写
  */
object RealLeaderEngine {

  val spark = JavaSparkUtils.getInstance().getSparkSession

  import spark.implicits._

  def readAndWriteRealLeaderData2HBase(): Unit = {
    val realLeaderDF = spark.read.format("jdbc")
      .option("url", JavaRealLeaderProperties.MYSQLURL)
      .option("dbtable", "真正领导数据源2")
      .option("user", JavaRealLeaderProperties.MYSQLUSER)
      .option("password", JavaRealLeaderProperties.MYSQLPASSWORD)
      .load()
      .withColumnRenamed("到店签单率", "to_store_rate")
      .withColumnRenamed("assessment_rate", "text_rating_rate")
      .withColumnRenamed("服务费", "display_amount")
      .filter(row => {
        val dt = row.getAs[Timestamp]("ctime")
        import java.text.SimpleDateFormat
        val formatter = new SimpleDateFormat("yyyyMMdd")
        val dateString = formatter.format(dt)
        dateString == MyUtils.getFromToday(0)
      })
    realLeaderDF.createOrReplaceTempView("rrr")

    val tbLowBudgetHotel2 = spark.read.format("jdbc")
      .option("url", JavaRealLeaderProperties.MYSQLURL)
      .option("dbtable", "tb_low_budget_hotel2")
      .option("user", JavaRealLeaderProperties.MYSQLUSER)
      .option("password", JavaRealLeaderProperties.MYSQLPASSWORD)
      .load()
      .withColumnRenamed("hotel", "hotel_name")
    tbLowBudgetHotel2.createOrReplaceTempView("hhh")

    val end = spark.sql("select hotel_id,hhh.section,hhh.worker_id,hotel_name,rrr.sort,distance,case_dot,reorder_rate,communication_level,design_sense,case_rate,all_score_final,number,text_rating_rate,display_amount,to_store_rate from rrr left join hhh on rrr.worker_id = hhh.worker_id and rrr.section = hhh.section")
    //      .filter(row => {
    //        (row.getAs[Double]("case_dot") != null &&
    //          row.getAs[Double]("reorder_rate") != null &&
    //          row.getAs[Double]("communication_level") != null &&
    //          row.getAs[Double]("design_sense") != null &&
    //          row.getAs[Double]("case_rate") != null &&
    //          row.getAs[Double]("all_score_final") != null &&
    //          row.getAs[Double]("number") != null &&
    //          row.getAs[Double]("text_rating_rate") != null &&
    //          row.getAs[Double]("display_amount") != null &&
    //          row.getAs[Double]("to_store_rate") != null
    //          )
    //      })

    //    end.show(false)
    //    println(end.count())

    val itime = JavaDateUtils.getDateYMDNew

    end.foreachPartition(ite => {
      ite.foreach(row => {
        try {
          val hotel_id = row.getAs[String]("hotel_id") // 1
          val section = row.getAs[Int]("section").toString // 2
          val worker_id = row.getAs[String]("worker_id") // 3
          val hotel_name = row.getAs[String]("hotel_name") // 4
          val sort = row.getAs[Double]("sort").toString // 5
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
          val list = new util.ArrayList[Put]()
          list.add(put)
          JavaHBaseUtils.putRows(JavaRealLeaderProperties.MVTBSMPLANNERINDICATOR, list)
        } catch {
          case e: Exception => println("新数据写入HBase失败. ")
        }
      })
    })

  }

  def readAndWriteRealLeaderOldData2HBase(): Unit = {
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

    val resultDf = spark.sql(s"select hhh.hotel_id,hhh.section,hhh.worker_id,hhh.sot,hhh.distance,case_dot,reorder_rate,communication_level,design_sense,case_rate,all_score_final,number,text_rating_rate,display_amount,to_store_rate from hhh left join aaa on hhh.worker_id = aaa.worker_id").distinct()
      .filter(row => {
        val hotel_id = row.getAs[String]("hotel_id")
        hotel_id.length == 36
      })

    //    println(resultDf.count())
    //    //    val en = resultDf.na.drop()
    //    val en = resultDf.filter(row => {
    //      val case_dot = row.getAs[Double]("case_dot")
    //      val reorder_rate = row.getAs[Double]("reorder_rate")
    //      val communication_level = row.getAs[Double]("communication_level")
    //      val design_sense = row.getAs[Double]("design_sense")
    //      val case_rate = row.getAs[Double]("case_rate")
    //      val all_score_final = row.getAs[Double]("all_score_final")
    //      val number = row.getAs[Double]("number")
    //      val text_rating_rate = row.getAs[Double]("text_rating_rate")
    //      val display_amount = row.getAs[Double]("display_amount")
    //      val to_store_rate = row.getAs[Double]("to_store_rate")
    //      (case_dot == null || reorder_rate == null || communication_level == null || design_sense == null
    //        || case_rate == null || all_score_final == null || number == null || text_rating_rate == null
    //        || display_amount == null || to_store_rate == null)
    //    })
    //    println(en.count())

    try {
      JavaHBaseUtils.deleteTable(JavaRealLeaderProperties.MVTBSMPLANNERINDICATOROLD)
      JavaHBaseUtils.createTable(JavaRealLeaderProperties.MVTBSMPLANNERINDICATOROLD, JavaRealLeaderProperties.cfsOfMVTBSMPLANNERINDICATOROLD)
    } catch {
      case e: Exception => e.printStackTrace()
    }

    val itime = JavaDateUtils.getDateYMDNew

    resultDf.foreachPartition(ite => {
      ite.foreach(row => {
        try {
          val hotel_id = row.getAs[String]("hotel_id") // 1
          val section = row.getAs[Int]("section").toString // 2
          val worker_id = row.getAs[String]("worker_id") // 3
          val hotel_name = JavaHBaseUtils.getValue("v2_rp_tb_hotel", hotel_id, "info", "name")
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
          val list = new util.ArrayList[Put]()
          list.add(put)
          JavaHBaseUtils.putRows(JavaRealLeaderProperties.MVTBSMPLANNERINDICATOROLD, list)
        } catch {
          case e: Exception => println("老数据写入HBase失败. ")
        }
      })
    })

  }


  def main(args: Array[String]): Unit = {
    val start = System.currentTimeMillis()
    readAndWriteRealLeaderOldData2HBase()
    val stop = System.currentTimeMillis()

    println("用时总计: " + (stop - start) + " ms.")
  }

}
