package com.yanjing.foruse.wangjaingbinguan

import java.util

import com.izhaowo.cores.utils._
import com.yanjing.foruse.DataValidation01Util
import com.yanjing.foruse.DataValidation01Util.LegalPlanner
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, Logger}

/**
  * 针对望江宾馆酒店，进行已预订策划师和可预订策划师的统计。
  * by hiwes 2019/09/26
  */
object DataValidation014WJBG {
  val spark = JavaSparkUtils.getInstance().getSparkSession

  import spark.implicits._

  /**
    * 主函数。
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hbase").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.kafka").setLevel(Level.WARN)

    // 调用获取最终预约时间的函数
    //    val maxTime = getLastChooseDay4Hotel("09f8e3bd-0053-44c0-85ff-3939690ecb09")
    //    println(maxTime) // 20200503

    val arr: util.List[String] = MyUtils.checkTime("20190926", "20200503")
    println(arr)
    val map: util.LinkedHashMap[String, Int] = getDoneNumberPlanner("09f8e3bd-0053-44c0-85ff-3939690ecb09", arr)
    println("已预订策划师相关数据===" + map)
    println("已预订策划师相关数据===" + map.size()) // 11个，其中13场婚礼。
    val map2: util.LinkedHashMap[String, Int] = getCanNumberPlanner("望江宾馆", "09f8e3bd-0053-44c0-85ff-3939690ecb09", 4, arr)
    println("可预订策划师相关数据===" + map2)
    println("可预订策划师相关数据===" + map2.size())
  }

  /**
    * 根据酒店ID，获取最后一天的预约时间
    *
    * @param hotel_id
    */
  def getLastChooseDay4Hotel(hotel_id: String): String = {
    var maxTime = 20190925l
    try {
      val result = JavaHBaseUtils.getScanner("v2_rp_tb_user_wedding")
      var res = result.next()
      while (res != null) {
        val hotelID = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("hotel_id")))
        if (hotelID != null && !hotelID.isEmpty && hotelID.length > 0 && !hotelID.equals("")) {
          if (hotelID.equals(hotel_id)) {
            val weddingDate = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("wedding_date")))
            if (weddingDate.length >= 10) {
              println(weddingDate)
              val inTime = (weddingDate.substring(0, 10).replaceAll("-", "")).toLong
              if (inTime > maxTime) {
                maxTime = inTime
              }
            }
          }
        }
        res = result.next()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
    return maxTime.toString
  }

  /**
    * 获取已经策划师数量数据
    *
    * @param hotel_id
    * @param arr
    * @return
    */
  def getDoneNumberPlanner(hotel_id: String, arr: util.List[String]): util.LinkedHashMap[String, Int] = {
    val map = new util.LinkedHashMap[String, Int]()
    try {
      val result = JavaHBaseUtils.getScanner("v2_rp_tb_user_wedding")
      var res = result.next()
      while (res != null) {
        val hotelID = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("hotel_id")))
        if (hotelID != null && !hotelID.isEmpty && hotelID.length > 0 && !hotelID.equals("")) {
          if (hotelID.equals(hotel_id)) {
            val userID = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("user_id")))
            val weddingDate = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("wedding_date")))
            if (weddingDate.length >= 10) {
              val inTime = (weddingDate.substring(0, 10).replaceAll("-", ""))
              val key = inTime + "=" + userID
              if (arr.contains(inTime)) {
                if (map.get(key) != null) {
                  map.put(key, map.get(key.toString) + 1)
                } else {
                  map.put(key, 1)
                }
              }
            }
          }
        }
        res = result.next()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
    return map
  }

  /**
    * 获取针对酒店的已定婚礼次数。
    *
    * @param hotel_id
    * @return
    */
  def getPlannerNumber4Done(hotel_id: String): Int = {
    var size = 0
    try {
      val result = JavaHBaseUtils.getScanner("v2_rp_tb_user_wedding")
      var res = result.next()
      while (res != null) {
        val hotelID = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("hotel_id")))
        if (hotelID != null && !hotelID.isEmpty && hotelID.length > 0 && !hotelID.equals("")) {
          if (hotelID.equals(hotel_id)) {
            val weddingDate = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("wedding_date")))
            if (weddingDate.length >= 10) {
              val inTime = (weddingDate.substring(0, 10).replaceAll("-", "")).toLong
              if (inTime >= 20190926) {
                size = size + 1
              }
            }
          }
        }
        res = result.next()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
    size
  }

  /**
    * 获取既定模型的可预订策划师数量数据
    *
    * @param hotel_id
    * @param section
    */
  def getCanNumberPlanner(hotel_name: String, hotel_id: String, section: Int, days: util.List[String]): util.LinkedHashMap[String, Int] = {
    val map = new util.LinkedHashMap[String, Int]()
    try {
      val tbLowBudgetHotel1 = spark.read.format("jdbc")
        .option("url", "jdbc:mysql://115.29.45.77:3306/izhaowo_worker")
        .option("dbtable", "tb_low_budget_hotel1")
        .option("user", "test_izw_2019")
        .option("password", "izw(1234)!#$%^[abcABC]9587")
        .load()
      tbLowBudgetHotel1.createOrReplaceTempView("lbg1")
      val hh1 = spark.sql(s"select * from lbg1 where hotel = '$hotel_name' and section = $section")
      hh1.createOrReplaceTempView("hhh")

      val tbLowBudgetTypeDF = spark.read.format("jdbc")
        .option("url", "jdbc:mysql://115.29.45.77:3306/izhaowo_worker")
        .option("dbtable", "tb_low_budget_type")
        .option("user", "test_izw_2019")
        .option("password", "izw(1234)!#$%^[abcABC]9587")
        .load()
        .withColumnRenamed("到店签单率", "to_store_rate")
        .withColumnRenamed("策划师文字评价率", "text_rating_rate")

      tbLowBudgetTypeDF.createOrReplaceTempView("bbb")

      val dfTotalDF = spark.read.format("jdbc")
        .option("url", "jdbc:mysql://115.29.45.77:3306/izhaowo_worker")
        .option("dbtable", "df_total")
        .option("user", "test_izw_2019")
        .option("password", "izw(1234)!#$%^[abcABC]9587")
        .load()
      dfTotalDF.createOrReplaceTempView("ddd")
      val allNeedDf = spark.sql(s"select bbb.worker_id,ddd.section,bbb.case_dot,bbb.reorder_rate,bbb.communication_level,bbb.design_sense,bbb.case_rate,bbb.all_score_final,bbb.number,bbb.to_store_rate,bbb.text_rating_rate,ddd.display_amount from bbb left join ddd on bbb.worker_id = ddd.worker_id where ddd.section = $section").distinct()
      allNeedDf.createOrReplaceTempView("aaa")

      // TODO. 获取51条策划师的所有数据。
      val resultDf = spark.sql(s"select * from hhh left join aaa on hhh.worker_id = aaa.worker_id")
      //    resultDf.show(3, false)

      val hahaDf = resultDf.filter(row => {
        val distance = row.getAs[String]("distance")
        val distanceKM = (distance.toFloat / 1000)
        distanceKM <= 20f
      })

      val arr = hahaDf.collect()
      println("符合距离条件要求的策划师数量为: " + arr.size)

      var set = Set[LegalPlanner]()
      var allSet = Set[LegalPlanner]()
      for (row <- arr) {
        val worker_id = row.getAs[String]("worker_id")
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

        // 组合对象。
        val lp = LegalPlanner(worker_id, case_dot, reorder_rate, communication_level, design_sense, case_rate,
          all_score_final, number, text_rating_rate, display_amount, to_store_rate)

        // 返回新对象。
        val newLP = DataValidation01Util.legalPlannerFilter(lp)
        if (newLP != null) {
          set = set.+(newLP)
        }
        allSet = allSet.+(lp)
      }
      println("1.符合距离要求的策划师数据===allSet:" + allSet)
      println("allSet size:" + allSet.size)
      println("2.符合约束条件的策划师数据===size:" + set)
      println("set size:" + set.size)
      println("他们分别是: ")
      set.foreach(u => {
        println(u)
      })
      println("===========================")

      try {
        val result = days.iterator()
        var day = result.next()
        while (day != null) {
          val lastPlannerSet: Set[LegalPlanner] = DataValidation01Util.noSchedulePlannerFilter(set, day)
          map.put(day, lastPlannerSet.size)
          day = result.next()
        }
      } catch {
        case e: Exception => "迭代错误，忽略"
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
    map
  }
}
