package com.yanjing.foruse

import com.izhaowo.cores.utils.{JavaHBaseUtils, JavaSparkUtils}
import com.yanjing.foruse.DataValidation01Util.LegalPlanner
import org.apache.log4j.{Level, Logger}

/**
  * 数据验证，策略源于2019.09.17肖老师给定策略。【望江宾馆-4】
  */
object DataValidation01 {

  val spark = JavaSparkUtils.getInstance().getSparkSession

  import spark.implicits._

  // 进行数据验证。
  def doDataValidation(hotel_id: String, hotel_name: String, section: Int): Unit = {

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
      //      val newLP = DataValidation01Util.legalPlannerFilter(lp)
      val newLP = DataValidation01Util.legalPlannerFilter84(lp)
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

    // 查询档期数据。20190917,20190918,20190919,20190920
    val lastPlannerSet: Set[LegalPlanner] = DataValidation01Util.noSchedulePlannerFilter(set, "20191024")
    println("3.符合档期要求的策划师数据===lastPlannerSet: " + lastPlannerSet)
    println(lastPlannerSet.size)
    println("他们分别是: ")
    lastPlannerSet.foreach(u => {
      println(u)
      //      val name = JavaHBaseUtils.getValue("v2_rp_tb_worker", u.worker_id, "info", "name")
      //      println("name = " + name)
    })
    println("===========================")

    //    val data4Original = DataValidation01Util.getTenDataByStatistical(lastPlannerSet)
    //    val data4Contraction = DataValidation01Util.getTenDataByStatistical2(lastPlannerSet)
    //    val data4Integral = DataValidation01Util.getTenDataByStatistical3(lastPlannerSet)
    //    val data4Predict = DataValidationUtils.getPredictNumberFromPythonInterface("31", "", "")
    val data4Predict = DataValidationUtils.getPredictNumberFromPythonInterface("84", "", "")

    //    println(data4Original)
    //    println(data4Contraction)
    //    println(data4Integral)
    println(data4Predict) // 打印预测数量。
  }


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hbase").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.kafka").setLevel(Level.WARN)

    val start = System.currentTimeMillis()
    doDataValidation("09f8e3bd-0053-44c0-85ff-3939690ecb09", "望江宾馆", 4)
    val stop = System.currentTimeMillis()
    println("all use time: " + (stop - start) / 1000 + "s.")
  }

}
