package com.yanjing.foruse

import com.izhaowo.cores.utils.{JavaHBaseUtils, JavaSparkUtils}
import com.yanjing.foruse.TotalDataValidationUtil.LegalPlanner
import org.apache.log4j.{Level, Logger}

object TotalDataValidation {

  val spark = JavaSparkUtils.getInstance().getSparkSession

  // 进行数据验证。
  def doDataValidation(hotel_id: String, hotel_name: String, section: Int): Set[LegalPlanner] = {

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
      val newLP = TotalDataValidationUtil.legalPlannerFilter(lp)
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
    val lastPlannerSet: Set[LegalPlanner] = TotalDataValidationUtil.noSchedulePlannerFilter(set, "20191012")
    println("3.符合档期要求的策划师数据===lastPlannerSet: " + lastPlannerSet)
    println(lastPlannerSet.size)
    println("他们分别是: ")
    lastPlannerSet.foreach(u => {
      println(u)
      val name = JavaHBaseUtils.getValue("v2_rp_tb_worker", u.worker_id, "info", "name")
      println("name = " + name)
    })
    println("===========================")
    lastPlannerSet
  }


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hbase").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.kafka").setLevel(Level.WARN)

    val start = System.currentTimeMillis()
    val lastPlannerSet1 = doDataValidation("0ded163e-92b8-4e58-be14-e2e26ffc4a4a", "大蓉和(紫荆店)", 5)
    val lastPlannerSet2 = doDataValidation("80a69eb8-0ebb-4e15-8dd7-f6689a6d2210", "天府阳光酒店", 5)
    val lastPlannerSet3 = doDataValidation("0023419d-8933-47dc-9663-b954dd0ea0b1", "蓉城盛宴酒楼", 5)
    val lastPlannerSet4 = doDataValidation("0afd6839-3c64-4851-a4f4-808ccc5163fa", "华亭宴会中心(皇冠假日酒店二楼)", 5)
    val lastPlannerSet5 = doDataValidation("3a1baa7f-17f7-4a66-946a-2bf0061c1f20", "红杏酒家(羊西店)", 5)
    println("===========================")
    println("1.一号酒店符合数据筛选要求的策划师数据===lastPlannerSet: " + lastPlannerSet1)
    println(lastPlannerSet1.size)
    println("他们分别是: ")
    lastPlannerSet1.foreach(u => {
      println(u)
      val name = JavaHBaseUtils.getValue("v2_rp_tb_worker", u.worker_id, "info", "name")
      println("name = " + name)
    })
    println("===========================")
    println("2.二号酒店符合数据筛选要求的策划师数据===lastPlannerSet: " + lastPlannerSet2)
    println(lastPlannerSet2.size)
    println("他们分别是: ")
    lastPlannerSet2.foreach(u => {
      println(u)
      val name = JavaHBaseUtils.getValue("v2_rp_tb_worker", u.worker_id, "info", "name")
      println("name = " + name)
    })
    println("===========================")
    println("3.三号酒店符合数据筛选要求的策划师数据===lastPlannerSet: " + lastPlannerSet3)
    println(lastPlannerSet3.size)
    println("他们分别是: ")
    lastPlannerSet3.foreach(u => {
      println(u)
      val name = JavaHBaseUtils.getValue("v2_rp_tb_worker", u.worker_id, "info", "name")
      println("name = " + name)
    })
    println("===========================")
    println("4.四号酒店符合数据筛选要求的策划师数据===lastPlannerSet: " + lastPlannerSet4)
    println(lastPlannerSet4.size)
    println("他们分别是: ")
    lastPlannerSet4.foreach(u => {
      println(u)
      val name = JavaHBaseUtils.getValue("v2_rp_tb_worker", u.worker_id, "info", "name")
      println("name = " + name)
    })
    println("===========================")
    println("5.五号酒店符合数据筛选要求的策划师数据===lastPlannerSet: " + lastPlannerSet5)
    println(lastPlannerSet5.size)
    println("他们分别是: ")
    lastPlannerSet5.foreach(u => {
      println(u)
      val name = JavaHBaseUtils.getValue("v2_rp_tb_worker", u.worker_id, "info", "name")
      println("name = " + name)
    })
    println("===========================")

    val stop = System.currentTimeMillis()
    println("all use time: " + (stop - start) / 1000 + "s.")
  }
}
