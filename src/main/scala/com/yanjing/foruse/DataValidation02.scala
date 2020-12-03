package com.yanjing.foruse

import com.izhaowo.cores.utils.JavaSparkUtils
import com.yanjing.foruse.DataValidation01.spark
import org.apache.log4j.{Level, Logger}

object DataValidation02 {
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

    // TODO. 获取55条策划师的所有数据。
    val resultDf = spark.sql(s"select * from hhh left join aaa on hhh.worker_id = aaa.worker_id")
    val hahaDf = resultDf.filter(row => {
      val distance = row.getAs[String]("distance")
      val distanceKM = (distance.toFloat / 1000)
      distanceKM <= 20f
    })

    val arr = hahaDf.collect()
    println("过滤限定区间之前的策划师数量问：" + arr.size)

    var list1 = List[Double]()
    var list4 = List[Double]()
    var list7 = List[Double]()
    var list9 = List[Double]()
    for (row <- arr) {
      val case_dot = row.getAs[Double]("case_dot")
      val design_sense = row.getAs[Double]("design_sense")
      val number = row.getAs[Double]("number")
      val display_amount = row.getAs[Double]("display_amount")

      list1 = list1.::(case_dot)
      list4 = list4.::(design_sense)
      list7 = list7.::(number)
      list9 = list9.::(display_amount)
    }

    println("list1====" + list1)
    println("list4====" + list4)
    println("list7====" + list7)
    println("list9====" + list9)
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
