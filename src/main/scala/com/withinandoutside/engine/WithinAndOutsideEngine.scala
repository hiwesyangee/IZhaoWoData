package com.withinandoutside.engine

import java.util

import com.izhaowo.cores.utils.{JavaHBaseUtils, JavaSparkUtils}
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.MaxAbsScaler
import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.DataFrame

/**
 * 更新域内域外市场指标-engine端,逻辑实现类
 *
 * by hiwes since 2020/11/16
 */
object WithinAndOutsideEngine {

  Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
  Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN)
  Logger.getLogger("org.apache.hbase").setLevel(Level.WARN)
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.apache.kafka").setLevel(Level.WARN)

  val spark = JavaSparkUtils.getInstance().getSparkSession

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val long = System.currentTimeMillis()
    startSaveWithinAndOutsideIndicatorsAll
    saveAllWeight2HBase

    val stop = System.currentTimeMillis()
    println("time = " + (stop - long) / 1000 + " s.")
  }

  /**
   * 读取原始大表
   */
  def startSaveWithinAndOutsideIndicatorsAll(): Unit = {
    val izhaowo_dispatch = "izhaowo_dispatch"
    val izhaowo_data_house = "izhaowo_data_house"

    val planner_hotel_distance_DF = spark.read.format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", s"jdbc:mysql://47.104.171.235:3306/$izhaowo_dispatch?useUnicode=true&characterEncoding=UTF8")
      .option("dbtable", "planner_hotel_distance")
      .option("user", "izhaowo")
      .option("password", "izhaowo@123")
      .option("numPartitions", 20)
      .option("partitionColumn", "distance")
      .option("lowerBound", 5000)
      .option("upperBound", 5500000)
      .load()
    planner_hotel_distance_DF.createOrReplaceTempView("planner_hotel_distance")

    val planner_wedding_info_DF = spark.read.format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", s"jdbc:mysql://47.104.171.235:3306/$izhaowo_data_house?useUnicode=true&characterEncoding=UTF8")
      .option("dbtable", "planner_wedding_info")
      .option("user", "izhaowo")
      .option("password", "izhaowo@123")
      .option("numPartitions", 10)
      .option("partitionColumn", "回单数")
      .option("lowerBound", 10)
      .option("upperBound", 120)
      .load()
      .withColumnRenamed("策划师名称", "planner_name")
      .withColumnRenamed("策划师所在省", "planner_province")
      //      .withColumnRenamed("首付前满意度评分", "down_payment_before_satisfaction_1")
      .withColumnRenamed("首付前满意度评分", "A1")
      //      .withColumnRenamed("首付前出方案速度评分", "down_payment_before_solution_speed_2")
      .withColumnRenamed("首付前出方案速度评分", "A2")
      //      .withColumnRenamed("尾款前整体打分", "balance_payment_before_overall_score_3")
      .withColumnRenamed("尾款前整体打分", "A3")
      //      .withColumnRenamed("尾款前服务意识分数", "balance_payment_before_service_consciousness_4")
      .withColumnRenamed("尾款前服务意识分数", "A4")
      //      .withColumnRenamed("尾款前审美能力分数", "balance_payment_before_aesthetic_ability_5")
      .withColumnRenamed("尾款前审美能力分数", "A5")
      //      .withColumnRenamed("尾款前效果还原度分数", "balance_payment_before_degree_of_reduction_6")
      .withColumnRenamed("尾款前效果还原度分数", "A6")
      //      .withColumnRenamed("尾款前控制预算分数", "balance_payment_before_control_budget_7")
      .withColumnRenamed("尾款前控制预算分数", "A7")
      //      .withColumnRenamed("尾款前形象气质分数", "balance_payment_before_image_temperament_8")
      .withColumnRenamed("尾款前形象气质分数", "A8")
      .withColumnRenamed("策划师id", "planner_id")
      .withColumnRenamed("预算区间分类", "section")
      .withColumnRenamed("付款阶段", "pay_stage")
      .withColumnRenamed("服务类型", "service_type")
    planner_wedding_info_DF.createOrReplaceTempView("planner_wedding_info")

    // todo 1.调用20to50
    startSaveWithinAndOutsideIndicators20to50

    // todo 2.调用50to100
    startSaveWithinAndOutsideIndicators50to100()

    // todo 3.调用100to150
    startSaveWithinAndOutsideIndicators100to150()

    // todo 4.调用150tomore
    startSaveWithinAndOutsideIndicators150tomore()
  }

  case class hotelCase(planner_id: String, hotel_id: String, distance: Int)

  /**
   * 直接进行每日定时任务，读取并存储域内市场指标
   */
  def startSaveWithinAndOutsideIndicators20to50(): Unit = {
    val hotel = spark.sql("select planner_id,hotel_id,distance from planner_hotel_distance where distance < 50000 and distance >= 20000")
      .sortWithinPartitions("distance")
      .dropDuplicates("hotel_id")
    hotel.createOrReplaceTempView("hotel_20to50")

    val end = spark.sql("SELECT hotel_20to50.hotel_id, planner_wedding_info.section, planner_wedding_info.planner_id, planner_wedding_info.planner_province,planner_wedding_info.A1,planner_wedding_info.A2,planner_wedding_info.A3,planner_wedding_info.A4,planner_wedding_info.A5,planner_wedding_info.A6,planner_wedding_info.A7,planner_wedding_info.A8 FROM hotel_20to50 left join planner_wedding_info on hotel_20to50.hotel_id = planner_wedding_info.hotel_id where planner_wedding_info.planner_status = 0 and planner_wedding_info.pay_stage = '尾款' and planner_wedding_info.service_type = '宴会设计'")
      .na.fill(0.0d, Seq("A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8"))

    val result = end.map(row => {
      val hotel_id = row.getAs[String]("hotel_id")
      val section = row.getAs[Double]("section").toInt
      val planner_id = row.getAs[String]("planner_id")
      val planner_province = row.getAs[String]("planner_province")
      val A1 = row.getAs[Double]("A1")
      val A2 = row.getAs[Double]("A2")
      val A3 = row.getAs[Double]("A3")
      val A4 = row.getAs[Double]("A4")
      val A5 = row.getAs[Double]("A5")
      val A6 = row.getAs[Double]("A6")
      val A7 = row.getAs[Double]("A7")
      val A8 = row.getAs[Double]("A8")
      (hotel_id, section, planner_id, planner_province, A1, A2, A3, A4, A5, A6, A7, A8, Vectors.dense(A1 * 2 - 5, A2 * 2 - 5, A3 * 2 - 5, A4 * 2 - 5, A5 * 2 - 5, A6 * 2 - 5, A7 * 2 - 5, A8 * 2 - 5))
    }).toDF("hotel_id", "section", "planner_id", "planner_province", "A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "features")

    // hotel_id   planner_id   planner_province   A1————A8
    // todo 按同一个hotel_id下的所有评分，求该策划师的归一化结果。
    val scaler = new MaxAbsScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")

    val scalerModel = scaler.fit(result)

    val scaledData = scalerModel.transform(result)

    // 获取最终需要的归一化结果。
    val yourneed = scaledData.select("hotel_id", "section", "planner_id", "planner_province", "A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "features", "scaledFeatures")

    val thelast = yourneed.map(row => {
      val hotel_id = row.getAs[String]("hotel_id")
      val section = row.getAs[Int]("section").toString
      val planner_id = row.getAs[String]("planner_id")
      val planner_province = row.getAs[String]("planner_province")
      val features = row.getAs[linalg.Vector]("features").toArray
      val labelArr = row.getAs[linalg.Vector]("scaledFeatures").toArray
      (hotel_id, section, planner_id, planner_province, features, labelArr, labelArr(0), labelArr(1), labelArr(2), labelArr(3), labelArr(4), labelArr(5), labelArr(6), labelArr(7))
    }).toDF("hotel_id", "section", "planner_id", "planner_province", "features", "scaledFeatures", "a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8")
      .select("hotel_id", "section", "planner_id", "planner_province", "a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8")
    // todo 注意，这里的cection里面，存在section=6的情况

    val ttype = "20to50"
    thelast.foreachPartition(ite => {
      val list = new java.util.ArrayList[Put]()
      ite.foreach(row => {
        val hotel_id = row.getAs[String]("hotel_id")
        val section = row.getAs[String]("section")
        val planner_id = row.getAs[String]("planner_id")
        val planner_province = row.getAs[String]("planner_province")
        val A1 = row.getAs[Double]("a1").toString
        val A2 = row.getAs[Double]("a2").toString
        val A3 = row.getAs[Double]("a3").toString
        val A4 = row.getAs[Double]("a4").toString
        val A5 = row.getAs[Double]("a5").toString
        val A6 = row.getAs[Double]("a6").toString
        val A7 = row.getAs[Double]("a7").toString
        val A8 = row.getAs[Double]("a8").toString

        val rowkey = ttype + "=" + hotel_id + "=" + section + "=" + planner_id
        val put = new Put(Bytes.toBytes(rowkey))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("type"), Bytes.toBytes(ttype))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("hotel_id"), Bytes.toBytes(hotel_id))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("section"), Bytes.toBytes(section))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("planner_id"), Bytes.toBytes(planner_id))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("planner_province"), Bytes.toBytes(planner_province))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A1"), Bytes.toBytes(A1))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A2"), Bytes.toBytes(A2))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A3"), Bytes.toBytes(A3))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A4"), Bytes.toBytes(A4))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A5"), Bytes.toBytes(A5))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A6"), Bytes.toBytes(A6))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A7"), Bytes.toBytes(A7))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A8"), Bytes.toBytes(A8))
        list.add(put)
      })
      JavaHBaseUtils.putRows("wanwan_planner_indicators", list)
    })
  }

  /**
   * 直接进行每日定时任务，读取并存储准域内市场指标
   */
  def startSaveWithinAndOutsideIndicators50to100(): Unit = {
    val hotel: DataFrame = spark.sql("select planner_id,hotel_id,distance from planner_hotel_distance where distance < 100000 and distance >= 50000")
      .sortWithinPartitions("distance")
      .dropDuplicates("hotel_id")
    hotel.createOrReplaceTempView("hotel_50to100")

    val end = spark.sql("SELECT hotel_50to100.hotel_id, planner_wedding_info.section, planner_wedding_info.planner_id, planner_wedding_info.planner_province,planner_wedding_info.A1,planner_wedding_info.A2,planner_wedding_info.A3,planner_wedding_info.A4,planner_wedding_info.A5,planner_wedding_info.A6,planner_wedding_info.A7,planner_wedding_info.A8 FROM hotel_50to100 left join planner_wedding_info on hotel_50to100.hotel_id = planner_wedding_info.hotel_id where planner_wedding_info.planner_status = 0 and planner_wedding_info.pay_stage = '尾款' and planner_wedding_info.service_type = '宴会设计'")
      .na.fill(0.0d, Seq("A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8"))

    val result = end.map(row => {
      val hotel_id = row.getAs[String]("hotel_id")
      val section = row.getAs[Double]("section").toInt
      val planner_id = row.getAs[String]("planner_id")
      val planner_province = row.getAs[String]("planner_province")
      val A1 = row.getAs[Double]("A1")
      val A2 = row.getAs[Double]("A2")
      val A3 = row.getAs[Double]("A3")
      val A4 = row.getAs[Double]("A4")
      val A5 = row.getAs[Double]("A5")
      val A6 = row.getAs[Double]("A6")
      val A7 = row.getAs[Double]("A7")
      val A8 = row.getAs[Double]("A8")
      (hotel_id, section, planner_id, planner_province, A1, A2, A3, A4, A5, A6, A7, A8, Vectors.dense(A1 * 2 - 5, A2 * 2 - 5, A3 * 2 - 5, A4 * 2 - 5, A5 * 2 - 5, A6 * 2 - 5, A7 * 2 - 5, A8 * 2 - 5))
    }).toDF("hotel_id", "section", "planner_id", "planner_province", "A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "features")

    // hotel_id   planner_id   planner_province   A1————A8
    // todo 按同一个hotel_id下的所有评分，求该策划师的归一化结果。
    val scaler = new MaxAbsScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")

    val scalerModel = scaler.fit(result)

    val scaledData = scalerModel.transform(result)

    // 获取最终需要的归一化结果。
    val yourneed = scaledData.select("hotel_id", "section", "planner_id", "planner_province", "A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "features", "scaledFeatures")

    val thelast = yourneed.map(row => {
      val hotel_id = row.getAs[String]("hotel_id")
      val section = row.getAs[Int]("section").toString
      val planner_id = row.getAs[String]("planner_id")
      val planner_province = row.getAs[String]("planner_province")
      val features = row.getAs[linalg.Vector]("features").toArray
      val labelArr = row.getAs[linalg.Vector]("scaledFeatures").toArray
      (hotel_id, section, planner_id, planner_province, features, labelArr, labelArr(0), labelArr(1), labelArr(2), labelArr(3), labelArr(4), labelArr(5), labelArr(6), labelArr(7))
    }).toDF("hotel_id", "section", "planner_id", "planner_province", "features", "scaledFeatures", "a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8")
      .select("hotel_id", "section", "planner_id", "planner_province", "a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8")
    // todo 注意，这里的cection里面，存在section=6的情况

    val ttype = "50to100"
    thelast.foreachPartition(ite => {
      val list = new java.util.ArrayList[Put]()
      ite.foreach(row => {
        val hotel_id = row.getAs[String]("hotel_id")
        val section = row.getAs[Int]("section").toString
        val planner_id = row.getAs[String]("planner_id")
        val planner_province = row.getAs[String]("planner_province")
        val A1 = row.getAs[Double]("a1").toString
        val A2 = row.getAs[Double]("a2").toString
        val A3 = row.getAs[Double]("a3").toString
        val A4 = row.getAs[Double]("a4").toString
        val A5 = row.getAs[Double]("a5").toString
        val A6 = row.getAs[Double]("a6").toString
        val A7 = row.getAs[Double]("a7").toString
        val A8 = row.getAs[Double]("a8").toString

        val rowkey = ttype + "=" + hotel_id + "=" + section + "=" + planner_id
        val put = new Put(Bytes.toBytes(rowkey))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("type"), Bytes.toBytes(ttype))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("hotel_id"), Bytes.toBytes(hotel_id))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("section"), Bytes.toBytes(section))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("planner_id"), Bytes.toBytes(planner_id))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("planner_province"), Bytes.toBytes(planner_province))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A1"), Bytes.toBytes(A1))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A2"), Bytes.toBytes(A2))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A3"), Bytes.toBytes(A3))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A4"), Bytes.toBytes(A4))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A5"), Bytes.toBytes(A5))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A6"), Bytes.toBytes(A6))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A7"), Bytes.toBytes(A7))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A8"), Bytes.toBytes(A8))
        list.add(put)
      })
      JavaHBaseUtils.putRows("wanwan_planner_indicators", list)
    })

  }

  /**
   * 直接进行每日定时任务，读取并存储域外市场指标
   */
  def startSaveWithinAndOutsideIndicators100to150(): Unit = {
    val hotel: DataFrame = spark.sql("select planner_id,hotel_id,distance from planner_hotel_distance where distance < 150000 and distance >= 100000")
      .sortWithinPartitions("distance")
      .dropDuplicates("hotel_id")
    hotel.createOrReplaceTempView("hotel_100to150")

    val end = spark.sql("SELECT hotel_100to150.hotel_id, planner_wedding_info.section, planner_wedding_info.planner_id, planner_wedding_info.planner_province,planner_wedding_info.A1,planner_wedding_info.A2,planner_wedding_info.A3,planner_wedding_info.A4,planner_wedding_info.A5,planner_wedding_info.A6,planner_wedding_info.A7,planner_wedding_info.A8 FROM hotel_100to150 left join planner_wedding_info on hotel_100to150.hotel_id = planner_wedding_info.hotel_id where planner_wedding_info.planner_status = 0 and planner_wedding_info.pay_stage = '尾款' and planner_wedding_info.service_type = '宴会设计'")
      .na.fill(0.0d, Seq("A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8"))

    val result = end.map(row => {
      val hotel_id = row.getAs[String]("hotel_id")
      val section = row.getAs[Double]("section").toInt
      val planner_id = row.getAs[String]("planner_id")
      val planner_province = row.getAs[String]("planner_province")
      val A1 = row.getAs[Double]("A1")
      val A2 = row.getAs[Double]("A2")
      val A3 = row.getAs[Double]("A3")
      val A4 = row.getAs[Double]("A4")
      val A5 = row.getAs[Double]("A5")
      val A6 = row.getAs[Double]("A6")
      val A7 = row.getAs[Double]("A7")
      val A8 = row.getAs[Double]("A8")
      (hotel_id, section, planner_id, planner_province, A1, A2, A3, A4, A5, A6, A7, A8, Vectors.dense(A1 * 2 - 5, A2 * 2 - 5, A3 * 2 - 5, A4 * 2 - 5, A5 * 2 - 5, A6 * 2 - 5, A7 * 2 - 5, A8 * 2 - 5))
    }).toDF("hotel_id", "section", "planner_id", "planner_province", "A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "features")

    // hotel_id   planner_id   planner_province   A1————A8
    // todo 按同一个hotel_id下的所有评分，求该策划师的归一化结果。
    val scaler = new MaxAbsScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")

    val scalerModel = scaler.fit(result)

    val scaledData = scalerModel.transform(result)

    // 获取最终需要的归一化结果。
    val yourneed = scaledData.select("hotel_id", "section", "planner_id", "planner_province", "A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "features", "scaledFeatures")

    val thelast = yourneed.map(row => {
      val hotel_id = row.getAs[String]("hotel_id")
      val section = row.getAs[Int]("section").toString
      val planner_id = row.getAs[String]("planner_id")
      val planner_province = row.getAs[String]("planner_province")
      val features = row.getAs[linalg.Vector]("features").toArray
      val labelArr = row.getAs[linalg.Vector]("scaledFeatures").toArray
      (hotel_id, section, planner_id, planner_province, features, labelArr, labelArr(0), labelArr(1), labelArr(2), labelArr(3), labelArr(4), labelArr(5), labelArr(6), labelArr(7))
    }).toDF("hotel_id", "section", "planner_id", "planner_province", "features", "scaledFeatures", "a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8")
      .select("hotel_id", "section", "planner_id", "planner_province", "a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8")
    // todo 注意，这里的cection里面，存在section=6的情况

    val ttype = "100to150"
    thelast.foreachPartition(ite => {
      val list = new java.util.ArrayList[Put]()
      ite.foreach(row => {
        val hotel_id = row.getAs[String]("hotel_id")
        val section = row.getAs[Int]("section").toString
        val planner_id = row.getAs[String]("planner_id")
        val planner_province = row.getAs[String]("planner_province")
        val A1 = row.getAs[Double]("a1").toString
        val A2 = row.getAs[Double]("a2").toString
        val A3 = row.getAs[Double]("a3").toString
        val A4 = row.getAs[Double]("a4").toString
        val A5 = row.getAs[Double]("a5").toString
        val A6 = row.getAs[Double]("a6").toString
        val A7 = row.getAs[Double]("a7").toString
        val A8 = row.getAs[Double]("a8").toString

        val rowkey = ttype + "=" + hotel_id + "=" + section + "=" + planner_id
        val put = new Put(Bytes.toBytes(rowkey))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("type"), Bytes.toBytes(ttype))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("hotel_id"), Bytes.toBytes(hotel_id))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("section"), Bytes.toBytes(section))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("planner_id"), Bytes.toBytes(planner_id))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("planner_province"), Bytes.toBytes(planner_province))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A1"), Bytes.toBytes(A1))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A2"), Bytes.toBytes(A2))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A3"), Bytes.toBytes(A3))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A4"), Bytes.toBytes(A4))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A5"), Bytes.toBytes(A5))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A6"), Bytes.toBytes(A6))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A7"), Bytes.toBytes(A7))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A8"), Bytes.toBytes(A8))
        list.add(put)
      })
      JavaHBaseUtils.putRows("wanwan_planner_indicators", list)
      //      JavaHBaseUtils.putRows("test", list)
    })
  }

  /**
   * 直接进行每日定时任务，读取并存储SUP市场指标
   */
  def startSaveWithinAndOutsideIndicators150tomore(): Unit = {
    val hotel: DataFrame = spark.sql("select planner_id,hotel_id,distance from planner_hotel_distance where distance >= 150000")
      .sortWithinPartitions("distance")
      .dropDuplicates(Seq("hotel_id")) // 这一条，有他吗44w条数据，然后distinct下来的2000多条。
    hotel.createOrReplaceTempView("hotel_150tomore")

    val end = spark.sql("SELECT hotel_150tomore.hotel_id, planner_wedding_info.section, planner_wedding_info.planner_id, planner_wedding_info.planner_province,planner_wedding_info.A1,planner_wedding_info.A2,planner_wedding_info.A3,planner_wedding_info.A4,planner_wedding_info.A5,planner_wedding_info.A6,planner_wedding_info.A7,planner_wedding_info.A8 FROM hotel_150tomore left join planner_wedding_info on hotel_150tomore.hotel_id = planner_wedding_info.hotel_id where planner_wedding_info.planner_status = 0 and planner_wedding_info.pay_stage = '尾款' and planner_wedding_info.service_type = '宴会设计'")
      .na.fill(0.0d, Seq("A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8"))

    val result = end.map(row => {
      val hotel_id = row.getAs[String]("hotel_id")
      val section = row.getAs[Double]("section").toInt
      val planner_id = row.getAs[String]("planner_id")
      val planner_province = row.getAs[String]("planner_province")
      val A1 = row.getAs[Double]("A1")
      val A2 = row.getAs[Double]("A2")
      val A3 = row.getAs[Double]("A3")
      val A4 = row.getAs[Double]("A4")
      val A5 = row.getAs[Double]("A5")
      val A6 = row.getAs[Double]("A6")
      val A7 = row.getAs[Double]("A7")
      val A8 = row.getAs[Double]("A8")
      (hotel_id, section, planner_id, planner_province, A1, A2, A3, A4, A5, A6, A7, A8, Vectors.dense(A1 * 2 - 5, A2 * 2 - 5, A3 * 2 - 5, A4 * 2 - 5, A5 * 2 - 5, A6 * 2 - 5, A7 * 2 - 5, A8 * 2 - 5))
    }).toDF("hotel_id", "section", "planner_id", "planner_province", "A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "features")

    // hotel_id   planner_id   planner_province   A1————A8
    // todo 按同一个hotel_id下的所有评分，求该策划师的归一化结果。
    val scaler = new MaxAbsScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")

    val scalerModel = scaler.fit(result)

    val scaledData = scalerModel.transform(result)

    // 获取最终需要的归一化结果。
    val yourneed = scaledData.select("hotel_id", "section", "planner_id", "planner_province", "A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "features", "scaledFeatures")

    val thelast = yourneed.map(row => {
      val hotel_id = row.getAs[String]("hotel_id")
      val section = row.getAs[Int]("section").toString
      val planner_id = row.getAs[String]("planner_id")
      val planner_province = row.getAs[String]("planner_province")
      val features = row.getAs[linalg.Vector]("features").toArray
      val labelArr = row.getAs[linalg.Vector]("scaledFeatures").toArray
      (hotel_id, section, planner_id, planner_province, features, labelArr, labelArr(0), labelArr(1), labelArr(2), labelArr(3), labelArr(4), labelArr(5), labelArr(6), labelArr(7))
    }).toDF("hotel_id", "section", "planner_id", "planner_province", "features", "scaledFeatures", "a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8")
      .select("hotel_id", "section", "planner_id", "planner_province", "a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8")
    // todo 注意，这里的cection里面，存在section=6的情况

    val ttype = "150tomore"
    thelast.foreachPartition(ite => {
      val list = new java.util.ArrayList[Put]()
      ite.foreach(row => {
        val hotel_id = row.getAs[String]("hotel_id")
        val section = row.getAs[Int]("section").toString
        val planner_id = row.getAs[String]("planner_id")
        val planner_province = row.getAs[String]("planner_province")
        val A1 = row.getAs[Double]("a1").toString
        val A2 = row.getAs[Double]("a2").toString
        val A3 = row.getAs[Double]("a3").toString
        val A4 = row.getAs[Double]("a4").toString
        val A5 = row.getAs[Double]("a5").toString
        val A6 = row.getAs[Double]("a6").toString
        val A7 = row.getAs[Double]("a7").toString
        val A8 = row.getAs[Double]("a8").toString

        val rowkey = ttype + "=" + hotel_id + "=" + section + "=" + planner_id
        val put = new Put(Bytes.toBytes(rowkey))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("type"), Bytes.toBytes(ttype))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("hotel_id"), Bytes.toBytes(hotel_id))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("section"), Bytes.toBytes(section))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("planner_id"), Bytes.toBytes(planner_id))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("planner_province"), Bytes.toBytes(planner_province))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A1"), Bytes.toBytes(A1))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A2"), Bytes.toBytes(A2))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A3"), Bytes.toBytes(A3))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A4"), Bytes.toBytes(A4))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A5"), Bytes.toBytes(A5))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A6"), Bytes.toBytes(A6))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A7"), Bytes.toBytes(A7))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A8"), Bytes.toBytes(A8))
        list.add(put)
      })
      //      JavaHBaseUtils.putRows("wanwan_planner_indicators_150_more", list)
      JavaHBaseUtils.putRows("wanwan_planner_indicators", list)
    })

  }

  /**
   * 存储所有权重
   */
  def saveAllWeight2HBase(): Unit = {
    // A1 首付前满意度评分
    // A2 首付前出方案速度评分
    // A3 尾款前整体打分
    // A4 尾款前服务意识分数
    // A5 尾款前审美能力分数
    // A6 尾款前效果还原度分数
    // A7 尾款前控制预算分数
    // A8 尾款前形象气质分数

    // todo 1] 20to50下，section1-5的指标权重，按A1——A8
    val section1_20to50Map: Map[Int, Double] = Map[Int, Double](1 -> 0.0986, 2 -> 0.1083, 3 -> 0.15, 4 -> 0.1791, 5 -> 0.1029, 6 -> 0.139, 7 -> 0.0814, 8 -> 0.1408)
    val section2_20to50Map = Map[Int, Double](1 -> 0.0975, 2 -> 0.0965, 3 -> 0.15, 4 -> 0.2669, 5 -> 0.0971, 6 -> 0.0969, 7 -> 0.0972, 8 -> 0.0979)
    val section3_20to50Map = Map[Int, Double](1 -> 0.1045, 2 -> 0.1045, 3 -> 0.15, 4 -> 0.1341, 5 -> 0.1341, 6 -> 0.1341, 7 -> 0.1045, 8 -> 0.1341)
    val section4_20to50Map = Map[Int, Double](1 -> 0.1255, 2 -> 0.1255, 3 -> 0.15, 4 -> 0.1184, 5 -> 0.1219, 6 -> 0.1219, 7 -> 0.1184, 8 -> 0.1184)
    val section5_20to50Map = Map[Int, Double](1 -> 0.1214, 2 -> 0.1214, 3 -> 0.15, 4 -> 0.1214, 5 -> 0.1214, 6 -> 0.1214, 7 -> 0.1214, 8 -> 0.1214)

    // todo 2] 50to100下，section1-5的指标权重，按A1——A8
    val section1_50to100Map = Map[Int, Double](1 -> 0.0736, 2 -> 0.1124, 3 -> 0.15, 4 -> 0.1202, 5 -> 0.1106, 6 -> 0.126, 7 -> 0.1946, 8 -> 0.1125)
    val section2_50to100Map = Map[Int, Double](1 -> 0.1024, 2 -> 0.1024, 3 -> 0.15, 4 -> 0.1024, 5 -> 0.1689, 6 -> 0.1689, 7 -> 0.1024, 8 -> 0.1024)
    val section3_50to100Map = Map[Int, Double](1 -> 0.0986, 2 -> 0.0986, 3 -> 0.15, 4 -> 0.0951, 5 -> 0.1004, 6 -> 0.1004, 7 -> 0.0986, 8 -> 0.2584)
    val section4_50to100Map = Map[Int, Double](1 -> 0.1024, 2 -> 0.1024, 3 -> 0.15, 4 -> 0.1024, 5 -> 0.1689, 6 -> 0.1024, 7 -> 0.1024, 8 -> 0.1689)

    // todo 3] 100to150下section1-3的指标权重，按A1——A8，且按四川、重庆、江西分类
    val section1_100to150_sichuanMap = Map[Int, Double](1 -> 0.0846, 2 -> 0.0927, 3 -> 0.15, 4 -> 0.1333, 5 -> 0.1333, 6 -> 0.0927, 7 -> 0.1217, 8 -> 0.1918)
    val section2_100to150_sichuanMap = Map[Int, Double](1 -> 0.096, 2 -> 0.1128, 3 -> 0.15, 4 -> 0.1647, 5 -> 0.1191, 6 -> 0.1191, 7 -> 0.1191, 8 -> 0.1191)
    val section3_100to150_sichuanMap = Map[Int, Double](1 -> 0.1045, 2 -> 0.1045, 3 -> 0.15, 4 -> 0.1045, 5 -> 0.1341, 6 -> 0.1341, 7 -> 0.1341, 8 -> 0.1341)
    val section1_100to150_chongqingMap = Map[Int, Double](1 -> 0.1214, 2 -> 0.1214, 3 -> 0.15, 4 -> 0.1214, 5 -> 0.1214, 6 -> 0.1214, 7 -> 0.1214, 8 -> 0.1214)
    val section2_100to150_chongqingMap = Map[Int, Double](1 -> 0.08, 2 -> 0.1027, 3 -> 0.15, 4 -> 0.1027, 5 -> 0.1318, 6 -> 0.1318, 7 -> 0.1318, 8 -> 0.1693)
    val section3_100to150_chongqingMap = Map[Int, Double](1 -> 0.098, 2 -> 0.1091, 3 -> 0.15, 4 -> 0.1374, 5 -> 0.1374, 6 -> 0.1091, 7 -> 0.1215, 8 -> 0.1374)
    val section1_100to150_jiangxiMap = Map[Int, Double](1 -> 0.1214, 2 -> 0.1214, 3 -> 0.15, 4 -> 0.1214, 5 -> 0.1214, 6 -> 0.1214, 7 -> 0.1214, 8 -> 0.1214)
    val section2_100to150_jiangxiMap = Map[Int, Double](1 -> 0.1024, 2 -> 0.1024, 3 -> 0.15, 4 -> 0.1024, 5 -> 0.1024, 6 -> 0.1689, 7 -> 0.1689, 8 -> 0.1024)
    val section3_100to150_jiangxiMap = Map[Int, Double](1 -> 0.1214, 2 -> 0.1214, 3 -> 0.15, 4 -> 0.1214, 5 -> 0.1214, 6 -> 0.1214, 7 -> 0.1214, 8 -> 0.1214)

    // todo 4] 150tomore下，section1-3的指标权重，按A1——A8，且按四川、重庆、江西分类
    val section1_150tomore_sichuanMap = Map[Int, Double](1 -> 0.1214, 2 -> 0.1214, 3 -> 0.15, 4 -> 0.1214, 5 -> 0.1214, 6 -> 0.1214, 7 -> 0.1214, 8 -> 0.1214)
    val section2_150tomore_sichuanMap = Map[Int, Double](1 -> 0.1449, 2 -> 0.1038, 3 -> 0.15, 4 -> 0.1038, 5 -> 0.1038, 6 -> 0.1449, 7 -> 0.1449, 8 -> 0.1038)
    val section3_150tomore_sichuanMap = Map[Int, Double](1 -> 0.1051, 2 -> 0.1242, 3 -> 0.15, 4 -> 0.1242, 5 -> 0.1242, 6 -> 0.1242, 7 -> 0.1242, 8 -> 0.1242)
    val section1_150tomore_chongqingMap = Map[Int, Double](1 -> 0.1214, 2 -> 0.1214, 3 -> 0.15, 4 -> 0.1214, 5 -> 0.1214, 6 -> 0.1214, 7 -> 0.1214, 8 -> 0.1214)
    val section2_150tomore_chongqingMap = Map[Int, Double](1 -> 0.1048, 2 -> 0.1048, 3 -> 0.15, 4 -> 0.1281, 5 -> 0.1281, 6 -> 0.1281, 7 -> 0.1281, 8 -> 0.1281)
    val section3_150tomore_chongqingMap = Map[Int, Double](1 -> 0.1051, 2 -> 0.1242, 3 -> 0.15, 4 -> 0.1242, 5 -> 0.1242, 6 -> 0.1242, 7 -> 0.1242, 8 -> 0.1242)
    val section1_150tomore_jiangxiMap = Map[Int, Double](1 -> 0.1214, 2 -> 0.1214, 3 -> 0.15, 4 -> 0.1214, 5 -> 0.1214, 6 -> 0.1214, 7 -> 0.1214, 8 -> 0.1214)
    val section2_150tomore_jiangxiMap = Map[Int, Double](1 -> 0.1048, 2 -> 0.1048, 3 -> 0.15, 4 -> 0.1281, 5 -> 0.1281, 6 -> 0.1281, 7 -> 0.1281, 8 -> 0.1281)
    val section3_150tomore_jiangxiMap = Map[Int, Double](1 -> 0.1051, 2 -> 0.1242, 3 -> 0.15, 4 -> 0.1242, 5 -> 0.1242, 6 -> 0.1242, 7 -> 0.1242, 8 -> 0.1242)


    val data = spark.createDataFrame(Seq(
      ("section1_20to50", 0.0986, 0.1083, 0.15, 0.1791, 0.1029, 0.139, 0.0814, 0.1408, getSortResultByMap(section1_20to50Map)),
      ("section2_20to50", 0.0975, 0.0965, 0.15, 0.2669, 0.0971, 0.0969, 0.0972, 0.0979, getSortResultByMap(section2_20to50Map)),
      ("section3_20to50", 0.1045, 0.1045, 0.15, 0.1341, 0.1341, 0.1341, 0.1045, 0.1341, getSortResultByMap(section3_20to50Map)),
      ("section4_20to50", 0.1255, 0.1255, 0.15, 0.1184, 0.1219, 0.1219, 0.1184, 0.1184, getSortResultByMap(section4_20to50Map)),
      ("section5_20to50", 0.1214, 0.1214, 0.15, 0.1214, 0.1214, 0.1214, 0.1214, 0.1214, getSortResultByMap(section5_20to50Map)),

      ("section1_50to100", 0.0736, 0.1124, 0.15, 0.1202, 0.1106, 0.126, 0.1946, 0.1125, getSortResultByMap(section1_50to100Map)),
      ("section2_50to100", 0.1024, 0.1024, 0.15, 0.1024, 0.1689, 0.1689, 0.1024, 0.1024, getSortResultByMap(section2_50to100Map)),
      ("section3_50to100", 0.0986, 0.0986, 0.15, 0.0951, 0.1004, 0.1004, 0.0986, 0.2584, getSortResultByMap(section3_50to100Map)),
      ("section4_50to100", 0.1024, 0.1024, 0.15, 0.1024, 0.1689, 0.1024, 0.1024, 0.1689, getSortResultByMap(section4_50to100Map)),

      // todo 3] 100to150下section1-3的指标权重，按A1——A8，且按四川、重庆、江西分类
      ("section1_100to150_sichuan", 0.0846, 0.0927, 0.15, 0.1333, 0.1333, 0.0927, 0.1217, 0.1918, getSortResultByMap(section1_100to150_sichuanMap)),
      ("section2_100to150_sichuan", 0.096, 0.1128, 0.15, 0.1647, 0.1191, 0.1191, 0.1191, 0.1191, getSortResultByMap(section2_100to150_sichuanMap)),
      ("section3_100to150_sichuan", 0.1045, 0.1045, 0.15, 0.1045, 0.1341, 0.1341, 0.1341, 0.1341, getSortResultByMap(section3_100to150_sichuanMap)),

      ("section1_100to150_chongqing", 0.1214, 0.1214, 0.15, 0.1214, 0.1214, 0.1214, 0.1214, 0.1214, getSortResultByMap(section1_100to150_chongqingMap)),
      ("section2_100to150_chongqing", 0.08, 0.1027, 0.15, 0.1027, 0.1318, 0.1318, 0.1318, 0.1693, getSortResultByMap(section2_100to150_chongqingMap)),
      ("section3_100to150_chongqing", 0.098, 0.1091, 0.15, 0.1374, 0.1374, 0.1091, 0.1215, 0.1374, getSortResultByMap(section3_100to150_chongqingMap)),
      ("section1_100to150_jiangxi", 0.1214, 0.1214, 0.15, 0.1214, 0.1214, 0.1214, 0.1214, 0.1214, getSortResultByMap(section1_100to150_jiangxiMap)),
      ("section2_100to150_jiangxi", 0.1024, 0.1024, 0.15, 0.1024, 0.1024, 0.1689, 0.1689, 0.1024, getSortResultByMap(section2_100to150_jiangxiMap)),
      ("section3_100to150_jiangxi", 0.1214, 0.1214, 0.15, 0.1214, 0.1214, 0.1214, 0.1214, 0.1214, getSortResultByMap(section3_100to150_jiangxiMap)),

      // todo 4] 150tomore下，section1-3的指标权重，按A1——A8，且按四川、重庆、江西分类
      ("section1_150tomore_sichuan", 0.1214, 0.1214, 0.15, 0.1214, 0.1214, 0.1214, 0.1214, 0.1214, getSortResultByMap(section1_150tomore_sichuanMap)),
      ("section2_150tomore_sichuan", 0.1449, 0.1038, 0.15, 0.1038, 0.1038, 0.1449, 0.1449, 0.1038, getSortResultByMap(section2_150tomore_sichuanMap)),
      ("section3_150tomore_sichuan", 0.1051, 0.1242, 0.15, 0.1242, 0.1242, 0.1242, 0.1242, 0.1242, getSortResultByMap(section3_150tomore_sichuanMap)),
      ("section1_150tomore_chongqing", 0.1214, 0.1214, 0.15, 0.1214, 0.1214, 0.1214, 0.1214, 0.1214, getSortResultByMap(section1_150tomore_chongqingMap)),
      ("section2_150tomore_chongqing", 0.1048, 0.1048, 0.15, 0.1281, 0.1281, 0.1281, 0.1281, 0.1281, getSortResultByMap(section2_150tomore_chongqingMap)),
      ("section3_150tomore_chongqing", 0.1051, 0.1242, 0.15, 0.1242, 0.1242, 0.1242, 0.1242, 0.1242, getSortResultByMap(section3_150tomore_chongqingMap)),
      ("section1_150tomore_jiangxi", 0.1214, 0.1214, 0.15, 0.1214, 0.1214, 0.1214, 0.1214, 0.1214, getSortResultByMap(section1_150tomore_jiangxiMap)),
      ("section2_150tomore_jiangxi", 0.1048, 0.1048, 0.15, 0.1281, 0.1281, 0.1281, 0.1281, 0.1281, getSortResultByMap(section2_150tomore_jiangxiMap)),
      ("section3_150tomore_jiangxi", 0.1051, 0.1242, 0.15, 0.1242, 0.1242, 0.1242, 0.1242, 0.1242, getSortResultByMap(section3_150tomore_jiangxiMap))

    )).toDF("rowkey", "A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "order")

    data.foreachPartition(ite => {
      val list = new java.util.ArrayList[Put]()
      ite.foreach(row => {
        val A1 = row.getAs[Double]("A1").toString
        val A2 = row.getAs[Double]("A2").toString
        val A3 = row.getAs[Double]("A3").toString
        val A4 = row.getAs[Double]("A4").toString
        val A5 = row.getAs[Double]("A5").toString
        val A6 = row.getAs[Double]("A6").toString
        val A7 = row.getAs[Double]("A7").toString
        val A8 = row.getAs[Double]("A8").toString
        val order = row.getAs[String]("order")
        val rowkey = row.getAs[String]("rowkey")
        val put = new Put(Bytes.toBytes(rowkey))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A1"), Bytes.toBytes(A1))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A2"), Bytes.toBytes(A2))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A3"), Bytes.toBytes(A3))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A4"), Bytes.toBytes(A4))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A5"), Bytes.toBytes(A5))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A6"), Bytes.toBytes(A6))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A7"), Bytes.toBytes(A7))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A8"), Bytes.toBytes(A8))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("order"), Bytes.toBytes(order))
        list.add(put)
      })
      JavaHBaseUtils.putRows("wanwan_planner_indicators_all_weight", list)
    })
  }

  /**
   * 通过(Int,Int)类型Map获取排序结果。
   *
   * @param map
   * @return
   */
  def getSortResultByMap(map: Map[Int, Double]): String = {
    val end: Seq[(Int, Double)] = map.toSeq.sortWith(_._2 > _._2)
    var testStr = ""
    end.foreach(map => {
      val id = map._1
      testStr += id + ","
    })
    val result = testStr.substring(0, testStr.length - 1)
    result
  }

  /**
   * 开始并存储当天弯弯策划师推荐结果。
   */
  def startAndSaveMCTSProcessResult(): Unit = {
    try {
      // Web参数：ttype域内外类型，section预算区间，weddate婚期
      /**
       * 也就是说，在最后查的时候，可以直接通过ttype=section查询到
       */
      // 首先，读取全量指标数据表
      var list = List[WanWanPlannerIndicators]()

      val result: util.Iterator[Result] = JavaHBaseUtils.getScanner("wanwan_planner_indicators").iterator()
      var res = result.next()
      while (res != null) {
        val ttype = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("type")))
        val section = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("section"))).toInt
        val province = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("planner_province")))
        val hotel_id = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("hotel_id")))
        val planner_id = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("planner_id")))
        val A1 = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("A1"))).toDouble
        val A2 = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("A2"))).toDouble
        val A3 = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("A3"))).toDouble
        val A4 = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("A4"))).toDouble
        val A5 = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("A5"))).toDouble
        val A6 = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("A6"))).toDouble
        val A7 = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("A7"))).toDouble
        val A8 = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("A8"))).toDouble
        list = list.++(List[WanWanPlannerIndicators](WanWanPlannerIndicators(ttype, section, province, hotel_id, planner_id, A1, A2, A3, A4, A5, A6, A7, A8)))
        res = result.next()
      }

      /**
       * "type", "section", "province", "hotel_id", "planner_id", "A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8"
       */
      val allIndicatorsDF: DataFrame = spark.createDataFrame(list).toDF("type", "section", "province", "hotel_id", "planner_id", "A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8")
        .cache()




      // 然后，读取指标权重数据表
      // 之后，对类型、预算区间、省份进行区分
      // 继续，对酒店进行区分
      // 最后，通过传递指标和权重，得到该类型+预算区间+酒店下对应的推荐结果，写入HBase。

      // 套入最终的方法，其实只需要详细指标+权重顺序，
      // 档期判断，放在

      allIndicatorsDF.unpersist()

    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  case class WanWanPlannerIndicators(ttype: String, section: Int, province: String, hotel_id: String, planner_id: String, A1: Double, A2: Double, A3: Double, A4: Double, A5: Double, A6: Double, A7: Double, A8: Double)

  /**
   * 处理20to50的MCTS数据，包含section1——5
   *
   * @param allIndicatorsDF
   */
  def startAndSaveMCTSProcessResult20to50(allIndicatorsDF: DataFrame): Unit = {
    // todo 1.处理section1下数据。
    val section1_20to50DF = allIndicatorsDF.filter(row => {
      row.getAs[String]("type") == "20to50" && row.getAs[Int]("section") == 1
    })
    val section1_20to50_order: String = JavaHBaseUtils.getValue("wanwan_planner_indicators_all_weight", "section1_20to50", "info", "order")


  }

  /**
   * 处理50to100的MCTS数据，包含section1——5
   *
   * @param allIndicatorsDF
   */
  def startAndSaveMCTSProcessResult50to100(allIndicatorsDF: DataFrame): Unit = {

  }

  /**
   * 处理100to150的MCTS数据，包含section1——5
   *
   * @param allIndicatorsDF
   */
  def startAndSaveMCTSProcessResult100to150(allIndicatorsDF: DataFrame): Unit = {

  }

  /**
   * 处理150tomore的MCTS数据，包含section1——5
   *
   * @param allIndicatorsDF
   */
  def startAndSaveMCTSProcessResult150tomore(allIndicatorsDF: DataFrame): Unit = {

  }

}
