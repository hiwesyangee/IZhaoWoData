package com.statusCheck.engine

import com.izhaowo.cores.utils.{JavaHBaseUtils, JavaMongoDBUtils, JavaSparkUtils, MyUtils}
import com.realleaderAndyanjing.engine.RealLeaderAndYanJingEngine.eyesData
import com.yanjing.engine.YanjingEngine.PlannerIndicator
import com.yanjing.foruse.model4AllHotel.Model4AllHotel.LegalPlanner4Model
import org.apache.hadoop.hbase.client.ResultScanner
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.bson.Document

object StatusCheckEngine {
  val spark = JavaSparkUtils.getInstance().getSparkSession

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hbase").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.kafka").setLevel(Level.WARN)

    val start = System.currentTimeMillis()
    smallMarketStatusChange
    val stop = System.currentTimeMillis()
    println("用时: " + (stop - start) / 1000 + "s.")
  }

  /**
    * 进行小市场状态修改
    */
  def smallMarketStatusChange(): Unit = {
    try {
      // todo 1.读取MongoDB小市场列表，并存储昨天的每日状态status2
      val vec = readStatus2DataFromMongoDB

      var allInfoVector = Vector[SmallMarketStatus]()
      val result = JavaHBaseUtils.getScanner("v2_rp_tb_hotel")
      var res = result.next()
      while (res != null) {
        val amap_id = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("amap_id")))
        val hotel_id = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("id")))
        for (sm <- vec) {
          if (sm.amapID.equals(amap_id) && hotel_id.length == 36) {
            allInfoVector = allInfoVector ++ Vector[SmallMarketStatus](SmallMarketStatus(hotel_id, sm.amapID, sm.section, sm.status, sm.status2))
          }
        }
        res = result.next()
      }

      // todo 2.读取策划师指标表old，进行缓存
      val indicatorsDF = readIndicatorsDataFromHBase.cache()

      // todo 3.根据每个小市场，过滤出自己的DF，并进行均衡验证
      for (infoVec <- allInfoVector) { // 遍历每个小市场信息——包含，hotel_id，amapID，section，status，status2
        val ownOriginDf = indicatorsDF.filter(row => {
          (row.getAs[String]("hotel_id").equals(infoVec.hotel_id)) &&
            (row.getAs[Int]("section").equals(infoVec.section))
        })

        // todo 2.进行距离过滤
        val distanceFilterDf: DataFrame = filterByDistance(ownOriginDf)

        // todo 3.进行档期过滤,并缓存这个经过了距离和档期过滤的DF【DF001】
        val scheduleFilterDf: DataFrame = filterBySchedule(distanceFilterDf) // 90次，档期查询。

        // todo 4.读取小市场原始模板
//        val smo: smallMarketTemplate = getSpecifiedTemplate(smallMarketId)

        // todo 5.进行约束条件过滤
//        val constraintFilterSet: Set[LegalPlanner4Model] = filterByConstraintNew(scheduleFilterDf, smo)

      }

      // todo 4.修改每日状态status2和昨日状态status
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  case class SmallMarketStatus(hotel_id: String, amapID: String, section: Int, status: Double, status2: Double) // status在第一次初始化的时候必定等于status2，存储昨日状态，之后修改status2位今日状态
  // 小市场模型模板对象
  case class smallMarketTemplate(a: Array[Double], b: Array[Double], a_equal: Array[Int], b_equal: Array[Int])

  // 1.读取MongoDB小市场列表，并存储昨天的每日状态status2
  def readStatus2DataFromMongoDB(): Vector[SmallMarketStatus] = {
    var vector = Vector[SmallMarketStatus]()

    try {
      val mondoDatabase = JavaMongoDBUtils.getInstance().getMongoDB
      val collection = mondoDatabase.getCollection("small_market_production")
      val bson = new Document
      bson.put("_id", 1)
      bson.put("status2", 1)
      val mongoCursor = collection.find().projection(bson).iterator
      while (mongoCursor.hasNext()) {
        val a: Document = mongoCursor.next();
        val smID = regJson(scala.util.parsing.json.JSON.parseFull(a.toJson)).get("_id").getOrElse().toString
        val amapID = smID.split("_")(0)
        val section = smID.split("_")(1).toInt
        val status2 = regJson(scala.util.parsing.json.JSON.parseFull(a.toJson)).get("status2").getOrElse().toString.toDouble
        vector = vector ++ Vector[SmallMarketStatus](SmallMarketStatus(null, amapID, section, status2, status2))
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
    vector
  }

  // 通过匹配，获取Json
  def regJson(json: Option[Any]) = json match {
    //转换类型
    case Some(map: collection.immutable.Map[String, Any]) => map
  }

  // 2.读取策划师指标表old，进行缓存
  def readIndicatorsDataFromHBase(): DataFrame = {
    val result: ResultScanner = JavaHBaseUtils.getScanner("mv_tb_small_market_planner_indicator_old")

    var vector = Vector[PlannerIndicator]()
    var res = result.next()
    while (res != null) {
      val hotel_id = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("hotel_id")))
      val section = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("section"))).toInt
      val worker_id = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("worker_id")))
      val hotel_name = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("hotel_name")))
      val sort = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("sort"))).toDouble
      val distance = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("distance"))).toLong
      val case_dot = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("case_dot"))).toDouble
      val reorder_rate = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("reorder_rate"))).toDouble
      val communication_level = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("communication_level"))).toDouble
      val design_sense = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("design_sense"))).toDouble
      val case_rate = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("case_rate"))).toDouble
      val all_score_final = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("all_score_final"))).toDouble
      val number = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("number"))).toDouble
      val text_rating_rate = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("text_rating_rate"))).toDouble
      val display_amount = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("display_amount"))).toDouble
      val to_store_rate = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("to_store_rate"))).toDouble
      vector = vector ++ Vector[PlannerIndicator](new PlannerIndicator(hotel_id, section, worker_id, hotel_name, sort, distance, case_dot, reorder_rate, communication_level, design_sense, case_rate, all_score_final, number, text_rating_rate, display_amount, to_store_rate))
      res = result.next()
    }
    vector.toDF()
  }

  def filterByDistance(resultDf: DataFrame): DataFrame = {
    val hahaDf = resultDf.filter(row => {
      val distance = row.getAs[Long]("distance")
      val distanceKM = (distance.toFloat / 1000)
      distanceKM <= 20f
    })
    hahaDf
  }

  def filterBySchedule(hahaDf: DataFrame): DataFrame = {
    val resultDf = hahaDf.filter(row => {
      val planner_id = row.getAs[String]("worker_id")
      // todo 获取策划师档期查询结果
      planner_id == null
    })
    resultDf
  }

}
