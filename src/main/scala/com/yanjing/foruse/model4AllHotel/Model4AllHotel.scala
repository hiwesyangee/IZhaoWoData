package com.yanjing.foruse.model4AllHotel

import com.izhaowo.cores.utils.{JavaDateUtils, JavaHBaseUtils, JavaSparkUtils}
import com.alibaba.fastjson.JSON
import com.yanjing.foruse.DataValidationUtils
import com.yanjing.foruse.DataValidationUtils.{postResponse, str_json5001}
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame

/**
  * 通过数据模型对所有给定的宾馆数据进行验证。
  */
object Model4AllHotel {
  val spark = JavaSparkUtils.getInstance().getSparkSession

  import spark.implicits._

  // case class : 合法策划师。
  case class LegalPlanner4Model(worker_id: String, case_dot: Double, reorder_rate: Double, communication_level: Double,
                                design_sense: Double, case_rate: Double, all_score_final: Double, number: Double,
                                text_rating_rate: Double, display_amount: Double, to_store_rate: Double)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hbase").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.kafka").setLevel(Level.WARN)

    val start = System.currentTimeMillis()
    // hotel_id,hotel_name,section,ID,[arrA],[arrB]
    // doDataValidation("09f8e3bd-0053-44c0-85ff-3939690ecb09", "望江宾馆", 4)
    doDataValidation(args(0), args(1), args(2).toInt, args(3), args(4), args(5))
    val stop = System.currentTimeMillis()
    println("all use time: " + (stop - start) / 1000 + "s.")
  }

  /**
    * 对模型进行数据验证。
    *
    * @param hotel_id   酒店id
    * @param hotel_name 酒店名字
    * @param section    预算区间
    * @param ID         接口id
    * @param arrA       数据下界
    * @param arrB       数据上界
    */
  def doDataValidation(hotel_id: String, hotel_name: String, section: Int, ID: String, arrA: String, arrB: String): Unit = {
    val resultDf = makeOriginalData(hotel_name, section)
    val hahaDf = filterByDistance(resultDf)
    val set = filterByConstraint(hahaDf, arrA, arrB)
    println(set.size)
    println(set)
    val lastSet = filterBySchedule(set, JavaDateUtils.getDateYMD.substring(0, 8))
    val integralResult = getInterfaceResult(ID)
    println("实际供给策划师数量: " + lastSet.size)
    println("打印详细数据如下: ")
    lastSet.foreach(u => {
      println(u.case_dot + "," + u.reorder_rate + "," + u.communication_level + "," + u.design_sense + "," + u.case_rate + "," + u.all_score_final + "," + u.number + "," + u.text_rating_rate + "," + u.display_amount + "," + u.to_store_rate)
    })
    println("数据模型积分计算结果: " + integralResult)
  }

  /**
    * 获取数据验证原始组装数据。
    *
    * @param hotel_name 酒店名字
    * @param section    预算区间
    * @return
    */
  def makeOriginalData(hotel_name: String, section: Int): DataFrame = {
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
    resultDf
  }

  /**
    * 1.进行距离限定。
    *
    * @param resultDf 初始组装DF
    * @return 距离限定后DF
    */
  def filterByDistance(resultDf: DataFrame): DataFrame = {
    val hahaDf = resultDf.filter(row => {
      val distance = row.getAs[String]("distance")
      val distanceKM = (distance.toFloat / 1000)
      distanceKM <= 20f
    })
    hahaDf
  }

  /**
    * 2.进行约束条件限定。
    *
    * @param hahaDf 距离限定后DF
    * @param arrA   积分模型数据下界
    * @param arrB   积分模型数据上界
    * @return 约束条件限定Set
    */
  def filterByConstraint(hahaDf: DataFrame, arrA: String, arrB: String): Set[LegalPlanner4Model] = {
    val arr4LowerBound = arrA.split(",") //  数据下界
    val arr4UpperBound = arrB.split(",") //  数据上界
    if (arr4LowerBound.size == 10 && arr4UpperBound.size == 10) {
      val arr = hahaDf.collect()
      var set = Set[LegalPlanner4Model]()
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
        val lp = LegalPlanner4Model(worker_id, case_dot, reorder_rate, communication_level, design_sense, case_rate,
          all_score_final, number, text_rating_rate, display_amount, to_store_rate)

        // 返回新对象。
        val newLP = legalPlannerFilter(lp, arr4LowerBound, arr4UpperBound)
        if (newLP != null) {
          set = set.+(newLP)
        }
      }
      set
    } else {
      throw new IllegalArgumentException("积分上下界数据异常,请进行检查。")
    }
  }

  /**
    * 3.进行档期条件限定
    *
    * @param set 约束条件限定Set
    * @param day 当天时间
    * @return
    */
  def filterBySchedule(set: Set[LegalPlanner4Model], day: String): Set[LegalPlanner4Model] = noSchedulePlannerFilter(set, day)

  def getInterfaceResult(id: String): String = DataValidationUtils.getPredictNumberFromPythonInterface(id, "", "")

  // 1.约束条件，进行合法策划师过滤
  def legalPlannerFilter(lp: LegalPlanner4Model, arrA: Array[String], ArrB: Array[String]): LegalPlanner4Model = {
    // var bool_res = true
    var bool_res = false
    if (lp.case_dot >= arrA(0).toDouble && lp.case_dot <= ArrB(0).toDouble) { // 1.number=16
      if (lp.reorder_rate >= arrA(1).toDouble && lp.reorder_rate <= ArrB(1).toDouble) { // 2.number=16
        if (lp.communication_level >= arrA(2).toDouble && lp.communication_level <= ArrB(2).toDouble) { // 3.number=13
          if (lp.design_sense >= arrA(3).toDouble && lp.design_sense <= ArrB(3).toDouble) { // 4.number=2
            if (lp.case_rate >= arrA(4).toDouble && lp.case_rate <= ArrB(4).toDouble) { // 5.number=1
              if (lp.all_score_final >= arrA(5).toDouble && lp.all_score_final <= ArrB(5).toDouble) { // 6.number=1
                if (lp.number >= arrA(6).toDouble && lp.number <= ArrB(6).toDouble) { // 7.number=1
                  if (lp.text_rating_rate >= arrA(7).toDouble && lp.text_rating_rate <= ArrB(7).toDouble) { // 8.number=1
                    if (lp.display_amount >= arrA(8).toDouble && lp.display_amount <= ArrB(8).toDouble) { // 9.number=0
                      if (lp.to_store_rate >= arrA(9).toDouble && lp.to_store_rate <= ArrB(9).toDouble) { // 10.number=0[1]
                        bool_res = true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (bool_res == true) {
      lp
    } else {
      null
    }
  }

  // 2.过滤无档期的策划师数据。【判断是否有档期，过滤Set】
  def noSchedulePlannerFilter(plannerSet: Set[LegalPlanner4Model], day: String): Set[LegalPlanner4Model] = {
    var set = Set[LegalPlanner4Model]()
    for (lp <- plannerSet) {
      if (hasSchedule4Planner(lp.worker_id, day: String)) {
        set = set.+(lp)
      }
    }
    set
  }

  // 2-1.过滤策划师使用有档期。【具体判断是否有档期，针对单个策划师】
  def hasSchedule4Planner(planner_id: String, day: String): Boolean = {
    val planner_name = getPlannerNameById(planner_id).split(" ")(0)
    if (planner_name != null) {
      val url = s"http://master:7979/FuzzyWorkers?workerName=$planner_name&weddate=$day"
      val end: String = getResponse(url)
      val arr = end.split(",")
      if (arr.length >= 8) {
        if ((arr(7).split(":")(1)).toInt > 0) {
          return true
        }
      }
    }
    // todo 发送http消息，通过调用接口获取到最近的策划师档期。
    return false
  }

  // 3.查询策划师昵称。【根据ID查询策划师name】
  def getPlannerNameById(planner_id: String): String = {
    val name = JavaHBaseUtils.getValue("v2_rp_tb_worker", planner_id, "info", "name")
    if (name != null) {
      name
    } else {
      null
    }
  }

  // 4.get方式直接获取数据
  def getResponse(url: String, header: String = null): String = {
    val httpClient = HttpClients.createDefault() // 创建 client 实例
    val get = new HttpGet(url) // 创建 get 实例

    if (header != null) { // 设置 header
      val json = JSON.parseObject(header)
      json.keySet().toArray.map(_.toString).foreach(key => get.setHeader(key, json.getString(key)))
    }

    val response = httpClient.execute(get) // 发送请求
    EntityUtils.toString(response.getEntity) // 获取返回结果
  }

  // 4.post方式直接获取数据
  def postResponse(url: String, params: String = null, header: String = null): String = {
    val httpClient = HttpClients.createDefault() // 创建 client 实例
    val post = new HttpPost(url) // 创建 post 实例

    // 设置 header
    if (header != null) {
      val json = JSON.parseObject(header)
      json.keySet().toArray.map(_.toString).foreach(key => post.setHeader(key, json.getString(key)))
    }

    if (params != null) {
      post.setEntity(new StringEntity(params, "UTF-8"))
    }

    val response = httpClient.execute(post) // 创建 client 实例
    EntityUtils.toString(response.getEntity, "UTF-8") // 获取返回结果
  }


}


