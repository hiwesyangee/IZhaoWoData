package com.realleaderAndyanjing.engine

import com.izhaowo.cores.utils.{JavaHBaseUtils, JavaSparkUtils}
import com.realleader.properties.JavaRealLeaderProperties
import com.yanjing.engine.SomeUtils

object Test2 {
  def main(args: Array[String]): Unit = {

    var i = 1
    while ( {
      i <= 5
    }) {
      var j = i + 1
      while ( {
        j <= 5
      }) {
        System.out.print(" ")

        {
          j += 1; j - 1
        }
      }
      var z = 1
      while ( {
        z <= i
      }) {
        if (i == 4 && z == 2) System.out.print(107.toChar + " ")
        else if (i == 4 && z == 3) System.out.print(98.toChar + " ")
        else System.out.print("*" + " ")

        {
          z += 1; z - 1
        }
      }
      System.out.print("\n")

      {
        i += 1; i - 1
      }
    }
  }

  // 210
  def check1(): Unit = {
    val df = RealLeaderAndYanJingEngine.getYanJingOriginalDataFromMySQL

    val needDf = df.filter(row => {
      val sec = row.getAs[Long]("section")
      sec == 4
    }).filter(row => {
      val worker_id = row.getAs[String]("worker_id")
      val distance = JavaHBaseUtils.getValue("v2_rp_workers_hotel_distance", "e812e6b3-d881-45e2-b5b3-e168bca74ff7" + ":" + worker_id, "info", "distance")
      val distanceKM = (distance.toFloat / 1000)
      distanceKM <= 20
    })


    println("count =" + needDf.count())
  }

  // 47
  def check2(): Unit = {
    val spark = JavaSparkUtils.getInstance().getSparkSession

    import spark.implicits._

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

    // todo 从这一步开始，就会出现对section进行限定的查询
    val allNeedDf = spark.sql(s"select bbb.worker_id,ddd.section,bbb.case_dot,bbb.reorder_rate,bbb.communication_level,bbb.design_sense,bbb.case_rate,bbb.all_score_final,bbb.number,bbb.to_store_rate,bbb.text_rating_rate,ddd.display_amount from bbb left join ddd on bbb.worker_id = ddd.worker_id where ddd.section = 4").distinct()

    // 获取过滤后距离和预算区间的数据。
    val needDf = allNeedDf.filter(row => {
      val worker_id = row.getAs[String]("worker_id")
      val distance = JavaHBaseUtils.getValue("v2_rp_workers_hotel_distance", "e812e6b3-d881-45e2-b5b3-e168bca74ff7" + ":" + worker_id, "info", "distance")
      val distanceKM = (distance.toFloat / 1000)
      distanceKM <= 20
    })

    println("count =" + needDf.count())
  }

  def check3(): Map[Double, Int] = {
    val otherInterval: List[Double] = SomeUtils.getSplitIntervalSetting(0, 5000, 100, "[]")
    otherInterval.foreach(println)


    val inList: List[Double] = List(1750d, 1850d, 1150d)

    var map = Map[Double, Int]()
    for (inData: Double <- otherInterval) {
      map = map.+(inData -> 0)
    }
    for (data: Double <- inList) {
      val realData = data - (data % 50)
      println("realData = " + realData)
      val oldValue: Int = map.get(realData).get
      map = map.+(realData -> (oldValue + 1))
    }
    map
  }

}
