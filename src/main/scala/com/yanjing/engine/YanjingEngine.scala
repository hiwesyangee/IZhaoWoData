package com.yanjing.engine

import java.sql.{Statement, Timestamp}

import com.izhaowo.cores.utils.{JavaHBaseUtils, JavaSQLServerConn, JavaSparkUtils, MyUtils}
import com.realleader.engine.RealLeaderEngine.spark
import com.realleader.properties.JavaRealLeaderProperties
import org.apache.hadoop.hbase.client.ResultScanner
import org.apache.hadoop.hbase.util.Bytes

object YanjingEngine {

  val spark = JavaSparkUtils.getInstance().getSparkSession

  import spark.implicits._

  /**
    * 存储寇耀处的df_total表数据到hbase.
    * 12.20 修改为真正领导数据源2表
    */
  def saveDfTotalData(hotel_id: String, hotel_name: String, section: Int): Unit = {
    //    val tbLowBudgetTypeDF = spark.read.format("jdbc")
    //      .option("url", "jdbc:mysql://115.29.45.77:3306/izhaowo_worker")
    //      .option("dbtable", "tb_low_budget_type")
    //      .option("user", "test_izw_2019")
    //      .option("password", "izw(1234)!#$%^[abcABC]9587")
    //      .load()
    //      .withColumnRenamed("到店签单率", "to_store_rate")
    //      .withColumnRenamed("策划师文字评价率", "text_rating_rate")
    //
    //    tbLowBudgetTypeDF.createOrReplaceTempView("bbb")
    //
    //    val dfTotalDF = spark.read.format("jdbc")
    //      .option("url", "jdbc:mysql://115.29.45.77:3306/izhaowo_worker")
    //      .option("dbtable", "df_total")
    //      .option("user", "test_izw_2019")
    //      .option("password", "izw(1234)!#$%^[abcABC]9587")
    //      .load()
    //    dfTotalDF.createOrReplaceTempView("ddd")
    //
    //    // todo 从这一步开始，就会出现对section进行限定的查询
    //    val allNeedDf = spark.sql(s"select bbb.worker_id,ddd.section,bbb.case_dot,bbb.reorder_rate,bbb.communication_level,bbb.design_sense,bbb.case_rate,bbb.all_score_final,bbb.number,bbb.to_store_rate,bbb.text_rating_rate,ddd.display_amount from bbb left join ddd on bbb.worker_id = ddd.worker_id where ddd.section = 3").distinct()
    //
    //    // 获取过滤后距离和预算区间的数据。
    //    val needDf = allNeedDf.filter(row => {
    //      val worker_id = row.getAs[String]("worker_id")
    //      val distance = JavaHBaseUtils.getValue("v2_rp_workers_hotel_distance", hotel_id + ":" + worker_id, "info", "distance")
    //      val distanceKM = (distance.toFloat / 1000)
    //      distanceKM <= 20
    //    })
    //    val tbLowBudgetHotel2 = spark.read.format("jdbc")
    //      .option("url", "jdbc:mysql://115.29.45.77:3306/izhaowo_worker")
    //      .option("dbtable", "tb_low_budget_hotel2")
    //      .option("user", "test_izw_2019")
    //      .option("password", "izw(1234)!#$%^[abcABC]9587")
    //      .load()
    //    tbLowBudgetHotel2.createOrReplaceTempView("lbg2")
    //    val hh1 = spark.sql(s"select * from lbg2 where hotel = '$hotel_name' and section = $section")
    //    hh1.createOrReplaceTempView("hhh")
    //
    //    val realLeaderDF = spark.read.format("jdbc")
    //      .option("url", "jdbc:mysql://115.29.45.77:3306/izhaowo_worker")
    //      .option("dbtable", "真正领导数据源2")
    //      .option("user", "test_izw_2019")
    //      .option("password", "izw(1234)!#$%^[abcABC]9587")
    //      .load()
    //      .withColumnRenamed("到店签单率", "to_store_rate")
    //      .withColumnRenamed("assessment_rate", "text_rating_rate")
    //      .withColumnRenamed("服务费", "display_amount")
    //      .filter(row => {
    //        val dt = row.getAs[Timestamp]("ctime")
    //        import java.text.SimpleDateFormat
    //        val formatter = new SimpleDateFormat("yyyyMMdd")
    //        val dateString = formatter.format(dt)
    //        dateString == MyUtils.getFromToday(0)
    //      })
    //    realLeaderDF.createOrReplaceTempView("ddd")
    //
    //    val allNeedDf = spark.sql(s"select ddd.worker_id,ddd.section,ddd.case_dot,ddd.reorder_rate,ddd.communication_level,ddd.design_sense,ddd.case_rate,ddd.all_score_final,ddd.number,ddd.to_store_rate,ddd.text_rating_rate,ddd.display_amount,ddd.sort from ddd where ddd.section = $section")
    //    allNeedDf.createOrReplaceTempView("aaa")
    //
    //    val resultDf = spark.sql(s"select * from hhh left join aaa on hhh.worker_id = aaa.worker_id")

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

    val resultDf = spark.sql(s"select hotel_id,hhh.section,hhh.worker_id,hotel_name,rrr.sort,distance,case_dot,reorder_rate,communication_level,design_sense,case_rate,all_score_final,number,text_rating_rate,display_amount,to_store_rate from rrr left join hhh on rrr.worker_id = hhh.worker_id and rrr.section = hhh.section where hotel_name = '$hotel_name' and hhh.section = $section")

    val needDf = resultDf.filter(row => {
      val distance = row.getAs[Long]("distance")
      val distanceKM = (distance.toFloat / 1000)
      distanceKM <= 20f
    })

    needDf.cache()

    // 提供SQLServer连接
    val conn = JavaSQLServerConn.getConnection
    val st: Statement = conn.createStatement()

    // 1.todo A	方案点：								case_dot
    val caseDotList: List[Double] = needDf.select("case_dot").rdd.map(row => row.getAs[Double]("case_dot")).collect().toList
    val caseDotInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
    val caseDotMap = SomeUtils.getRightIntervalBySetting(caseDotList, caseDotInterval)
    //    caseDotMap.toArray.sortBy(_._1.toDouble).toMap  // 针对数值进行排序

    caseDotMap.foreach(a => {
      val case_dot = JavaSomeUtils.get2Double(a._1.toDouble)
      val num = a._2
      val other = "Vj ≠ 1"
      val sql = s"insert into new_canNum_v1_case_dot(hotel_id,hotel_name,section,case_dot,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$case_dot,'$other',$num,getdate());"
      st.executeUpdate(sql)
    })

    // 2.todo B 回单率：                reorder_rate
    val reorderRateList: List[Double] = needDf.select("reorder_rate").rdd.map(row => row.getAs[Double]("reorder_rate")).collect().toList
    val reorderRateInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
    val reorderRateMap = SomeUtils.getRightIntervalBySetting(reorderRateList, reorderRateInterval)

    reorderRateMap.foreach(a => {
      val case_dot = JavaSomeUtils.get2Double(a._1.toDouble)
      val num = a._2
      val other = "Vj ≠ 2"
      val sql = s"insert into new_canNum_v2_reorder_rate(hotel_id,hotel_name,section,reorder_rate,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$case_dot,'$other',$num,getdate());"
      st.executeUpdate(sql)
    })

    // 3.todo C 沟通水平：                communication_level
    val communicationLevelList: List[Double] = needDf.select("communication_level").rdd.map(row => row.getAs[Double]("communication_level")).collect().toList
    val communicationLevelInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
    val communicationLevelMap = SomeUtils.getRightIntervalBySetting(communicationLevelList, communicationLevelInterval)

    communicationLevelMap.foreach(a => {
      val communication_level = JavaSomeUtils.get2Double(a._1.toDouble)
      val num = a._2
      val other = "Vj ≠ 3"
      val sql = s"insert into new_canNum_v3_communication_level(hotel_id,hotel_name,section,communication_level,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$communication_level,'$other',$num,getdate());"
      st.executeUpdate(sql)
    })

    // 4.todo D 设计感：                design_sense
    val designSenseList: List[Double] = needDf.select("design_sense").rdd.map(row => row.getAs[Double]("design_sense")).collect().toList
    val designSenseInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
    val designSenseMap = SomeUtils.getRightIntervalBySetting(designSenseList, designSenseInterval)

    designSenseMap.foreach(a => {
      val design_sense = JavaSomeUtils.get2Double(a._1.toDouble)
      val num = a._2
      val other = "Vj ≠ 4"
      val sql = s"insert into new_canNum_v4_design_sense(hotel_id,hotel_name,section,design_sense,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$design_sense,'$other',$num,getdate());"
      st.executeUpdate(sql)
    })

    // 5.todo E 设计感：                case_rate
    val caseRateList: List[Double] = needDf.select("case_rate").rdd.map(row => row.getAs[Double]("case_rate")).collect().toList
    val caseRateInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
    val caseRateMap = SomeUtils.getRightIntervalBySetting(caseRateList, caseRateInterval)

    caseRateMap.foreach(a => {
      val case_rate = JavaSomeUtils.get2Double(a._1.toDouble)
      val num = a._2
      val other = "Vj ≠ 5"
      val sql = s"insert into new_canNum_v5_case_rate(hotel_id,hotel_name,section,case_rate,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$case_rate,'$other',$num,getdate());"
      st.executeUpdate(sql)
    })

    // 6.todo F 设计感：                all_score_final
    val allScoreFinalList: List[Double] = needDf.select("all_score_final").rdd.map(row => row.getAs[Double]("all_score_final")).collect().toList
    val allScoreFinalInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
    val allScoreFinalMap = SomeUtils.getRightIntervalBySetting(allScoreFinalList, allScoreFinalInterval)

    allScoreFinalMap.foreach(a => {
      val all_score_final = JavaSomeUtils.get2Double(a._1.toDouble)
      val num = a._2
      val other = "Vj ≠ 6"
      val sql = s"insert into new_canNum_v6_all_score_final(hotel_id,hotel_name,section,all_score_final,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$all_score_final,'$other',$num,getdate());"
      st.executeUpdate(sql)
    })

    // 7.todo G 设计感：                number
    val numberList: List[Double] = needDf.select("number").rdd.map(row => row.getAs[Double]("number")).collect().toList
    val numberInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
    val numberMap = SomeUtils.getRightIntervalBySetting(numberList, numberInterval)

    numberMap.foreach(a => {
      val number = JavaSomeUtils.get2Double(a._1.toDouble)
      val num = a._2
      val other = "Vj ≠ 7"
      val sql = s"insert into new_canNum_v7_number(hotel_id,hotel_name,section,number,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$number,'$other',$num,getdate());"
      st.executeUpdate(sql)
    })

    // 8.todo H 文字评价率：                text_rating_rate
    val textRatingRateList: List[Double] = needDf.select("text_rating_rate").rdd.map(row => row.getAs[Double]("text_rating_rate")).collect().toList
    val textRatingRateInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
    val textRatingRateMap = SomeUtils.getRightIntervalBySetting(textRatingRateList, textRatingRateInterval)

    textRatingRateMap.foreach(a => {
      val text_rating_rate = JavaSomeUtils.get2Double(a._1.toDouble)
      val num = a._2
      val other = "Vj ≠ 8"
      val sql = s"insert into new_canNum_v8_text_rating_rate(hotel_id,hotel_name,section,text_rating_rate,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$text_rating_rate,'$other',$num,getdate());"
      st.executeUpdate(sql)
    })

    // 9.todo I 服务费：                display_amount
    val displayAmountList: List[Double] = needDf.select("display_amount").rdd.map(row => row.getAs[Double]("display_amount")).collect().toList
    val displayAmountInterval = SomeUtils.getSplitIntervalSetting(0, 5000, 100, "[]")
    val displayAmountMap = SomeUtils.getRightIntervalBySetting(displayAmountList, displayAmountInterval)

    displayAmountMap.foreach(a => {
      val display_amount = JavaSomeUtils.get2Double(a._1.toDouble)
      val num = a._2
      val other = "Vj ≠ 9"
      val sql = s"insert into new_canNum_v9_display_amount(hotel_id,hotel_name,section,display_amount,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$display_amount,'$other',$num,getdate());"
      st.executeUpdate(sql)
    })

    // 10.todo J 到店率：                to_store_rate
    val toStoreRateList: List[Double] = needDf.select("to_store_rate").rdd.map(row => row.getAs[Double]("to_store_rate")).collect().toList
    val toStoreRateInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
    val toStoreRateMap = SomeUtils.getRightIntervalBySetting(toStoreRateList, toStoreRateInterval)

    toStoreRateMap.foreach(a => {
      val to_store_rate = JavaSomeUtils.get2Double(a._1.toDouble)
      val num = a._2
      val other = "Vj ≠ 10"
      val sql = s"insert into new_canNum_v10_to_store_rate(hotel_id,hotel_name,section,to_store_rate,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$to_store_rate,'$other',$num,getdate());"
      st.executeUpdate(sql)
    })

    needDf.unpersist()

    //    断开SQLServer连接
    JavaSQLServerConn.closeConnection(conn)
  }

//  def saveDfTotalData2(hotel_id: String, hotel_name: String, section: Int): Unit = {
//    val start = hotel_id + "=" + section.toString + "=0"
//    val stop = hotel_id + "=" + section.toString + "=zzzzzzzzzzzzzzzzzzzzzzzzzzz"
//    val result: ResultScanner = JavaHBaseUtils.getScanner(JavaRealLeaderProperties.MVTBSMPLANNERINDICATOR, start, stop)
//
//    var list = List[PlannerIndicator]()
//    var res = result.next()
//    while (res != null) {
//      val hotel_id = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("hotel_id")))
//      val section = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("section"))).toInt
//      val worker_id = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("worker_id")))
//      val hotel_name = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("hotel_name")))
//      val sort = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("sort"))).toDouble
//      val distance = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("distance"))).toLong
//      val case_dot = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("case_dot"))).toDouble
//      val reorder_rate = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("reorder_rate"))).toDouble
//      val communication_level = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("communication_level"))).toDouble
//      val design_sense = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("design_sense"))).toDouble
//      val case_rate = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("case_rate"))).toDouble
//      val all_score_final = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("all_score_final"))).toDouble
//      val number = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("number"))).toDouble
//      val text_rating_rate = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("text_rating_rate"))).toDouble
//      val display_amount = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("display_amount"))).toDouble
//      val to_store_rate = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("to_store_rate"))).toDouble
//      list = list.++(List[PlannerIndicator](new PlannerIndicator(hotel_id, section, worker_id, hotel_name, sort, distance, case_dot, reorder_rate, communication_level, design_sense, case_rate, all_score_final, number, text_rating_rate, display_amount, to_store_rate)))
//      res = result.next()
//    }
//    val resultDf = list.toDF()
//
//    val needDf = resultDf.filter(row => {
//      val distance = row.getAs[Long]("distance")
//      val distanceKM = (distance.toFloat / 1000)
//      distanceKM <= 20f
//    })
//
//    needDf.cache()
//
//    // 提供SQLServer连接
//    val conn = JavaSQLServerConn.getConnection
//    val st: Statement = conn.createStatement()
//
//    // 1.todo A	方案点：								case_dot
//    val caseDotList: List[Double] = needDf.select("case_dot").rdd.map(row => row.getAs[Double]("case_dot")).collect().toList
//    val caseDotInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
//    val caseDotMap = SomeUtils.getRightIntervalBySetting(caseDotList, caseDotInterval)
//    //    caseDotMap.toArray.sortBy(_._1.toDouble).toMap  // 针对数值进行排序
//
//    caseDotMap.foreach(a => {
//      val case_dot = JavaSomeUtils.get2Double(a._1.toDouble)
//      val num = a._2
//      val other = "Vj ≠ 1"
//      val sql = s"insert into new_canNum_v1_case_dot(hotel_id,hotel_name,section,case_dot,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$case_dot,'$other',$num,getdate());"
//      st.executeUpdate(sql)
//    })
//
//    // 2.todo B 回单率：                reorder_rate
//    val reorderRateList: List[Double] = needDf.select("reorder_rate").rdd.map(row => row.getAs[Double]("reorder_rate")).collect().toList
//    val reorderRateInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
//    val reorderRateMap = SomeUtils.getRightIntervalBySetting(reorderRateList, reorderRateInterval)
//
//    reorderRateMap.foreach(a => {
//      val case_dot = JavaSomeUtils.get2Double(a._1.toDouble)
//      val num = a._2
//      val other = "Vj ≠ 2"
//      val sql = s"insert into new_canNum_v2_reorder_rate(hotel_id,hotel_name,section,reorder_rate,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$case_dot,'$other',$num,getdate());"
//      st.executeUpdate(sql)
//    })
//
//    // 3.todo C 沟通水平：                communication_level
//    val communicationLevelList: List[Double] = needDf.select("communication_level").rdd.map(row => row.getAs[Double]("communication_level")).collect().toList
//    val communicationLevelInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
//    val communicationLevelMap = SomeUtils.getRightIntervalBySetting(communicationLevelList, communicationLevelInterval)
//
//    communicationLevelMap.foreach(a => {
//      val communication_level = JavaSomeUtils.get2Double(a._1.toDouble)
//      val num = a._2
//      val other = "Vj ≠ 3"
//      val sql = s"insert into new_canNum_v3_communication_level(hotel_id,hotel_name,section,communication_level,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$communication_level,'$other',$num,getdate());"
//      st.executeUpdate(sql)
//    })
//
//    // 4.todo D 设计感：                design_sense
//    val designSenseList: List[Double] = needDf.select("design_sense").rdd.map(row => row.getAs[Double]("design_sense")).collect().toList
//    val designSenseInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
//    val designSenseMap = SomeUtils.getRightIntervalBySetting(designSenseList, designSenseInterval)
//
//    designSenseMap.foreach(a => {
//      val design_sense = JavaSomeUtils.get2Double(a._1.toDouble)
//      val num = a._2
//      val other = "Vj ≠ 4"
//      val sql = s"insert into new_canNum_v4_design_sense(hotel_id,hotel_name,section,design_sense,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$design_sense,'$other',$num,getdate());"
//      st.executeUpdate(sql)
//    })
//
//    // 5.todo E 设计感：                case_rate
//    val caseRateList: List[Double] = needDf.select("case_rate").rdd.map(row => row.getAs[Double]("case_rate")).collect().toList
//    val caseRateInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
//    val caseRateMap = SomeUtils.getRightIntervalBySetting(caseRateList, caseRateInterval)
//
//    caseRateMap.foreach(a => {
//      val case_rate = JavaSomeUtils.get2Double(a._1.toDouble)
//      val num = a._2
//      val other = "Vj ≠ 5"
//      val sql = s"insert into new_canNum_v5_case_rate(hotel_id,hotel_name,section,case_rate,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$case_rate,'$other',$num,getdate());"
//      st.executeUpdate(sql)
//    })
//
//    // 6.todo F 设计感：                all_score_final
//    val allScoreFinalList: List[Double] = needDf.select("all_score_final").rdd.map(row => row.getAs[Double]("all_score_final")).collect().toList
//    val allScoreFinalInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
//    val allScoreFinalMap = SomeUtils.getRightIntervalBySetting(allScoreFinalList, allScoreFinalInterval)
//
//    allScoreFinalMap.foreach(a => {
//      val all_score_final = JavaSomeUtils.get2Double(a._1.toDouble)
//      val num = a._2
//      val other = "Vj ≠ 6"
//      val sql = s"insert into new_canNum_v6_all_score_final(hotel_id,hotel_name,section,all_score_final,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$all_score_final,'$other',$num,getdate());"
//      st.executeUpdate(sql)
//    })
//
//    // 7.todo G 设计感：                number
//    val numberList: List[Double] = needDf.select("number").rdd.map(row => row.getAs[Double]("number")).collect().toList
//    val numberInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
//    val numberMap = SomeUtils.getRightIntervalBySetting(numberList, numberInterval)
//
//    numberMap.foreach(a => {
//      val number = JavaSomeUtils.get2Double(a._1.toDouble)
//      val num = a._2
//      val other = "Vj ≠ 7"
//      val sql = s"insert into new_canNum_v7_number(hotel_id,hotel_name,section,number,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$number,'$other',$num,getdate());"
//      st.executeUpdate(sql)
//    })
//
//    // 8.todo H 文字评价率：                text_rating_rate
//    val textRatingRateList: List[Double] = needDf.select("text_rating_rate").rdd.map(row => row.getAs[Double]("text_rating_rate")).collect().toList
//    val textRatingRateInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
//    val textRatingRateMap = SomeUtils.getRightIntervalBySetting(textRatingRateList, textRatingRateInterval)
//
//    textRatingRateMap.foreach(a => {
//      val text_rating_rate = JavaSomeUtils.get2Double(a._1.toDouble)
//      val num = a._2
//      val other = "Vj ≠ 8"
//      val sql = s"insert into new_canNum_v8_text_rating_rate(hotel_id,hotel_name,section,text_rating_rate,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$text_rating_rate,'$other',$num,getdate());"
//      st.executeUpdate(sql)
//    })
//
//    // 9.todo I 服务费：                display_amount
//    val displayAmountList: List[Double] = needDf.select("display_amount").rdd.map(row => row.getAs[Double]("display_amount")).collect().toList
//    val displayAmountInterval = SomeUtils.getSplitIntervalSetting(0, 5000, 100, "[]")
//    val displayAmountMap = SomeUtils.getRightIntervalBySetting(displayAmountList, displayAmountInterval)
//
//    displayAmountMap.foreach(a => {
//      val display_amount = JavaSomeUtils.get2Double(a._1.toDouble)
//      val num = a._2
//      val other = "Vj ≠ 9"
//      val sql = s"insert into new_canNum_v9_display_amount(hotel_id,hotel_name,section,display_amount,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$display_amount,'$other',$num,getdate());"
//      st.executeUpdate(sql)
//    })
//
//    // 10.todo J 到店率：                to_store_rate
//    val toStoreRateList: List[Double] = needDf.select("to_store_rate").rdd.map(row => row.getAs[Double]("to_store_rate")).collect().toList
//    val toStoreRateInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
//    val toStoreRateMap = SomeUtils.getRightIntervalBySetting(toStoreRateList, toStoreRateInterval)
//
//    toStoreRateMap.foreach(a => {
//      val to_store_rate = JavaSomeUtils.get2Double(a._1.toDouble)
//      val num = a._2
//      val other = "Vj ≠ 10"
//      val sql = s"insert into new_canNum_v10_to_store_rate(hotel_id,hotel_name,section,to_store_rate,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$to_store_rate,'$other',$num,getdate());"
//      st.executeUpdate(sql)
//    })
//
//    needDf.unpersist()
//
//    //    断开SQLServer连接
//    JavaSQLServerConn.closeConnection(conn)
//  }

  def saveDfTotalDataOld(hotel_id: String, hotel_name: String, section: Int): Unit = {
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
    val allNeedDf = spark.sql(s"select bbb.worker_id,ddd.section,bbb.case_dot,bbb.reorder_rate,bbb.communication_level,bbb.design_sense,bbb.case_rate,bbb.all_score_final,bbb.number,bbb.to_store_rate,bbb.text_rating_rate,ddd.display_amount from bbb left join ddd on bbb.worker_id = ddd.worker_id where ddd.section = $section").distinct()

    // 获取过滤后距离和预算区间的数据。
    val needDf = allNeedDf.filter(row => {
      val worker_id = row.getAs[String]("worker_id")
      val distance = JavaHBaseUtils.getValue("v2_rp_workers_hotel_distance", hotel_id + ":" + worker_id, "info", "distance")
      val distanceKM = (distance.toFloat / 1000)
      distanceKM <= 20
    })
    needDf.cache()
    //    val realLeaderDF = spark.read.format("jdbc")
    //      .option("url", JavaRealLeaderProperties.MYSQLURL)
    //      .option("dbtable", "真正领导数据源2")
    //      .option("user", JavaRealLeaderProperties.MYSQLUSER)
    //      .option("password", JavaRealLeaderProperties.MYSQLPASSWORD)
    //      .load()
    //      .withColumnRenamed("到店签单率", "to_store_rate")
    //      .withColumnRenamed("assessment_rate", "text_rating_rate")
    //      .withColumnRenamed("服务费", "display_amount")
    //      .filter(row => {
    //        val dt = row.getAs[Timestamp]("ctime")
    //        import java.text.SimpleDateFormat
    //        val formatter = new SimpleDateFormat("yyyyMMdd")
    //        val dateString = formatter.format(dt)
    //        dateString == MyUtils.getFromToday(0)
    //      })
    //    realLeaderDF.createOrReplaceTempView("rrr")
    //
    //    val tbLowBudgetHotel2 = spark.read.format("jdbc")
    //      .option("url", JavaRealLeaderProperties.MYSQLURL)
    //      .option("dbtable", "tb_low_budget_hotel2")
    //      .option("user", JavaRealLeaderProperties.MYSQLUSER)
    //      .option("password", JavaRealLeaderProperties.MYSQLPASSWORD)
    //      .load()
    //      .withColumnRenamed("hotel", "hotel_name")
    //    tbLowBudgetHotel2.createOrReplaceTempView("hhh")
    //
    //    val resultDf = spark.sql(s"select hotel_id,hhh.section,hhh.worker_id,hotel_name,rrr.sort,distance,case_dot,reorder_rate,communication_level,design_sense,case_rate,all_score_final,number,text_rating_rate,display_amount,to_store_rate from rrr left join hhh on rrr.worker_id = hhh.worker_id and rrr.section = hhh.section where hotel_name = '$hotel_name' and hhh.section = $section")
    //
    //    val needDf = resultDf.filter(row => {
    //      val distance = row.getAs[Long]("distance")
    //      val distanceKM = (distance.toFloat / 1000)
    //      distanceKM <= 20f
    //    })

    //提供SQLServer连接
    val conn = JavaSQLServerConn.getConnection
    val st: Statement = conn.createStatement()

    // 1.todo A	方案点：								case_dot
    val caseDotList: List[Double] = needDf.select("case_dot").rdd.map(row => row.getAs[Double]("case_dot")).collect().toList
    val caseDotInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
    val caseDotMap = SomeUtils.getRightIntervalBySetting(caseDotList, caseDotInterval)
    //    caseDotMap.toArray.sortBy(_._1.toDouble).toMap  // 针对数值进行排序

    caseDotMap.foreach(a => {
      val case_dot = JavaSomeUtils.get2Double(a._1.toDouble)
      val num = a._2
      val other = "Vj ≠ 1"
      val sql = s"insert into canNum_v1_case_dot(hotel_id,hotel_name,section,case_dot,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$case_dot,'$other',$num,getdate());"
      st.executeUpdate(sql)
    })

    // 2.todo B 回单率：                reorder_rate
    val reorderRateList: List[Double] = needDf.select("reorder_rate").rdd.map(row => row.getAs[Double]("reorder_rate")).collect().toList
    val reorderRateInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
    val reorderRateMap = SomeUtils.getRightIntervalBySetting(reorderRateList, reorderRateInterval)

    reorderRateMap.foreach(a => {
      val case_dot = JavaSomeUtils.get2Double(a._1.toDouble)
      val num = a._2
      val other = "Vj ≠ 2"
      val sql = s"insert into canNum_v2_reorder_rate(hotel_id,hotel_name,section,reorder_rate,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$case_dot,'$other',$num,getdate());"
      st.executeUpdate(sql)
    })

    // 3.todo C 沟通水平：                communication_level
    val communicationLevelList: List[Double] = needDf.select("communication_level").rdd.map(row => row.getAs[Double]("communication_level")).collect().toList
    val communicationLevelInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
    val communicationLevelMap = SomeUtils.getRightIntervalBySetting(communicationLevelList, communicationLevelInterval)

    communicationLevelMap.foreach(a => {
      val communication_level = JavaSomeUtils.get2Double(a._1.toDouble)
      val num = a._2
      val other = "Vj ≠ 3"
      val sql = s"insert into canNum_v3_communication_level(hotel_id,hotel_name,section,communication_level,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$communication_level,'$other',$num,getdate());"
      st.executeUpdate(sql)
    })

    // 4.todo D 设计感：                design_sense
    val designSenseList: List[Double] = needDf.select("design_sense").rdd.map(row => row.getAs[Double]("design_sense")).collect().toList
    val designSenseInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
    val designSenseMap = SomeUtils.getRightIntervalBySetting(designSenseList, designSenseInterval)

    designSenseMap.foreach(a => {
      val design_sense = JavaSomeUtils.get2Double(a._1.toDouble)
      val num = a._2
      val other = "Vj ≠ 4"
      val sql = s"insert into canNum_v4_design_sense(hotel_id,hotel_name,section,design_sense,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$design_sense,'$other',$num,getdate());"
      st.executeUpdate(sql)
    })

    // 5.todo E 设计感：                case_rate
    val caseRateList: List[Double] = needDf.select("case_rate").rdd.map(row => row.getAs[Double]("case_rate")).collect().toList
    val caseRateInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
    val caseRateMap = SomeUtils.getRightIntervalBySetting(caseRateList, caseRateInterval)

    caseRateMap.foreach(a => {
      val case_rate = JavaSomeUtils.get2Double(a._1.toDouble)
      val num = a._2
      val other = "Vj ≠ 5"
      val sql = s"insert into canNum_v5_case_rate(hotel_id,hotel_name,section,case_rate,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$case_rate,'$other',$num,getdate());"
      st.executeUpdate(sql)
    })

    // 6.todo F 设计感：                all_score_final
    val allScoreFinalList: List[Double] = needDf.select("all_score_final").rdd.map(row => row.getAs[Double]("all_score_final")).collect().toList
    val allScoreFinalInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
    val allScoreFinalMap = SomeUtils.getRightIntervalBySetting(allScoreFinalList, allScoreFinalInterval)

    allScoreFinalMap.foreach(a => {
      val all_score_final = JavaSomeUtils.get2Double(a._1.toDouble)
      val num = a._2
      val other = "Vj ≠ 6"
      val sql = s"insert into canNum_v6_all_score_final(hotel_id,hotel_name,section,all_score_final,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$all_score_final,'$other',$num,getdate());"
      st.executeUpdate(sql)
    })

    // 7.todo G 设计感：                number
    val numberList: List[Double] = needDf.select("number").rdd.map(row => row.getAs[Double]("number")).collect().toList
    val numberInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
    val numberMap = SomeUtils.getRightIntervalBySetting(numberList, numberInterval)

    numberMap.foreach(a => {
      val number = JavaSomeUtils.get2Double(a._1.toDouble)
      val num = a._2
      val other = "Vj ≠ 7"
      val sql = s"insert into canNum_v7_number(hotel_id,hotel_name,section,number,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$number,'$other',$num,getdate());"
      st.executeUpdate(sql)
    })

    // 8.todo H 文字评价率：                text_rating_rate
    val textRatingRateList: List[Double] = needDf.select("text_rating_rate").rdd.map(row => row.getAs[Double]("text_rating_rate")).collect().toList
    val textRatingRateInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
    val textRatingRateMap = SomeUtils.getRightIntervalBySetting(textRatingRateList, textRatingRateInterval)

    textRatingRateMap.foreach(a => {
      val text_rating_rate = JavaSomeUtils.get2Double(a._1.toDouble)
      val num = a._2
      val other = "Vj ≠ 8"
      val sql = s"insert into canNum_v8_text_rating_rate(hotel_id,hotel_name,section,text_rating_rate,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$text_rating_rate,'$other',$num,getdate());"
      st.executeUpdate(sql)
    })

    // 9.todo I 服务费：                display_amount
    val displayAmountList: List[Double] = needDf.select("display_amount").rdd.map(row => row.getAs[Double]("display_amount")).collect().toList
    val displayAmountInterval = SomeUtils.getSplitIntervalSetting(0, 5000, 100, "[]")
    val displayAmountMap = SomeUtils.getRightIntervalBySetting(displayAmountList, displayAmountInterval)

    displayAmountMap.foreach(a => {
      val display_amount = JavaSomeUtils.get2Double(a._1.toDouble)
      val num = a._2
      val other = "Vj ≠ 9"
      val sql = s"insert into canNum_v9_display_amount(hotel_id,hotel_name,section,display_amount,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$display_amount,'$other',$num,getdate());"
      st.executeUpdate(sql)
    })

    // 10.todo J 到店率：                to_store_rate
    val toStoreRateList: List[Double] = needDf.select("to_store_rate").rdd.map(row => row.getAs[Double]("to_store_rate")).collect().toList
    val toStoreRateInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
    val toStoreRateMap = SomeUtils.getRightIntervalBySetting(toStoreRateList, toStoreRateInterval)

    toStoreRateMap.foreach(a => {
      val to_store_rate = JavaSomeUtils.get2Double(a._1.toDouble)
      val num = a._2
      val other = "Vj ≠ 10"
      val sql = s"insert into canNum_v10_to_store_rate(hotel_id,hotel_name,section,to_store_rate,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$to_store_rate,'$other',$num,getdate());"
      st.executeUpdate(sql)
    })

    needDf.unpersist()

    //    断开SQLServer连接
    JavaSQLServerConn.closeConnection(conn)
  }

  // 策划师指标对象
  case class PlannerIndicator(hotel_id: String, section: Int, worker_id: String, hotel_name: String, sort: Double, distance: Long, case_dot: Double,
                              reorder_rate: Double, communication_level: Double, design_sense: Double, case_rate: Double, all_score_final: Double,
                              number: Double, text_rating_rate: Double, display_amount: Double, to_store_rate: Double)

//  def saveDfTotalDataOld2(hotel_id: String, hotel_name: String, section: Int): Unit = {
//    val start = hotel_id + "=" + section.toString + "=0"
//    val stop = hotel_id + "=" + section.toString + "=zzzzzzzzzzzzzzzzzzzzzzzzzzz"
//    val result: ResultScanner = JavaHBaseUtils.getScanner(JavaRealLeaderProperties.MVTBSMPLANNERINDICATOROLD, start, stop)
//
//    var list = List[PlannerIndicator]()
//    var res = result.next()
//    while (res != null) {
//      val hotel_id = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("hotel_id")))
//      val section = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("section"))).toInt
//      val worker_id = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("worker_id")))
//      val hotel_name = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("hotel_name")))
//      val sort = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("sort"))).toDouble
//      val distance = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("distance"))).toLong
//      val case_dot = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("case_dot"))).toDouble
//      val reorder_rate = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("reorder_rate"))).toDouble
//      val communication_level = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("communication_level"))).toDouble
//      val design_sense = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("design_sense"))).toDouble
//      val case_rate = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("case_rate"))).toDouble
//      val all_score_final = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("all_score_final"))).toDouble
//      val number = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("number"))).toDouble
//      val text_rating_rate = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("text_rating_rate"))).toDouble
//      val display_amount = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("display_amount"))).toDouble
//      val to_store_rate = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("to_store_rate"))).toDouble
//      list = list.++(List[PlannerIndicator](new PlannerIndicator(hotel_id, section, worker_id, hotel_name, sort, distance, case_dot, reorder_rate, communication_level, design_sense, case_rate, all_score_final, number, text_rating_rate, display_amount, to_store_rate)))
//      res = result.next()
//    }
//    val allNeedDf = list.toDF()
//
//    // 获取过滤后距离和预算区间的数据。
//    val needDf = allNeedDf.filter(row => {
//      val distance = row.getAs[Long]("distance")
//      val distanceKM = (distance.toFloat / 1000)
//      distanceKM <= 20
//    })
//    needDf.cache()
//    //    val realLeaderDF = spark.read.format("jdbc")
//    //      .option("url", JavaRealLeaderProperties.MYSQLURL)
//    //      .option("dbtable", "真正领导数据源2")
//    //      .option("user", JavaRealLeaderProperties.MYSQLUSER)
//    //      .option("password", JavaRealLeaderProperties.MYSQLPASSWORD)
//    //      .load()
//    //      .withColumnRenamed("到店签单率", "to_store_rate")
//    //      .withColumnRenamed("assessment_rate", "text_rating_rate")
//    //      .withColumnRenamed("服务费", "display_amount")
//    //      .filter(row => {
//    //        val dt = row.getAs[Timestamp]("ctime")
//    //        import java.text.SimpleDateFormat
//    //        val formatter = new SimpleDateFormat("yyyyMMdd")
//    //        val dateString = formatter.format(dt)
//    //        dateString == MyUtils.getFromToday(0)
//    //      })
//    //    realLeaderDF.createOrReplaceTempView("rrr")
//    //
//    //    val tbLowBudgetHotel2 = spark.read.format("jdbc")
//    //      .option("url", JavaRealLeaderProperties.MYSQLURL)
//    //      .option("dbtable", "tb_low_budget_hotel2")
//    //      .option("user", JavaRealLeaderProperties.MYSQLUSER)
//    //      .option("password", JavaRealLeaderProperties.MYSQLPASSWORD)
//    //      .load()
//    //      .withColumnRenamed("hotel", "hotel_name")
//    //    tbLowBudgetHotel2.createOrReplaceTempView("hhh")
//    //
//    //    val resultDf = spark.sql(s"select hotel_id,hhh.section,hhh.worker_id,hotel_name,rrr.sort,distance,case_dot,reorder_rate,communication_level,design_sense,case_rate,all_score_final,number,text_rating_rate,display_amount,to_store_rate from rrr left join hhh on rrr.worker_id = hhh.worker_id and rrr.section = hhh.section where hotel_name = '$hotel_name' and hhh.section = $section")
//    //
//    //    val needDf = resultDf.filter(row => {
//    //      val distance = row.getAs[Long]("distance")
//    //      val distanceKM = (distance.toFloat / 1000)
//    //      distanceKM <= 20f
//    //    })
//
//    //提供SQLServer连接
//    val conn = JavaSQLServerConn.getConnection
//    val st: Statement = conn.createStatement()
//
//    // 1.todo A	方案点：								case_dot
//    val caseDotList: List[Double] = needDf.select("case_dot").rdd.map(row => row.getAs[Double]("case_dot")).collect().toList
//    val caseDotInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
//    val caseDotMap = SomeUtils.getRightIntervalBySetting(caseDotList, caseDotInterval)
//    //    caseDotMap.toArray.sortBy(_._1.toDouble).toMap  // 针对数值进行排序
//
//    caseDotMap.foreach(a => {
//      val case_dot = JavaSomeUtils.get2Double(a._1.toDouble)
//      val num = a._2
//      val other = "Vj ≠ 1"
//      val sql = s"insert into canNum_v1_case_dot(hotel_id,hotel_name,section,case_dot,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$case_dot,'$other',$num,getdate());"
//      st.executeUpdate(sql)
//    })
//
//    // 2.todo B 回单率：                reorder_rate
//    val reorderRateList: List[Double] = needDf.select("reorder_rate").rdd.map(row => row.getAs[Double]("reorder_rate")).collect().toList
//    val reorderRateInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
//    val reorderRateMap = SomeUtils.getRightIntervalBySetting(reorderRateList, reorderRateInterval)
//
//    reorderRateMap.foreach(a => {
//      val case_dot = JavaSomeUtils.get2Double(a._1.toDouble)
//      val num = a._2
//      val other = "Vj ≠ 2"
//      val sql = s"insert into canNum_v2_reorder_rate(hotel_id,hotel_name,section,reorder_rate,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$case_dot,'$other',$num,getdate());"
//      st.executeUpdate(sql)
//    })
//
//    // 3.todo C 沟通水平：                communication_level
//    val communicationLevelList: List[Double] = needDf.select("communication_level").rdd.map(row => row.getAs[Double]("communication_level")).collect().toList
//    val communicationLevelInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
//    val communicationLevelMap = SomeUtils.getRightIntervalBySetting(communicationLevelList, communicationLevelInterval)
//
//    communicationLevelMap.foreach(a => {
//      val communication_level = JavaSomeUtils.get2Double(a._1.toDouble)
//      val num = a._2
//      val other = "Vj ≠ 3"
//      val sql = s"insert into canNum_v3_communication_level(hotel_id,hotel_name,section,communication_level,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$communication_level,'$other',$num,getdate());"
//      st.executeUpdate(sql)
//    })
//
//    // 4.todo D 设计感：                design_sense
//    val designSenseList: List[Double] = needDf.select("design_sense").rdd.map(row => row.getAs[Double]("design_sense")).collect().toList
//    val designSenseInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
//    val designSenseMap = SomeUtils.getRightIntervalBySetting(designSenseList, designSenseInterval)
//
//    designSenseMap.foreach(a => {
//      val design_sense = JavaSomeUtils.get2Double(a._1.toDouble)
//      val num = a._2
//      val other = "Vj ≠ 4"
//      val sql = s"insert into canNum_v4_design_sense(hotel_id,hotel_name,section,design_sense,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$design_sense,'$other',$num,getdate());"
//      st.executeUpdate(sql)
//    })
//
//    // 5.todo E 设计感：                case_rate
//    val caseRateList: List[Double] = needDf.select("case_rate").rdd.map(row => row.getAs[Double]("case_rate")).collect().toList
//    val caseRateInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
//    val caseRateMap = SomeUtils.getRightIntervalBySetting(caseRateList, caseRateInterval)
//
//    caseRateMap.foreach(a => {
//      val case_rate = JavaSomeUtils.get2Double(a._1.toDouble)
//      val num = a._2
//      val other = "Vj ≠ 5"
//      val sql = s"insert into canNum_v5_case_rate(hotel_id,hotel_name,section,case_rate,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$case_rate,'$other',$num,getdate());"
//      st.executeUpdate(sql)
//    })
//
//    // 6.todo F 设计感：                all_score_final
//    val allScoreFinalList: List[Double] = needDf.select("all_score_final").rdd.map(row => row.getAs[Double]("all_score_final")).collect().toList
//    val allScoreFinalInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
//    val allScoreFinalMap = SomeUtils.getRightIntervalBySetting(allScoreFinalList, allScoreFinalInterval)
//
//    allScoreFinalMap.foreach(a => {
//      val all_score_final = JavaSomeUtils.get2Double(a._1.toDouble)
//      val num = a._2
//      val other = "Vj ≠ 6"
//      val sql = s"insert into canNum_v6_all_score_final(hotel_id,hotel_name,section,all_score_final,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$all_score_final,'$other',$num,getdate());"
//      st.executeUpdate(sql)
//    })
//
//    // 7.todo G 设计感：                number
//    val numberList: List[Double] = needDf.select("number").rdd.map(row => row.getAs[Double]("number")).collect().toList
//    val numberInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
//    val numberMap = SomeUtils.getRightIntervalBySetting(numberList, numberInterval)
//
//    numberMap.foreach(a => {
//      val number = JavaSomeUtils.get2Double(a._1.toDouble)
//      val num = a._2
//      val other = "Vj ≠ 7"
//      val sql = s"insert into canNum_v7_number(hotel_id,hotel_name,section,number,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$number,'$other',$num,getdate());"
//      st.executeUpdate(sql)
//    })
//
//    // 8.todo H 文字评价率：                text_rating_rate
//    val textRatingRateList: List[Double] = needDf.select("text_rating_rate").rdd.map(row => row.getAs[Double]("text_rating_rate")).collect().toList
//    val textRatingRateInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
//    val textRatingRateMap = SomeUtils.getRightIntervalBySetting(textRatingRateList, textRatingRateInterval)
//
//    textRatingRateMap.foreach(a => {
//      val text_rating_rate = JavaSomeUtils.get2Double(a._1.toDouble)
//      val num = a._2
//      val other = "Vj ≠ 8"
//      val sql = s"insert into canNum_v8_text_rating_rate(hotel_id,hotel_name,section,text_rating_rate,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$text_rating_rate,'$other',$num,getdate());"
//      st.executeUpdate(sql)
//    })
//
//    // 9.todo I 服务费：                display_amount
//    val displayAmountList: List[Double] = needDf.select("display_amount").rdd.map(row => row.getAs[Double]("display_amount")).collect().toList
//    val displayAmountInterval = SomeUtils.getSplitIntervalSetting(0, 5000, 100, "[]")
//    val displayAmountMap = SomeUtils.getRightIntervalBySetting(displayAmountList, displayAmountInterval)
//
//    displayAmountMap.foreach(a => {
//      val display_amount = JavaSomeUtils.get2Double(a._1.toDouble)
//      val num = a._2
//      val other = "Vj ≠ 9"
//      val sql = s"insert into canNum_v9_display_amount(hotel_id,hotel_name,section,display_amount,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$display_amount,'$other',$num,getdate());"
//      st.executeUpdate(sql)
//    })
//
//    // 10.todo J 到店率：                to_store_rate
//    val toStoreRateList: List[Double] = needDf.select("to_store_rate").rdd.map(row => row.getAs[Double]("to_store_rate")).collect().toList
//    val toStoreRateInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
//    val toStoreRateMap = SomeUtils.getRightIntervalBySetting(toStoreRateList, toStoreRateInterval)
//
//    toStoreRateMap.foreach(a => {
//      val to_store_rate = JavaSomeUtils.get2Double(a._1.toDouble)
//      val num = a._2
//      // 2.todo B 回单率：                reorder_rate
//      val reorderRateList: List[Double] = needDf.select("reorder_rate").rdd.map(row => row.getAs[Double]("reorder_rate")).collect().toList
//      val reorderRateInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
//      val reorderRateMap = SomeUtils.getRightIntervalBySetting(reorderRateList, reorderRateInterval)
//
//      reorderRateMap.foreach(a => {
//        val case_dot = JavaSomeUtils.get2Double(a._1.toDouble)
//        val num = a._2
//        val other = "Vj ≠ 2"
//        val sql = s"insert into canNum_v2_reorder_rate(hotel_id,hotel_name,section,reorder_rate,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$case_dot,'$other',$num,getdate());"
//        st.executeUpdate(sql)
//      })
//
//      // 3.todo C 沟通水平：                communication_level
//      val communicationLevelList: List[Double] = needDf.select("communication_level").rdd.map(row => row.getAs[Double]("communication_level")).collect().toList
//      val communicationLevelInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
//      val communicationLevelMap = SomeUtils.getRightIntervalBySetting(communicationLevelList, communicationLevelInterval)
//
//      communicationLevelMap.foreach(a => {
//        val communication_level = JavaSomeUtils.get2Double(a._1.toDouble)
//        val num = a._2
//        val other = "Vj ≠ 3"
//        val sql = s"insert into canNum_v3_communication_level(hotel_id,hotel_name,section,communication_level,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$communication_level,'$other',$num,getdate());"
//        st.executeUpdate(sql)
//      })
//
//      // 4.todo D 设计感：                design_sense
//      val designSenseList: List[Double] = needDf.select("design_sense").rdd.map(row => row.getAs[Double]("design_sense")).collect().toList
//      val designSenseInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
//      val designSenseMap = SomeUtils.getRightIntervalBySetting(designSenseList, designSenseInterval)
//
//      designSenseMap.foreach(a => {
//        val design_sense = JavaSomeUtils.get2Double(a._1.toDouble)
//        val num = a._2
//        val other = "Vj ≠ 4"
//        val sql = s"insert into canNum_v4_design_sense(hotel_id,hotel_name,section,design_sense,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$design_sense,'$other',$num,getdate());"
//        st.executeUpdate(sql)
//      })
//
//      // 5.todo E 设计感：                case_rate
//      val caseRateList: List[Double] = needDf.select("case_rate").rdd.map(row => row.getAs[Double]("case_rate")).collect().toList
//      val caseRateInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
//      val caseRateMap = SomeUtils.getRightIntervalBySetting(caseRateList, caseRateInterval)
//
//      caseRateMap.foreach(a => {
//        val case_rate = JavaSomeUtils.get2Double(a._1.toDouble)
//        val num = a._2
//        val other = "Vj ≠ 5"
//        val sql = s"insert into canNum_v5_case_rate(hotel_id,hotel_name,section,case_rate,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$case_rate,'$other',$num,getdate());"
//        st.executeUpdate(sql)
//      })
//
//      // 6.todo F 设计感：                all_score_final
//      val allScoreFinalList: List[Double] = needDf.select("all_score_final").rdd.map(row => row.getAs[Double]("all_score_final")).collect().toList
//      val allScoreFinalInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
//      val allScoreFinalMap = SomeUtils.getRightIntervalBySetting(allScoreFinalList, allScoreFinalInterval)
//
//      allScoreFinalMap.foreach(a => {
//        val all_score_final = JavaSomeUtils.get2Double(a._1.toDouble)
//        val num = a._2
//        val other = "Vj ≠ 6"
//        val sql = s"insert into canNum_v6_all_score_final(hotel_id,hotel_name,section,all_score_final,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$all_score_final,'$other',$num,getdate());"
//        st.executeUpdate(sql)
//      })
//
//      // 7.todo G 设计感：                number
//      val numberList: List[Double] = needDf.select("number").rdd.map(row => row.getAs[Double]("number")).collect().toList
//      val numberInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
//      val numberMap = SomeUtils.getRightIntervalBySetting(numberList, numberInterval)
//
//      numberMap.foreach(a => {
//        val number = JavaSomeUtils.get2Double(a._1.toDouble)
//        val num = a._2
//        val other = "Vj ≠ 7"
//        val sql = s"insert into canNum_v7_number(hotel_id,hotel_name,section,number,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$number,'$other',$num,getdate());"
//        st.executeUpdate(sql)
//      })
//
//      // 8.todo H 文字评价率：                text_rating_rate
//      val textRatingRateList: List[Double] = needDf.select("text_rating_rate").rdd.map(row => row.getAs[Double]("text_rating_rate")).collect().toList
//      val textRatingRateInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
//      val textRatingRateMap = SomeUtils.getRightIntervalBySetting(textRatingRateList, textRatingRateInterval)
//
//      textRatingRateMap.foreach(a => {
//        val text_rating_rate = JavaSomeUtils.get2Double(a._1.toDouble)
//        val num = a._2
//        val other = "Vj ≠ 8"
//        val sql = s"insert into canNum_v8_text_rating_rate(hotel_id,hotel_name,section,text_rating_rate,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$text_rating_rate,'$other',$num,getdate());"
//        st.executeUpdate(sql)
//      })
//
//      // 9.todo I 服务费：                display_amount
//      val displayAmountList: List[Double] = needDf.select("display_amount").rdd.map(row => row.getAs[Double]("display_amount")).collect().toList
//      val displayAmountInterval = SomeUtils.getSplitIntervalSetting(0, 5000, 100, "[]")
//      val displayAmountMap = SomeUtils.getRightIntervalBySetting(displayAmountList, displayAmountInterval)
//
//      displayAmountMap.foreach(a => {
//        val display_amount = JavaSomeUtils.get2Double(a._1.toDouble)
//        val num = a._2
//        val other = "Vj ≠ 9"
//        val sql = s"insert into canNum_v9_display_amount(hotel_id,hotel_name,section,display_amount,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$display_amount,'$other',$num,getdate());"
//        st.executeUpdate(sql)
//      })
//
//      // 10.todo J 到店率：                to_store_rate
//      val toStoreRateList: List[Double] = needDf.select("to_store_rate").rdd.map(row => row.getAs[Double]("to_store_rate")).collect().toList
//      val toStoreRateInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
//      val toStoreRateMap = SomeUtils.getRightIntervalBySetting(toStoreRateList, toStoreRateInterval)
//
//      toStoreRateMap.foreach(a => {
//        val to_store_rate = JavaSomeUtils.get2Double(a._1.toDouble)
//        val num = a._2
//        val other = "Vj ≠ 10"
//        val sql = s"insert into canNum_v10_to_store_rate(hotel_id,hotel_name,section,to_store_rate,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$to_store_rate,'$other',$num,getdate());"
//        st.executeUpdate(sql)
//      })
//      val other = "Vj ≠ 10"
//      val sql = s"insert into canNum_v10_to_store_rate(hotel_id,hotel_name,section,to_store_rate,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$to_store_rate,'$other',$num,getdate());"
//      st.executeUpdate(sql)
//    })
//
//    needDf.unpersist()
//
//    //    断开SQLServer连接
//    JavaSQLServerConn.closeConnection(conn)
//  }

}
