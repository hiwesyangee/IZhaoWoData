package com.realleaderAndyanjing.engine

import com.alibaba.fastjson.JSON
import com.izhaowo.cores.utils.{JavaSQLServerConn, JavaSparkUtils, MyUtils}
import com.realleader.properties.JavaRealLeaderProperties
import com.realleaderAndyanjing.engine.RealLeaderAndYanJingEngine.spark
import com.yanjing.engine.SomeUtils
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame


/**
  * 测试SQLServer数据库的批量写入
  */
object Test {
  def main(args: Array[String]): Unit = {
    //    val start = System.currentTimeMillis()
    //    //    batchSaveData2SQLServer2()
    //    println("time1 = " + cheackData)
    //    val stop = System.currentTimeMillis()
    //    println("time = " + (stop - start) + "ms.")

    //    val map = mutable.HashMap[Double, Int]()
    //    map.+=(0d -> 1)
    //    map.+=(1d -> 2)
    //    map.foreach(kv => {
    //      println(kv._1 + "==" + kv._2)
    //    })
    //
    //    println(map.size)

    //    val time1 = System.currentTimeMillis()
    //    var vector = Vector[String]()
    //    for (i <- 0 to 50000)
    //      vector = vector ++ Vector("String")
    //    val time2 = System.currentTimeMillis()
    //    var list = List[String]()
    //    for (i <- 0 to 50000)
    //      list = list.++(List("String"))
    //    val time3 = System.currentTimeMillis()
    //
    //    vector.foreach(println)
    //    println("======================")
    //    list.foreach(println)
    //
    //

    //    var allIntervalVector = SomeUtils.getSplitIntervalSettingVector(0, 5000, 100, "[]")
    //    allIntervalVector.foreach(println)
    //    println("删除")
    //    allIntervalVector = allIntervalVector.drop(allIntervalVector.size)
    //    allIntervalVector.foreach(println)
    //
    //    var vec = Vector[Int]()
    //    var size = 0
    //    for (list <- 1 to 81) {
    //      size = size + 1
    //      vec = vec ++ Vector(list)
    //      if (size % 5 == 0) {
    //        println("==============开始50一次的循环")
    //        println("vector开始是:" + vec.size)
    //        vec.foreach(println)
    //        vec = Vector[Int]()
    //        println("vector结束是:" + vec.size)
    //      }
    //    }
    //    vec.foreach(println)

    //    val start = System.currentTimeMillis()
    //    val weddate = MyUtils.getFromToday(120)
    //    val url = s"http://master:7979/ServiceWorker?weddate=$weddate&vocationId=d677824c-bae7-11e7-99f0-408d5c0cd580"
    //    val jsonResult: String = postResponse(url)
    //
    //    val arr = jsonResult.split(",\"workerId\":\"").filter(str => {
    //      !str.startsWith("[{\"msisdn\":\"")
    //    }).map(_.substring(0, 36))
    //
    //    arr.foreach(println)
    //
    //    val stop = System.currentTimeMillis()
    //    println((stop - start) / 1000 + "s.")
    //    println(arr.size)
    val vector = Vector[PersonTest](PersonTest("1", "tom", 17),
      PersonTest("2", "jerry", 16),
      PersonTest("3", "老胡", 20))

    val spark = JavaSparkUtils.getInstance().getSparkSession
    import spark.implicits._

    val rdd: RDD[PersonTest] = spark.sparkContext.parallelize(vector)
    val df = rdd.toDF()
    df.show(false)


    df.repartition(1).write.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ",") //默认以","分割
      .option("encoding", "utf-8")
      .save("/Users/hiwes/Downloads/test")

  }


  case class PersonTest(id: String, name: String, age: Int)

  // String转Json
  def str_json(string_json: String): collection.immutable.Map[String, Any] = {
    var first: collection.immutable.Map[String, Any] = collection.immutable.Map()
    val jsonS = scala.util.parsing.json.JSON.parseFull(string_json)
    //不确定数据的类型时，此处加异常判断
    if (jsonS.isInstanceOf[Option[Any]]) {
      first = regJson(jsonS)
    }
    first
  }

  // 通过匹配，获取Json
  def regJson(json: Option[Any]) = json match {
    //转换类型
    case Some(map: collection.immutable.Map[String, Any]) => map
  }

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


  def batchSaveData2SQLServer1(): Unit = {
    val conn = JavaSQLServerConn.getConnection
    conn.setAutoCommit(false)
    val stmt = conn.prepareStatement("insert into canNum_v1_case_dot(hotel_id,hotel_name,section,case_dot,other_condition,can_num,itime) VALUES (?,?,?,?,?,?,getdate())")
    for (j <- 0 to 20000) {
      stmt.setString(1, "测试hotel_id")
      stmt.setString(2, "测试hotel_name")
      stmt.setInt(3, 2)
      stmt.setFloat(4, 0.1f)
      stmt.setString(5, "Vj≠1")
      stmt.setInt(6, 1)
      stmt.addBatch()
    }
    stmt.executeBatch();
    conn.commit
    stmt.close
  }

  def batchSaveData2SQLServer2(): Unit = {
    val conn = JavaSQLServerConn.getConnection
    var i = 0
    //设置批量处理的数量
    val batchSize = 5000
    val stmt = conn.prepareStatement("insert into canNum_v1_case_dot(hotel_id,hotel_name,section,case_dot,other_condition,can_num,itime) VALUES (?,?,?,?,?,?,getdate())")
    conn.setAutoCommit(false)
    for (j <- 0 until 20000) {
      i = i + 1;
      stmt.setString(1, "测试hotel_id")
      stmt.setString(2, "测试hotel_name")
      stmt.setInt(3, 2)
      stmt.setFloat(4, 0.1f)
      stmt.setString(5, "Vj≠1")
      stmt.setInt(6, 1)
      stmt.addBatch()
      if (i % batchSize == 0) {
        stmt.executeBatch
        conn.commit
      }
    }
    if (i % batchSize != 0) {
      stmt.executeBatch
      conn.commit
    }

    JavaSQLServerConn.closeStatement(stmt)
    JavaSQLServerConn.closeConnection(conn)
  }

  def cheackData(): Int = {
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
    val need = allNeedDf
    //      .filter(row => {
    //      row.getAs[String]("hotel_id").equals("c0aa9212-9aed-4801-800d-8c412e972c49")
    //    })
    need.count().toInt
  }


}
