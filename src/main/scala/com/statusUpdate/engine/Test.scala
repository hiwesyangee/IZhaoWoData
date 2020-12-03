package com.statusUpdate.engine

import com.izhaowo.cores.utils.{JavaHBaseUtils, JavaMongoDBUtils, JavaSparkUtils}
import com.mongodb.client.{FindIterable, MongoCollection, MongoCursor, MongoDatabase}
import com.mongodb.client.model.Filters
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.bson.Document

/**
  * 读取所有的MongoDB数据库中的均衡小市场。
  *
  * @author hiwes at 08/24
  */
object Test {
  def main(args: Array[String]): Unit = {
    val mondoDatabase = JavaMongoDBUtils.getInstance.getMongoDB
    val collection = mondoDatabase.getCollection("small_market_production")
    val bson = new Document
    bson.put("small_market_name", 1)
    bson.put("status2", 1)
    val mongoCursor = collection.find(Filters.eq("status2", 1)).projection(bson).iterator
    var i = 0

    val map = Test2.getAllHotelIdAndSMid()

    var list = List[String]()
    while (mongoCursor.hasNext()) { //
      val a = mongoCursor.next()
      val b = a.toJson()

      val inArr = b.split(": \"")
      val smid = inArr(1).split("_")(0)
      val result: String = inArr(2).split("\"")(0)
      val hotel_id = map.get(smid)
      val end = new String(hotel_id + " " + result)
      println(end)
      list = list.++(List(end))
      i += 1
    }
    println(list.size)
    println("i = " + i)

    val spark = JavaSparkUtils.getInstance().getSparkSession
    import spark.implicits._

    val rdd: RDD[String] = spark.sparkContext.parallelize(list)
    val dataFrame: DataFrame = rdd.map(x => {
      val arr = x.split(" ")
      val hotel_id = map.get(arr(0))
      println(hotel_id + " " + arr(1) + " " + arr(3))
      (hotel_id, arr(1), arr(3))
    }).toDF("hotel_id", "hotel_name", "section")
    save(dataFrame)
  }

  def save(dataFrame: DataFrame): Unit = {
    dataFrame.repartition(1).write.format("com.databricks.spark.csv")
      .option("header", "true")
      //      .option("delimiter", " ") //默认以","分割
      .option("encoding", "utf-8")
      .save("/opt/modelvalidation/SimilarityCSV/result")
  }

}
