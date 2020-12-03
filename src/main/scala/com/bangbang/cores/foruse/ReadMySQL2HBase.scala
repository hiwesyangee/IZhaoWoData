package com.bangbang.cores.foruse

import java.util

import com.bangbang.cores.engine.BangBangStreamingDataProcessEngine
import com.bangbang.cores.properties.JavaBangBangProperties
import com.izhaowo.cores.utils.{JavaHBasePoolUtils, JavaSparkUtils}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes

/**
  * 读取MYSQL数据库3张表到HBase
  */
object ReadMySQL2HBase {
  def main(args: Array[String]): Unit = {
    val start = System.currentTimeMillis()
    val spark = JavaSparkUtils.getInstance().getSparkSession
    import spark.implicits._

    // 读取izhaowo_user_wedding下的tb_planner_recom_record
    val tb_User_Df = spark.read.format("jdbc")
      .option("url", JavaBangBangProperties.MYSQLURLTBUSERWEDDING)
      .option("dbtable", JavaBangBangProperties.tb_planner_recom_record)
      .option("user", JavaBangBangProperties.MYSQLUSER14)
      .option("password", JavaBangBangProperties.MYSQLPASSWORD14)
      .load()
    val endDF = tb_User_Df.filter(row => {
      row.getAs[Int]("type") == 0 &&
        row.getAs[String]("wedding_id").length > 0
    })

    endDF.na.fill("null").cache() // 空值补全

    endDF.foreachPartition(ite => {
      ite.foreach(row => {
        val wedding_id = row.getAs[String]("wedding_id")
        val put = new Put(Bytes.toBytes(row.getAs[String]("id")))
        for (i <- 1 to row.length - 1) {
          val value = row.get(i)
          var end: String = "" // 使用中间对象，代替value的取值
          if (value != null && !value.equals("null")) { // 当原值不为空，"并且" 不等于null补全的，才进行转型
            end = value.toString
          }
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(JavaBangBangProperties.columnsOfTBPLANNERRECOMRECORD(i - 1)), Bytes.toBytes(end))
        }
        var hotel_id = ""
        if (wedding_id != null && !wedding_id.equals("null")) {
          hotel_id = BangBangStreamingDataProcessEngine.getWeddingHotel(wedding_id)
        }
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("hotel_id"), Bytes.toBytes(hotel_id))
        val list = new util.ArrayList[Put]()
        list.add(put)
        JavaHBasePoolUtils.putRows(JavaBangBangProperties.TBPLANNERRECOMRECORD, list)
      })
    })
    endDF.unpersist()
    JavaHBasePoolUtils.closePool()

    val stop = System.currentTimeMillis()
    println("use time: " + (stop - start) / 1000 + "s.")
  }
}
