package com.izhaowo.cores.foruse

import java.util

import com.izhaowo.cores.properties.JavaXianChiFanProperties
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

    // 读取izhaowo_crm下的tb_user
    val tb_User_Df = spark.read.format("jdbc")
      .option("url", JavaXianChiFanProperties.MYSQLURLTBCRM)
      .option("dbtable", JavaXianChiFanProperties.tb_user)
      .option("user", JavaXianChiFanProperties.MYSQLUSER13)
      .option("password", JavaXianChiFanProperties.MYSQLPASSWORD13)
      .load()
    tb_User_Df.na.fill("null") // 空值补全
    tb_User_Df.cache()
    tb_User_Df.foreachPartition(ite => {
      ite.foreach(row => {
        val put = new Put(Bytes.toBytes(row.getAs[String](0)))
        for (i <- 1 to row.length - 1) {
          val value = row.get(i)
          var end: String = "" // 使用中间对象，代替value的取值
          if (value != null && !value.equals("null")) { // 当原值不为空，"并且" 不等于null补全的，才进行转型
            end = value.toString
          }
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(JavaXianChiFanProperties.columnsOfTBUSER(i - 1)), Bytes.toBytes(end))
        }
        val list = new util.ArrayList[Put]()
        list.add(put)
        JavaHBasePoolUtils.putRows(JavaXianChiFanProperties.TBUSER, list)
      })
    })
    tb_User_Df.unpersist()

    // 读取izhaowo_user_wedding下的tb_hotel
    val tb_Hotel_Df = spark.read.format("jdbc")
      .option("url", JavaXianChiFanProperties.MYSQLURLTBUSERWEDDING)
      .option("dbtable", JavaXianChiFanProperties.tb_hotel)
      .option("user", JavaXianChiFanProperties.MYSQLUSER13)
      .option("password", JavaXianChiFanProperties.MYSQLPASSWORD13)
      .load()
    tb_Hotel_Df.na.fill("null") // 空值补全
    tb_Hotel_Df.cache()
    tb_Hotel_Df.foreachPartition(ite => {
      ite.foreach(row => {
        val put = new Put(Bytes.toBytes(row.getAs[String](0)))
        for (i <- 1 to row.length - 1) {
          val value = row.get(i)
          var end: String = "" // 使用中间对象，代替value的取值
          if (value != null && !value.equals("null")) { // 当原值不为空，"并且" 不等于null补全的，才进行转型
            end = value.toString
          }
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(JavaXianChiFanProperties.columnsOfTBHOTEL(i - 1)), Bytes.toBytes(end))
        }
        val list = new util.ArrayList[Put]()
        list.add(put)
        JavaHBasePoolUtils.putRows(JavaXianChiFanProperties.TBHOTEL, list)
      })
    })
    tb_Hotel_Df.unpersist()

    // 读取izhaowo_user_wedding下的tb_user_wedding
    val tb_User_Wedding_Df = spark.read.format("jdbc")
      .option("url", JavaXianChiFanProperties.MYSQLURLTBUSERWEDDING)
      .option("dbtable", JavaXianChiFanProperties.tb_user_wedding)
      .option("user", JavaXianChiFanProperties.MYSQLUSER13)
      .option("password", JavaXianChiFanProperties.MYSQLPASSWORD13)
      .load()
    tb_User_Wedding_Df.na.fill("null") // 空值补全
    tb_User_Wedding_Df.cache()
    tb_User_Wedding_Df.foreachPartition(ite => {
      ite.foreach(row => {
        val put = new Put(Bytes.toBytes(row.getAs[String](0)))
        for (i <- 1 to row.length - 1) {
          val value = row.get(i)
          var end: String = "" // 使用中间对象，代替value的取值
          if (value != null && !value.equals("null")) { // 当原值不为空，"并且" 不等于null补全的，才进行转型
            end = value.toString
          }
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(JavaXianChiFanProperties.columnsOfTBUSERWEDDING(i - 1)), Bytes.toBytes(end))
        }
        val list = new util.ArrayList[Put]()
        list.add(put)
        JavaHBasePoolUtils.putRows(JavaXianChiFanProperties.TBUSERWEDDING, list)
      })
    })
    tb_User_Wedding_Df.unpersist()
    JavaHBasePoolUtils.closePool()

    val stop = System.currentTimeMillis()
    println("use time: " + (stop - start) / 1000 + "s.")
  }
}
