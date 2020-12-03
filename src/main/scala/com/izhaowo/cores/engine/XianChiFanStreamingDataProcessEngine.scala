package com.izhaowo.cores.engine

import java.util

import com.izhaowo.cores.properties.JavaXianChiFanProperties
import com.izhaowo.cores.utils.JavaHBaseUtils
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes

/**
  * 爱找我————先吃饭stream数据处理逻辑类
  *
  * @version 1.3
  * @since 2019/06/18 by Hiwes
  */
object XianChiFanStreamingDataProcessEngine {
  /**
    * 针对不同的数据进行处理
    *
    * @param line
    */
  def DBContentsDispose(line: String): Unit = {
    if (line != null) {
      if (line.equals("ceshi")) {
        println(line)
      } else {
        val arr: Array[String] = line.replaceAll("\"", "").split(",")
        if (arr.length == 24 || arr.length == 21) { // tb_user方法调用
          saveTbUserData2HBase(arr)
          println("tb_user数据传输:")
          println(line)
        } else if (arr.length == 11) { // tb_hotel方法调用
          saveTbHotelData2HBase(arr)
          println("tb_hotel数据传输:")
          println(line)
        } else if (arr.length == 22) { // tb_user_wedding方法调用
          // 查询本条数据是否重复
          val result = JavaHBaseUtils.getValue(JavaXianChiFanProperties.TBUSERWEDDING, arr(0), "info", "user_id")
          if (result == null) {
            saveTbUserWeddingData2HBase(arr)
            println("tb_user_wedding数据传输:")
            println(line)
          }
        }
      }
    }
  }

  /**
    * tb_user数据写入
    *
    * @param arr
    */
  def saveTbUserData2HBase(arr: Array[String]): Unit = {
    val put = new Put(Bytes.toBytes(arr(0)))
    for (i <- 1 to arr.length - 1) {
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(JavaXianChiFanProperties.columnsOfTBUSER(i - 1)), Bytes.toBytes(arr(i)));
    }
    val list = new util.ArrayList[Put]()
    list.add(put)
    JavaHBaseUtils.putRows(JavaXianChiFanProperties.TBUSER, list)
  }

  /**
    * tb_hotel数据写入
    *
    * @param arr
    */
  def saveTbHotelData2HBase(arr: Array[String]): Unit = {
    val put = new Put(Bytes.toBytes(arr(0)))
    for (i <- 1 to arr.length - 1) {
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(JavaXianChiFanProperties.columnsOfTBHOTEL(i - 1)), Bytes.toBytes(arr(i)));
    }
    val list = new util.ArrayList[Put]()
    list.add(put)
    JavaHBaseUtils.putRows(JavaXianChiFanProperties.TBHOTEL, list)
  }

  /**
    * tb_user_wedding数据写入
    *
    * @param arr
    */
  def saveTbUserWeddingData2HBase(arr: Array[String]): Unit = {
    val put = new Put(Bytes.toBytes(arr(0)))
    for (i <- 1 to arr.length - 1) {
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(JavaXianChiFanProperties.columnsOfTBUSERWEDDING(i - 1)), Bytes.toBytes(arr(i)));
    }
    val list = new util.ArrayList[Put]()
    list.add(put)
    JavaHBaseUtils.putRows(JavaXianChiFanProperties.TBUSERWEDDING, list)
  }

  /**
    * 获取婚礼酒店id
    *
    * @param wedding_id
    * @return
    */
  def getWeddingHotel(wedding_id: String): String = JavaHBaseUtils.getValue(JavaXianChiFanProperties.TBUSERWEDDING, wedding_id, "info", "hotel_id")

}
