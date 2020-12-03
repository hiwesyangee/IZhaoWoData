package com.xianchifan.cores.engine

import java.util

import com.izhaowo.cores.utils.JavaHBaseUtils
import com.xianchifan.cores.properties.JavaXianChiFanPlannerScheduleProperties
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes

/**
  * Xianchifan项目数据处理类
  *
  * @version 1.6
  * @since 2019/07/04 by Hiwes
  */
object XianChiFanPlannerScheduleStreamingDataProcessEngine {
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
        if (arr.length == 8) { // tb_worker_schedule数据存储.
          saveTbWorkerScheduleData2HBase(arr)
          println("tb_worker_schedule数据传输:")
          println(line)
        } else if (arr.length > 15) { // tb_worker数据存储.
          saveTbWorkerData2HBase(arr)
          println("tb_worker数据传输:")
          println(line)
        }
      }
    }
  }

  /**
    * tb_worker_schedule数据写入
    * 包含mapid查询
    *
    * @param arr
    */
  def saveTbWorkerScheduleData2HBase(arr: Array[String]): Unit = {
    try {
      val put = new Put(Bytes.toBytes(arr(0)))
      for (i <- 0 to arr.length - 2) { //0___7-2=5
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(JavaXianChiFanPlannerScheduleProperties.columnsOfTBWORKERSCHEDULE(i)), Bytes.toBytes(arr(i + 1)));
      }
      val list = new util.ArrayList[Put]()
      list.add(put)
      JavaHBaseUtils.putRows(JavaXianChiFanPlannerScheduleProperties.TBWORKERSCHEDULE, list)
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  /**
    * tb_worker数据写入____只存储部分数据
    * 包含mapid查询
    *
    * @param arr
    */
  def saveTbWorkerData2HBase(arr: Array[String]): Unit = {
    try {
      val put = new Put(Bytes.toBytes(arr(0)))
      for (i <- 0 to arr.length - 2) { // 0___17-2=15
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(JavaXianChiFanPlannerScheduleProperties.columnsOfTBWORKER(i)), Bytes.toBytes(arr(i + 1)));
      }
      val list = new util.ArrayList[Put]()
      list.add(put)
      JavaHBaseUtils.putRows(JavaXianChiFanPlannerScheduleProperties.TBWORKER, list)
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  def main(args: Array[String]): Unit = {
  }

}
