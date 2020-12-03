package com.realleaderAndyanjing.client

import java.text.SimpleDateFormat
import java.util.{Date, Timer, TimerTask}

import com.izhaowo.cores.utils.JavaDateUtils
import com.realleaderAndyanjing.engine.RealLeaderAndYanJingEngine
import org.apache.log4j.{Level, Logger}

/**
  * RealLeader和YanJing Client定时任务入口类
  */
object RealLeaderAndYanJingClient {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hbase").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.kafka").setLevel(Level.WARN)
    timerRun()
  }

  def timerRun(): Unit = {
    // 一天的毫秒数
    val daySpan = 24 * 60 * 60 * 1000
    // 规定的每天时间00:30:00运行
    val sdf = new SimpleDateFormat("yyyy-MM-dd 00:30:00") // 每天两点定时执行。

    // 首次运行时间
    try {
      var startTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(sdf.format(new Date))
      // 如果今天的已经过了 首次运行时间就改为明天
      if (System.currentTimeMillis > startTime.getTime) {
        System.out.println("今日时间已过，明天首次运行。")
        startTime = new Date(startTime.getTime + daySpan)
      }
      val t = new Timer
      val task = new TimerTask() {
        override def run(): Unit = { // 0.打印当前时间
          val start = System.currentTimeMillis
          val today = JavaDateUtils.stamp2DateYMD(String.valueOf(start))

          // todo 包含RealLeader和YanJing两部分的数据统计和数据读写。
          //          RealLeaderAndYanJingEngine.realLeaderDataStatistics()
          val stop = System.currentTimeMillis
          RealLeaderAndYanJingEngine.eyesData()
          val stop2 = System.currentTimeMillis

          //          System.out.println(today + " 每日定时任务——RealLeader策划师指标数据关联,用时: " + (stop - start) / 1000 + "s.")
          System.out.println(today + " 每日定时任务——Yanjing可预订策划师数据统计,用时: " + (stop2 - stop) / 1000 + "s.")
        }
      }
      // 以每24小时执行一次
      t.schedule(task, startTime, daySpan)
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }
}
