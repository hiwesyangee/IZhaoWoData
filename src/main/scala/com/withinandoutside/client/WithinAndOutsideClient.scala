package com.withinandoutside.client

import java.text.SimpleDateFormat
import java.util.{Date, Timer, TimerTask}

import com.izhaowo.cores.utils.JavaDateUtils
import com.withinandoutside.engine.WithinAndOutsideEngine
import org.apache.log4j.{Level, Logger}

/**
 * 更新域内域外市场指标-client端,定时任务调用engine类
 *
 * by hiwes since 2020/11/16
 */
object WithinAndOutsideClient {

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
    val sdf = new SimpleDateFormat("yyyy-MM-dd 01:00:00") // 每天1点定时执行。

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

          System.out.println(today + " 每日定时任务——更新域内域外市场指标, 开始.")
          val stop = System.currentTimeMillis

          // todo 更新域内市场指标
          WithinAndOutsideEngine.startSaveWithinAndOutsideIndicatorsAll()

          val stop2 = System.currentTimeMillis
          System.out.println(today + " 每日定时任务——更新域内域外市场指标, 用时: " + (stop2 - stop) / 1000 + "s.")
        }
      }
      // 以每24小时执行一次
      t.schedule(task, startTime, daySpan)
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }
}
