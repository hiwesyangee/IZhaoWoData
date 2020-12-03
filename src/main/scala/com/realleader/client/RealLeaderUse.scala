package com.realleader.client

import com.izhaowo.cores.utils.JavaDateUtils
import com.realleader.engine.RealLeaderEngine

/**
  * 立即执行小市场策划师指标表数据读写过程。
  * by hiwes 2019/12/24
  */
object RealLeaderUse {

  def main(args: Array[String]): Unit = {
    val start = System.currentTimeMillis
    val today = JavaDateUtils.stamp2DateYMD(String.valueOf(start))

    // 开始记录任务时长
//    RealLeaderEngine.readAndWriteRealLeaderData2HBase()
    RealLeaderEngine.readAndWriteRealLeaderOldData2HBase()

    val stop = System.currentTimeMillis
    System.out.println(today + " 每日定时任务——读写小市场策划师指标表,用时: " + (stop - start) / 1000 + "s.")
  }
}
