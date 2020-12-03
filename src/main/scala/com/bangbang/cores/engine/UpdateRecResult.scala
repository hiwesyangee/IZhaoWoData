package com.bangbang.cores.engine

import java.util.Timer

/**
  * 全新1.5版本的Timmer类
  */
object UpdateRecResult {
  def timeMaker(): Unit = {
    val timer = new Timer
    // 启动20s之后执行，之后周期1h循环执行
    //        timer.schedule(TimmerTask, 8 * 60 * 60 * 1000, 24 * 60 * 60 * 1000)
    timer.schedule(TimmerTask, 1 * 20 * 1000, 24 * 60 * 60 * 1000)

    /** 在凌晨修改为24小时一次 */
  }
}
