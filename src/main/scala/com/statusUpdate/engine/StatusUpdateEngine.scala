package com.statusUpdate.engine

import com.izhaowo.cores.utils.MyUtils
import com.yanjing.foruse.DataValidationUtils

object StatusUpdateEngine {
  def main(args: Array[String]): Unit = {
    startStatusSaveAndUpdate()
  }

  def startStatusSaveAndUpdate(): Unit = {
    val today = MyUtils.getFromToday(0)
    val url = s"http://hadoop002:8381/newCheck?day=$today"
    val end: String = DataValidationUtils.postResponse(url)
    println(end)
    //    println(DataValidationUtils.postResponse("http://hadoop002:8381/hello"))
  }
}
