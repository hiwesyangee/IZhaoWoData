package com.realleader.foruse

import java.sql.Timestamp

import com.izhaowo.cores.utils.{JavaSparkUtils, MyUtils}
import com.realleader.engine.RealLeaderEngine.spark
import com.realleader.properties.JavaRealLeaderProperties

object Test {
  def main(args: Array[String]): Unit = {
    //    val spark = JavaSparkUtils.getInstance().getSparkSession
    //    import spark.implicits._

    //    var testMap = Map[String, Int]()
    //    testMap = testMap.+("a" -> 1)

    var list = List[String]()
    //    list = list.::("1")
    //    list = list.:::(List("1"))
    //    list= list.:+("1")
    list = list.++(List("1"))

    list.foreach(row => {
      println(row)
    })

  }
}
