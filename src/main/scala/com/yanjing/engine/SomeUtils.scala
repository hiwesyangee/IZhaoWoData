package com.yanjing.engine

import java.text.DecimalFormat

import com.izhaowo.cores.utils.MyUtils

import scala.collection.mutable

/**
  * 一些眼睛业务方法的工具类。
  */
object SomeUtils {

  /**
    * 均分数值区间，返回List
    *
    * @param first       起始值
    * @param last        终止值
    * @param granularity 区间个数
    * @param ttype       类型，包含：[],[),(],()
    * @return 区间List
    */
  def getSplitIntervalSetting(first: Double, last: Double, granularity: Int, ttype: String): List[Double] = {
    var list: List[Double] = List()
    val step = (last - first) / (granularity.toDouble)
    for (i <- first to last by step) {
      list = list.+:(i.formatted("%.2f").toDouble)
    }
    list = list.sortWith((x, y) => x < y)
    ttype match {
      case "[]" => list = list
      case "[)" => list = list.dropRight(1)
      case "(]" => list = list.drop(1)
      case "()" => list = list.drop(1).dropRight(1)
      case _ => throw new Exception("Illegal type...")
    }
    list
  }


  /**
    * 均分数值区间，返回Vector
    *
    * @param first       起始值
    * @param last        终止值
    * @param granularity 区间个数
    * @param ttype       类型，包含：[],[),(],()
    * @return 区间List
    */
  def getSplitIntervalSettingVector(first: Double, last: Double, granularity: Int, ttype: String): Vector[Double] = {
    var vec: Vector[Double] = Vector()
    val step = (last - first) / (granularity.toDouble)
    for (i <- first to last by step) {
      vec = vec ++ Vector(i.formatted("%.2f").toDouble)
    }
    vec = vec.sortWith((x, y) => x < y)
    ttype match {
      case "[]" => vec = vec
      case "[)" => vec = vec.dropRight(1)
      case "(]" => vec = vec.drop(1)
      case "()" => vec = vec.drop(1).dropRight(1)
      case _ => throw new Exception("Illegal type...")
    }
    vec
  }

  /**
    * 统计区间内元素个数，指定保留两位.
    *
    * @param dataArr 待统计元素list
    * @param inArr   区间list
    * @return
    */
  def getRightIntervalBySetting(dataArr: List[Double], inArr: List[Double]) = {
    var map = Map[String, Int]()
    for (inData: Double <- inArr) {
      for (data: Double <- dataArr) {
        if (!inData.equals(inArr.max)) {
          if (data >= inData && data < inArr(inArr.indexOf(inData) + 1)) {
            if (!map.keySet.contains(inData.formatted("%.2f"))) {
              map = map.+(inData.formatted("%.2f") -> 1)
            } else {
              map = map.+(inData.formatted("%.2f") -> (map.get(inData.formatted("%.2f")).get + 1))
            }
          }
        } else {
          if (data >= inData) {
            if (!map.keySet.contains(inData.formatted("%.2f"))) {
              map = map.+(inData.formatted("%.2f") -> 1)
            } else {
              map = map.+(inData.formatted("%.2f") -> (map.get(inData.formatted("%.2f")).get + 1))
            }
          }
        }
      }
      if (!map.keySet.contains(inData.formatted("%.2f"))) { // 将循环放在一层嵌套内，将时间进行缩减，一层内处理完数据直接进行验证。
        map = map.+(inData.formatted("%.2f") -> 0)
      }
    }
    map
  }


  /**
    * 统计区间内元素个数，指定保留两位.
    *
    * @param dataArr 待统计元素list
    * @param inArr   区间list
    * @return
    */
  def getRightIntervalBySetting2(dataArr: List[Double], inArr: List[Double]) = {
    var map = Map[Double, Int]()
    for (inData: Double <- inArr) {
      map = map.+(inData -> 0)
    }
    for (data: Double <- dataArr) {
      val realData = data.formatted("%.2f").toDouble
      val oldValue: Int = map.get(realData).get
      map = map.+(realData -> (oldValue + 1))
    }
    map
  }

  /**
    * 使用可变Map进行统计区间元素个数，指定保留2位。【指向非服务费的指标】
    *
    * @param dataArr 待统计元素Vector
    * @param inArr   区间Vector
    * @return
    */
  def getRightIntervalBySetting22(dataArr: Vector[Double], inArr: Vector[Double]) = {
    val map = mutable.HashMap[Double, Int]()
    for (inData: Double <- inArr) {
      map.+=(inData -> 0)
    }
    for (data: Double <- dataArr) {
      val realData = data.formatted("%.2f").toDouble
      val oldValue: Int = map.get(realData).get
      map.+=(realData -> (oldValue + 1))
    }
    map
  }

  /**
    * 统计区间内元素个数，指定保留两位.
    *
    * @param dataArr 待统计元素list
    * @param inArr   区间list
    * @return
    */
  def getRightIntervalBySetting3(dataArr: List[Double], inArr: List[Double]) = {
    var map = Map[Double, Int]()
    for (inData: Double <- inArr) {
      map = map.+(inData -> 0)
    }
    for (data: Double <- dataArr) {
      val realData = data - (data % 50)
      val oldValue: Int = map.get(realData).get
      map = map.+(realData -> (oldValue + 1))
    }
    map
  }

  /**
    * 使用可变Map进行统计区间元素个数，指定保留2位。【指向服务费的指标】
    *
    * @param dataArr 待统计元素Vector
    * @param inArr   区间Vector
    * @return
    */
  def getRightIntervalBySetting33(dataArr: Vector[Double], inArr: Vector[Double]) = {
    val map = mutable.HashMap[Double, Int]()
    for (inData: Double <- inArr) {
      map.+=(inData -> 0)
    }
    for (data: Double <- dataArr) {
      val realData = data - (data % 50)
      val oldValue: Int = map.get(realData).get
      map.+=(realData -> (oldValue + 1))
    }
    map
  }

  /**
    * 统计区间内元素个数，指定保留两位.
    *
    * @param dataArr 待统计元素list
    * @param inArr   区间list
    * @return
    */
  //  def getRightIntervalBySetting(dataArr: List[Double], inArr: List[Double], digit: Int) = {
  //    var map = Map[String, Int]()
  //    for (inData: Double <- inArr) {
  //      for (data: Double <- dataArr) {
  //        val index = inArr.indexOf(inData)
  //        if (index > 0) {
  //          if (data < inData && data >= inArr(inArr.indexOf(inData) - 1)) {
  //            if (!map.keySet.contains(inData.formatted("%." + digit.toString + "f"))) {
  //              map = map.+(inData.formatted("%." + digit.toString + "f") -> 1)
  //            } else {
  //              map = map.+(inData.formatted("%." + digit.toString + "f") -> (map.get(inData.formatted("%." + digit.toString + "f")).get + 1))
  //            }
  //          }
  //        } else if (index == 0) {
  //          if (data <= inData) {
  //            if (!map.keySet.contains(inData.formatted("%." + digit.toString + "f"))) {
  //              map = map.+(inData.formatted("%." + digit.toString + "f") -> 1)
  //            } else {
  //              map = map.+(inData.formatted("%." + digit.toString + "f") -> (map.get(inData.formatted("%." + digit.toString + "f")).get + 1))
  //            }
  //          }
  //        }
  //      }
  //      if (!map.keySet.contains(inData.formatted("%." + digit.toString + "f"))) { // 将循环放在一层嵌套内，将时间进行缩减，一层内处理完数据直接进行验证。
  //        map = map.+(inData.formatted("%." + digit.toString + "f") -> 0)
  //      }
  //    }
  //    map
  //  }
  def getRightIntervalBySetting(dataArr: List[Double], inArr: List[Double], digit: Int) = {
    var map = Map[String, Int]()
    for (inData: Double <- inArr) {
      for (data: Double <- dataArr) {
        if (!inData.equals(inArr.max)) {
          if (data >= inData && data < inArr(inArr.indexOf(inData) + 1)) {
            if (!map.keySet.contains(inData.formatted("%." + digit.toString + "f"))) {
              map = map.+(inData.formatted("%." + digit.toString + "f") -> 1)
            } else {
              map = map.+(inData.formatted("%." + digit.toString + "f") -> (map.get(inData.formatted("%." + digit.toString + "f")).get + 1))
            }
          }
        } else {
          if (data >= inData) {
            if (!map.keySet.contains(inData.formatted("%." + digit.toString + "f"))) {
              map = map.+(inData.formatted("%." + digit.toString + "f") -> 1)
            } else {
              map = map.+(inData.formatted("%." + digit.toString + "f") -> (map.get(inData.formatted("%." + digit.toString + "f")).get + 1))
            }
          }
        }
      }
      if (!map.keySet.contains(inData.formatted("%." + digit.toString + "f"))) { // 将循环放在一层嵌套内，将时间进行缩减，一层内处理完数据直接进行验证。
        map = map.+(inData.formatted("%." + digit.toString + "f") -> 0)
      }
    }
    map
  }

  def main(args: Array[String]): Unit = {
    //    val dataArr: Vector[Double] = Vector(-1.0d, -0.99424d, -0.01412d, 0d, 0.01124d, 0.11124d, 0.99222d, 1d)
    //    val inArr: Vector[Double] = getSplitIntervalSettingVector(-1d, 1d, 200, "[]")
    //    for (i <- 1 to 196) {
    //      val map: mutable.HashMap[Double, Int] = getRightIntervalBySetting22(dataArr, inArr)
    //      map.foreach(m => {
    //        println(m._1 + "===>" + m._2)
    //      })
    //    }
    val data = 0.222222d
    val data2 = 0.2255555d
    val realData = data.formatted("%.2f").toDouble
    val realData2 = data2.formatted("%.2f").toDouble
    println(realData)
    println(realData2)
  }
}
