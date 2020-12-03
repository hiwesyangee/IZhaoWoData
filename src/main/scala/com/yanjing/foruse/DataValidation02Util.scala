package com.yanjing.foruse

object DataValidation02Util {

  // 异化V3中的最大值计算。
  def getV3MaxV(list: List[Double]): Double = {
    var max = 0d
    var resultL = 0d
    for (l <- list) {
      val en = (1 / (0.014 * Math.sqrt(2 * Math.PI))) * (Math.exp(Math.pow(l + 0.34, 2) / (-0.000392)))
      if (en >= max) {
        max = en
        resultL = l
      }
    }
    resultL
  }


  def main(args: Array[String]): Unit = {
    val list = List[Double](-1d, -0.9d, -0.8d, -0.7d, -0.6d, -0.5d, -0.4d, -0.3d, -0.2d, -0.1d, 0, 0.1d, 0.2d, 0.3d, 0.4d, 0.5d, 0.6d, 0.7d, 0.8d, 0.9d, 1d)
    val en = getV3MaxV(list)
    println(en)
  }

}
