package com.bangbang.cores.foruse

import com.izhaowo.cores.utils.JavaSparkUtils

object TestScala {

  def main(args: Array[String]): Unit = {
    val sc =JavaSparkUtils.getInstance().getSparkSession.sparkContext

    val rdd = sc.parallelize(Array(1,2,3,4,5))

    val acu = sc.longAccumulator("Accu")
    rdd.foreach(row =>{
      acu.add(row)
    })

    println(acu.value)
    acu.reset()
    println(acu.value)

  }
}
