package com.yanjing.foruse

object Test {
  def main(args: Array[String]): Unit = {
    println(Math.exp(0))
    println(Math.exp((0.33 * 0.33) / (-0.0002)))
    println((1 / (Math.sqrt(Math.PI * 2) * (0.01))))

    println((1 / (Math.sqrt(Math.PI * 2) * (0.01))) * Math.exp((0.33 * 0.33) / (-0.0002)))
    println(39.894228040143275 * 6)
  }

}
