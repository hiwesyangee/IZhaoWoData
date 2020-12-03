package com.izhaowo.cores.foruse

import java.util

import redis.clients.jedis.Jedis

object RedisTest {
  def main(args: Array[String]): Unit = {
    testRedis4
  }

  def testRedis1(): Unit = {
    val jedis = new Jedis("localhost")
    println("连接成功.")
    // 查看服务是否运行
    println("服务正在运行: " + jedis.ping())
  }

  def testRedis2(): Unit = {
    val jedis = new Jedis("localhost")
    println("连接成功.")

    //设置 redis 字符串数据
    jedis.set("mykey", "www.izhaowo.com")

    // 获取存储的数据并输出
    println("redis 存储的字符串为: " + jedis.get("mykey"))
  }

  def testRedis3(): Unit = {
    //连接本地的 Redis 服务//连接本地的 Redis 服务
    val jedis = new Jedis("localhost")
    println("连接成功")
    //存储数据到列表中
    jedis.lpush("tutorial-list", "Redis")
    jedis.lpush("tutorial-list", "Mongodb")
    jedis.lpush("tutorial-list", "Mysql")
    // 获取存储的数据并输出
    val list = jedis.lrange("tutorial-list", 0, 2).toArray()
    for (i <- list) {
      println("列表项为: " + i)
    }
  }

  def testRedis4(): Unit = {
    //连接本地的 Redis 服务//连接本地的 Redis 服务
    val jedis = new Jedis("localhost")
    println("连接成功")

    // 获取数据并输出
    val keys: util.Set[String] = jedis.keys("*")
    val it = keys.iterator
    while (it.hasNext) {
      val key = it.next();
      System.out.println("key: " + key);
    }
  }

}
