package com.sida.cores.properties

object SidaProperties {

  // mysql参数【寇耀数据库】
  // 外网
  //  val MYSQLURL = "jdbc:mysql://115.29.45.77:3306/izhaowo_worker?useUnicode=true&characterEncoding=UTF8";
  // 内网
  val MYSQLURL = "jdbc:mysql://10.80.185.36:3306/izhaowo_worker?useUnicode=true&characterEncoding=UTF8"

  val MYSQLUSER = "test_izw_2019"
  val MYSQLPASSWORD = "izw(5678)!#$%^[ABCabc]9587"

  // planner_sida。rowkey ----> planner=0-999.   进行非同累加
  val PLANNERSIDA4HBASE = "planner_sida"
  val cfsOfPLANNERSIDA4HBASE = Array("info")
  val columnsOfPLANNERSIDA4HBASE = Array("flag", "sot", "hid", "hname", "aid", "aname", "cid", "cname", "pid", "pname", "planner_id", "planner_name") // 主持、化妆、摄像、摄影、策划

  // sida_rank。rowkey ----> worker_id
  val SIDARANK4HBASE = "sida_rank"
  val cfsOfSIDARANK4HBASE = Array("info")
  val columnsOfSIDARANK4HBASE = Array("name", "sot", "flag", "worker_id","longitude", "latitude", "lonAndLat")

}
