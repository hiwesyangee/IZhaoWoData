package com.izhaowo.cores.properties

/**
  * 全新XianChiFanProperties常数类
  *
  * @version 2.0
  * @since 2019/06/17 by Hiwes
  */
object XianChiFanProperties {
  // Spark相关配置
  val APPNAME = "IZhaowoBangBangData_1.0.0"
  val MASRTERNAME = "local[4]"
  val PARALLELISM = "500"
  val SERIALIZER = "org.apache.spark.serializer.KryoSerializer"
  //  val CHECKPOINTPATH = "hdfs://master:8020/opt/checkpoint2/"
  // 本机测试用
  val CHECKPOINTPATH = "hdfs://hiwes:8020/opt/checkpoint2/"

  // HBase相关配置
  //    val ZKQUORUM = "master:2181"
  val ZKQUORUM = "hiwes:2181"

  // Kafka相关配置
  val StringTOPIC = Array("xianchifan", "test")
  val GROUP = "flume_kafka_streaming_group"
  // val BOOTSTARP_SERVERS = "master:9092"
  // 本机测试用
  val BOOTSTARP_SERVERS = "hiwes:9092"

  // MySQL数据库连接URL
  val MYSQLURL = "jdbc:mysql://115.29.45.77:3306/izhaowo_worker"

  // MySQL数据库连接user
  val MYSQLUSER = "test_izw_2019"
  // MySQL数据库连接password
  val MYSQLPASSWORD = "izw(5678)!#$%^[ABCabc]9587"

  // MySQL元数据表planner_sida及对应存储路径
  val PLANNER_SIDA = "planner_sida"
  val PLANNER_SIDAPATH = "hdfs://hiwes:8020/opt/data/bangbang/planner_sida/"
  // MySQL元数据表sida_rank及对应存储路径
  val SIDA_RANK = "sida_rank"
  val SIDA_RANKPATH = "hdfs://hiwes:8020/opt/data/bangbang/sida_rank/"

  // SQLServer数据库连接URL
  val SQLSERVERURL = "jdbc:sqlserver://121.42.61.195:1433;databaseName=izhaowoDataCenter"
  // SQLServer数据库连接user
  val SQLSERVERUSER = "test"
  // SQLServer数据库连接password
  val SQLSERVERPASSWORD = "tes@t123456#pwd2018&"

  // SQLServer元数据表tb_worker_service_info及对应存储路径
  val WORKERSERVICEINFO = "tb_worker_service_info"
  val WORKERSERVICEINFOPATH = "hdfs://hiwes:8020/opt/data/bangbang/worker_service/"

  // HBase数据库表相关
  val PLANNERSIDA4HBASE = "planner_sida"

  val cfsOfPLANNERSIDA4HBASE = Array("info")
  val columnsOfPLANNERSIDA4HBASE = Array("sot", "pid", "pname", "cid", "cname", "aid", "aname", "hid", "hname")

  val SIDARANK4HBASE = "sida_rank"
  val cfsOfSIDARANK4HBASE = Array("info")
  val columnsOfSIDARANK4HBASE = Array("name", "sot", "flag")

  val WORKERSERVICEINFO4HBASE = "worker_service_info"
  val cfsOfWORKERSERVICEINFO4HBASE = Array("info")
  val columnsOfWORKERSERVICEINFO4HBASE = Array("province", "flag", "service_id", "service_name", "service_amount")

}
