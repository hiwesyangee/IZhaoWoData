package com.izhaowo.cores.properties;

/**
 * 全新JavaXianChiFanProperties常数类，Java版本
 *
 * @version 2.0
 * @since 2019/06/17 by Hiwes
 */
public class JavaXianChiFanProperties {

    // Spark相关配置
    public static final String APPNAME = "IZhaowoBangBangData_1.0.0";
    public static final String MASRTERNAME = "local[2]";
    public static final String PARALLELISM = "500";
    public static final String SERIALIZER = "org.apache.spark.serializer.KryoSerializer";
//    public static final String CHECKPOINTPATH = "hdfs://master:8020/opt/checkpoint2/";

    // HBase相关配置
    public static final String ZKQUORUM = "master:2181";
    // 本机测试用
//    public static final String ZKQUORUM = "hiwes:2181";

    // MySQL数据库连接URL
//    public static final String MYSQLURL = "jdbc:mysql://115.29.45.77:3306/izhaowo_worker";
    public static final String MYSQLURL = "jdbc:mysql://10.80.185.36:3306/izhaowo_worker?useUnicode=true&characterEncoding=UTF8";

    // MySQL数据库连接user
    public static final String MYSQLUSER = "test_izw_2019";
    // MySQL数据库连接password
    public static final String MYSQLPASSWORD = "izw(5678)!#$%^[ABCabc]9587";

    // MySQL元数据表planner_sida及对应存储路径
    public static final String PLANNER_SIDA = "planner_sida";
    public static final String PLANNER_SIDAPATH = "hdfs://master:8020/opt/data/bangbang/planner_sida/";
    // MySQL元数据表sida_rank及对应存储路径
    public static final String SIDA_RANK = "sida_rank";
    public static final String SIDA_RANKPATH = "hdfs://master:8020/opt/data/bangbang/sida_rank/";

    // SQLServer数据库连接URL
    public static final String SQLSERVERURL = "jdbc:sqlserver://121.42.61.195:1433;databaseName=izhaowoDataCenter";
    // SQLServer数据库连接user
    public static final String SQLSERVERUSER = "test";
    // SQLServer数据库连接password
    public static final String SQLSERVERPASSWORD = "tes@t123456#pwd2018&";

    // SQLServer元数据表tb_worker_service_info及对应存储路径
    public static final String WORKERSERVICEINFO = "tb_worker_service_info";
    public static final String WORKERSERVICEINFOPATH = "hdfs://master:8020/opt/data/bangbang/worker_service/";

    // HBase数据库表相关——————1.2版本（2019.06.06）
    public static final String PLANNERSIDA4HBASE = "planner_sida"; // :::
    public static final String[] cfsOfPLANNERSIDA4HBASE = {"info"};
    public static final String[] columnsOfPLANNERSIDA4HBASE = {"sot", "pid", "pname", "cid", "cname", "aid", "aname", "hid", "hname"};

    public static final String SIDARANK4HBASE = "sida_rank";
    public static final String[] cfsOfSIDARANK4HBASE = {"info"};
    public static final String[] columnsOfSIDARANK4HBASE = {"name", "sot", "flag"};

    public static final String WORKERSERVICEINFO4HBASE = "worker_service_info"; // :::
    public static final String[] cfsOfWORKERSERVICEINFO4HBASE = {"info"};
    public static final String[] columnsOfWORKERSERVICEINFO4HBASE = {"province", "flag", "service_id", "service_name", "service_amount"};

    // HBase数据库表相关——————1.3版本（2019.06.18）
    public static final String TBUSER = "tb_user"; // rowkey:id
    public static final String[] cfsOfTBUSER = {"info"};
    public static final String[] columnsOfTBUSER = {"name", "province", "city", "wechat", "msisdn", "wedding_date", "address", "counselor", "remark", "first_channel", "second_channel", "ctime", "utime", "is_delete", "crm_center_key", "budget_min", "budget_max", "tag1", "tag2", "old", "hotel_name", "hotel_id", "uid"};//23+1

    public static final String TBHOTEL = "tb_hotel"; // rowkey:id
    public static final String[] cfsOfTBHOTEL = {"info"};
    public static final String[] columnsOfTBHOTEL = {"amap_id", "name", "address", "province", "city", "zone", "longitude", "latitude", "ctime", "utime"}; //10+1

    public static final String TBUSERWEDDING = "tb_user_wedding"; // rowkey:id
    public static final String[] cfsOfTBUSERWEDDING = {"info"};
    public static final String[] columnsOfTBUSERWEDDING = {"user_id", "wedding_date", "status", "broker_id", "contacts_name", "contacts_msisdn", "contacts_avator", "role", "memo", "hotel", "budget_max", "ctime", "utime", "budget_min", "complete_time", "code", "arrange_service_fee_percent", "hotel_id", "wedding_flag", "hotel_address", "type"}; //21+1

    // Kafka相关————————1.3版本（2019.06.18）
    // Kafka相关配置
    public static final String[] StringTOPIC = {"budget_hotel_data", "test"};
    public static final String GROUP = "flume_kafka_streaming_group2";
    public static final String BOOTSTARP_SERVERS = "master:9092";
    // 本机测试用
//    public static final String BOOTSTARP_SERVERS = "hiwes:9092";

    // mysql相关————————1.3版本（2019.06.18）
    // MySQL数据库连接URL
    public static final String MYSQLURLTBCRM = "jdbc:mysql://rm-m5ejc86pt76j133u3.mysql.rds.aliyuncs.com:3306/izhaowo_crm?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false&serverTimezone=Asia/Shanghai&useSSL=true&dontTrackOpenResources=true&defaultFetchSize=10000&useCursorFetch=true";
    public static final String MYSQLURLTBUSERWEDDING = "jdbc:mysql://rm-m5ejc86pt76j133u3.mysql.rds.aliyuncs.com:3306/izhaowo_user_wedding?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false&serverTimezone=Asia/Shanghai&useSSL=true&dontTrackOpenResources=true&defaultFetchSize=10000&useCursorFetch=true";

    // MySQL数据库连接user
    public static final String MYSQLUSER13 = "jim";
    // MySQL数据库连接password
    public static final String MYSQLPASSWORD13 = "zwjim_123";
    // MySQL元数据表
    public static final String tb_user = "tb_user";
    public static final String tb_hotel = "tb_hotel";
    public static final String tb_user_wedding = "tb_user_wedding";

}
