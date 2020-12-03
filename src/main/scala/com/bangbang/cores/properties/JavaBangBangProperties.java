package com.bangbang.cores.properties;

/**
 * 全新JavaBangBangProperties常数类，Java版本
 *
 * @version 2.0
 * @since 2019/06/17 by Hiwes
 */
public class JavaBangBangProperties {

    // Spark相关配置
    public static final String APPNAME = "IZhaowoBangBangData_1.0.0";
    public static final String MASRTERNAME = "local[4]";
    public static final String PARALLELISM = "500";
    public static final String SERIALIZER = "org.apache.spark.serializer.KryoSerializer";

    // HBase相关配置
    public static final String ZKQUORUM = "master:2181";

    // HBase数据库表相关——————1.4版本（2019.06.19）
    public static final String TBPLANNERRECOMRECORD = "tb_planner_recom_record"; // rowkey:id
    public static final String[] cfsOfTBPLANNERRECOMRECORD = {"info"};
    public static final String[] columnsOfTBPLANNERRECOMRECORD = {"worker_id", "wedding_id", "ctime", "utime", "ranking", "type", "crm_user_id", "hotel_id"}; //7+1+1,最后的hotel_id，需要查询TBUSERWEDDINGHOTEL

    // Kafka相关————————1.4版本（2019.06.19）
    // Kafka相关配置
    public static final String[] StringTOPIC = {"tb_planner_recom_record_hotel", "test"};
    public static final String GROUP = "flume_kafka_streaming_group";
    public static final String BOOTSTARP_SERVERS = "master:9092";

    // mysql相关————————1.4版本（2019.06.19）
    // MySQL数据库连接URL
    public static final String MYSQLURLTBUSERWEDDING = "jdbc:mysql://rm-m5ejc86pt76j133u3.mysql.rds.aliyuncs.com:3306/izhaowo_user_wedding";

    // MySQL数据库连接user
    public static final String MYSQLUSER14 = "jim";
    // MySQL数据库连接password
    public static final String MYSQLPASSWORD14 = "zwjim_123";
    // MySQL元数据表
    public static final String tb_planner_recom_record = "tb_planner_recom_record";

    // HBase相关————————1.5版本（2019.06.21）增加对stream数据的处理逻辑，进行策划师预定需求和供给数量的统计。
    public static final String TBUSERWEDDINGTEAMMEMBER = "tb_user_wedding_team_member"; // rowkey:id
    public static final String[] cfsOfTBUSERWEDDINGTEAMMEMBER = {"info"};
    public static final String[] columnsOfTBUSERWEDDINGTEAMMEMBER = {"wedding_id", "hotel_id", "vocation", "sort", "ctime", "utime"}; // 每条数据代表

    public static final String TBPLANNERDEMAND = "tb_planner_demand"; // 需求表。rowkey:酒店id=年月日=1
    public static final String[] cfsOfTBPLANNERDEMAND = {"info"};
    public static final String[] columnsOfTBPLANNERDEMAND = {"wedding_id", "planner_id", "province", "city", "zone", "hotel_name", "wedding_date", "budget_min", "budget_max", "done_num"}; // done_num: 已预订策划师数量

    public static final String TBPLANNERSUPPLY = "tb_planner_supply"; // 供给表。rowkey:酒店id=年月日=1
    public static final String[] cfsOfTBPLANNERSUPPLY = {"info"};
    public static final String[] columnsOfTBPLANNERSUPPLY = {"wedding_id", "planner_id", "province", "city", "zone", "hotel_name", "wedding_date", "budget_min", "budget_max", "can_num"}; // can_num: 可预订策划师数量

    // 08.08修改后策划师供求业务————推荐结果记录.todo 需要在每日读取tb_planner_supply表之前读取，然后写入后再往SQLServer数据库写入
    public static final String RECOMPLANNERRESULT = "recom_planner_result"; // 供给表。rowkey:wedding_id=1
    public static final String[] cfsOfRECOMPLANNERRESULT = {"info"};
    public static final String[] columnsOfRECOMPLANNERRESULT = {"wedding_id", "planner_id"};

    // SQLServer数据库相关
    public static final String SQLSERVERUSER = "spark";
    public static final String SQLSERVERPASSWORD = "spark_20190522";
}
