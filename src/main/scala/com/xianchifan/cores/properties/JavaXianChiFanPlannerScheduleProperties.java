package com.xianchifan.cores.properties;

/**
 * 全新JavaBangBangProperties常数类，Java版本
 *
 * @version 2.0
 * @since 2019/06/17 by Hiwes
 */
public class JavaXianChiFanPlannerScheduleProperties {

    // Spark相关配置
    public static final String APPNAME = "IZhaowoBangBangData_1.0.0";
    public static final String MASRTERNAME = "local[4]";
    public static final String PARALLELISM = "500";
    public static final String SERIALIZER = "org.apache.spark.serializer.KryoSerializer";

    // HBase相关配置
    public static final String ZKQUORUM = "master:2181";

    // Kafka相关————————1.4版本（2019.06.19）
    // Kafka相关配置
    public static final String[] StringTOPIC = {"tb_planner_recom_use", "test"};
    public static final String GROUP = "flume_kafka_streaming_group4";
    public static final String BOOTSTARP_SERVERS = "master:9092";

    // mysql相关————————1.4版本（2019.06.19）
    // MySQL数据库连接URL
    public static final String MYSQLURLIZHAOWOWORKER = "jdbc:mysql://rm-m5ejc86pt76j133u3.mysql.rds.aliyuncs.com:3306/izhaowo_worker?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false&serverTimezone=Asia/Shanghai&useSSL=true&dontTrackOpenResources=true&defaultFetchSize=10000&useCursorFetch=true";
    public static final String MYSQLURLIZHAOWOWORKERSCHEDULE = "jdbc:mysql://rm-m5ejc86pt76j133u3.mysql.rds.aliyuncs.com:3306/izhaowo_schedule?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false&serverTimezone=Asia/Shanghai&useSSL=true&dontTrackOpenResources=true&defaultFetchSize=10000&useCursorFetch=true";
    public static final String MYSQLURLIZHAODATAMING = "jdbc:mysql://rm-m5ejc86pt76j133u3.mysql.rds.aliyuncs.com:3306/izhaowo_dataming?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false&serverTimezone=Asia/Shanghai&useSSL=true&dontTrackOpenResources=true&defaultFetchSize=10000&useCursorFetch=true";


    // MySQL数据库连接user
    public static final String MYSQLUSER16 = "jim";
    // MySQL数据库连接password
    public static final String MYSQLPASSWORD16 = "zwjim_123";

    // HBase相关————————1.6版本（2019.07.04）增加对stream数据的处理逻辑，进行策划师预定需求和供给数量的统计。
    public static final String TBWORKERSCHEDULE = "tb_worker_schedule"; // 职业人档期表，主要使用了type字段，0工作，1休息，2系统闭单。使用id作为key，使用时需要对表进行遍历。
    public static final String[] cfsOfTBWORKERSCHEDULE = {"info"};
    public static final String[] columnsOfTBWORKERSCHEDULE = {"worker_id", "schedule_id", "schedule_date", "type", "ctime", "utime", "flag"}; //7+1

    public static final String TBWORKER = "tb_worker"; // 职业人信息表，主要使用了daily_limit字段。使用worker_id作为rowkey。
    public static final String[] cfsOfTBWORKER = {"info"};
    public static final String[] columnsOfTBWORKER = {"name", "vocation_id", "user_id", "height", "weight", "sex", "birthday", "profile", "home_page", "daily_limit", "status", "ctime", "utime", "tag", "real_name", "avator"}; // 16+1

    public static final String TBLOWBUDGETHOTEL = "tb_low_budget_hotel"; // 策划师推荐表，主要使用了section和sot字段。使用worker_id:1作为rowkey。
    public static final String[] cfsOfTBLOWBUDGETHOTEL = {"info"};
    public static final String[] columnsOfTBLOWBUDGETHOTEL = {"province", "city", "zone", "hotel", "sot", "section", "sot_state", "bound_con"};

}
