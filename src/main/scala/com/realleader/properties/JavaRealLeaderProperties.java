package com.realleader.properties;

/**
 * 全新JavaXianChiFanProperties常数类，Java版本
 *
 * @version 2.0
 * @since 2019/06/17 by Hiwes
 */
public class JavaRealLeaderProperties {

    // Spark相关配置
    public static final String APPNAME = "IZhaowoData-2.0";
    public static final String MASRTERNAME = "local[4]";
    public static final String PARALLELISM = "500";
    public static final String SERIALIZER = "org.apache.spark.serializer.KryoSerializer";

    // HBase相关配置
    public static final String ZKQUORUM = "master:2181";

    // mysql参数【寇耀数据库】
    // 外网
//    public static final String MYSQLURL = "jdbc:mysql://115.29.45.77:3306/izhaowo_worker?useUnicode=true&characterEncoding=UTF8";
    // 内网
    public static final String MYSQLURL = "jdbc:mysql://10.80.185.36:3306/izhaowo_worker?useUnicode=true&characterEncoding=UTF8";
    public static final String MYSQLUSER = "test_izw_2019";
    public static final String MYSQLPASSWORD = "izw(5678)!#$%^[ABCabc]9587";

    /**
     * 12.24新增，小市场策划师指标表。【每日定时从寇耀数据库中读取并存储到HBase。】
     */
    public static final String MVTBSMPLANNERINDICATOR = "mv_tb_small_market_planner_indicator"; // rowkey: hotel_id=section=worker_id
    public static final String[] cfsOfMVTBSMPLANNERINDICATOR = {"info"};
    public static final String[] columnsOfMVTBSMPLANNERINDICATOR = {"hotel_id", "section", "worker_id", "hotel_name", "sort", "distance", "case_dot", "reorder_rate", "communication_level", "design_sense", "case_rate", "all_score_final", "number", "text_rating_rate", "display_amount", "to_store_rate", "itime"}; // 17

    /**
     * 01.06新增，小市场策划师指标老表。
     */
    public static final String MVTBSMPLANNERINDICATOROLD = "mv_tb_small_market_planner_indicator_old"; // rowkey: hotel_id=section=worker_id
    public static final String[] cfsOfMVTBSMPLANNERINDICATOROLD = {"info"};
    public static final String[] columnsOfMVTBSMPLANNERINDICATOROLD = {"hotel_id", "section", "worker_id", "hotel_name", "sort", "distance", "case_dot", "reorder_rate", "communication_level", "design_sense", "case_rate", "all_score_final", "number", "text_rating_rate", "display_amount", "to_store_rate", "itime"}; // 17

}
