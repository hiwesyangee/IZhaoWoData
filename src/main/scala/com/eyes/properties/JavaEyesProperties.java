package com.eyes.properties;

/**
 * 自定义眼睛常数类
 *
 * @time 2020-03-30
 */
public class JavaEyesProperties {

    // 眼睛原始待统计小市场表
    public static final String MVTBEYESSMALLMARKETTABLE = "mv_tb_eyes_smallmarket_table"; // rowkey:hotel_id=section
    public static final String[] cfsOfMVTBEYESSMALLMARKETTABLE = {"info"};
    public static final String[] columnsOfMVTBEYESSMALLMARKETTABLE = {"hotel_id", "section", "smId"};

    // 眼睛原始小市场策划师指标表
    public static final String MVTBEYESPLANNERINDICATORSTABLE = "mv_tb_eyes_planner_indicators_table"; // towkey:hotel_id=section==yyyyMMdd
    public static final String[] cfsOfMVTBEYESPLANNERINDICATORSTABLE = {"info"};
    public static final String[] columnsOfMVTBEYESPLANNERINDICATORSTABLE = {"planner_id", "case_dot", "reorder_rate", "communication_level", "design_sense", "case_rate", "all_score_final", "number", "text_rating_rate", "display_amount", "to_store_rate"};


}
