package com.bangbang.cores.client;

import com.bangbang.cores.engine.SaveData2SQLServer;
import com.izhaowo.cores.utils.JavaDateUtils;

public class User4Data {
    public static void main(String[] args) {

        // 0.打印当前时间
        long start = System.currentTimeMillis();
        String today = JavaDateUtils.stamp2DateYMD(String.valueOf(start));
        // 1.每日清空HBase数据表，tb_planner_supply和tb_planner_demand；
        SaveData2SQLServer.truncateTableInHBase();
        long step1 = System.currentTimeMillis();
        System.out.println("1.每日任务清空表用时: " + (step1 - start) / 1000 + "s.");
        // 2.读取MySQL数据库，对tb_user_wedding_team_member表进行统计，得到已预订策划师并存入tb_planner_demand；
        SaveData2SQLServer.savePlannerDemandData2HBase();
        long step2 = System.currentTimeMillis();
        System.out.println("2.每日任务写入已预定数据用时: " + (step2 - step1) / 1000 + "s.");
        // 3.读取v2_rp_tb_planner_recom_record，针对utime在20190819之前的数据进行统计，得到可预订策划师历史数据并存入tb_planner_supply；
        SaveData2SQLServer.saveHistoryRecordData2HBase();
        long step3 = System.currentTimeMillis();
        System.out.println("3.每日任务写入历史可预定数据用时: " + (step3 - step2) / 1000 + "s.");
        // 4.读取HBase数据库，对recom_planner_result表进行统计，得到可预订策划师并存入tb_planner_supply
        SaveData2SQLServer.saveNowRecordData2HBase();
        long step4 = System.currentTimeMillis();
        System.out.println("4.每日任务写入现在可预定数据用时: " + (step4 - step3) / 1000 + "s.");
        // 5.读取tb_planner_supply，写入SQLServer。
        SaveData2SQLServer.savePlannnerSupply2SQLServer();
        long step5 = System.currentTimeMillis();
        System.out.println("6.每日任务写入可预定到SQLServer数据用时: " + (step5 - step4) / 1000 + "s.");
        // 6.读取tb_planner_demand，写入SQLServer；
        SaveData2SQLServer.savePlannnerDemand2SQLServer();
        long step6 = System.currentTimeMillis();
        System.out.println("5.每日任务写入已预定到SQLServer数据用时: " + (step6 - step5) / 1000 + "s.");

        long stop = System.currentTimeMillis();
        System.out.println(today + " 每日定时任务用时: " + (stop - start) / 1000 + "s.");

    }
}
