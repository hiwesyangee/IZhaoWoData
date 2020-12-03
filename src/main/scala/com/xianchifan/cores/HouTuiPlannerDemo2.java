package com.xianchifan.cores;

import com.izhaowo.cores.utils.JavaHBaseUtils;
import com.izhaowo.cores.utils.MakeHashRow;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class HouTuiPlannerDemo2 {
    public static void main(String[] args) {
//        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN);
//        Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN);
//        Logger.getLogger("org.apache.hbase").setLevel(Level.WARN);
//        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
//        Logger.getLogger("org.apache.kafka").setLevel(Level.WARN);
//
//        // 1.获取所有策划师set
//        Set<String> allPlannerSet = new HashSet<>();
//        ResultScanner plannerScanner = JavaHBaseUtils.getScanner("worker_service_info");
//        for (Result result : plannerScanner) {
//            String planner_id = Bytes.toString(result.getRow()).split(":::")[0];
//            String flag = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("flag")));
//            if (flag != null && flag.equals("0")) {
//                allPlannerSet.add(planner_id);
//            }
//        }
//        System.out.println("策划师数量: " + allPlannerSet.size()); // 138
//
//        // 2.获取所有已下单策划师数据Map
//        Map<String, String> donePlannerMap = new HashMap<>();
//        ResultScanner donePlannerResult = JavaHBaseUtils.getScanner("tb_worker_wedding_order");
//        for (Result result : donePlannerResult) {
//            String flag = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("flag")));
//            if (flag != null && flag.equals("0")) {
//                String wedding_date = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("wedding_date")));
//                if (wedding_date != null) {
//                    wedding_date = wedding_date.substring(0, 10);
//                }
//                String worker_id = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("worker_id")));
//                String wedding_id = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("wedding_id")));
//                if (Integer.parseInt(wedding_date.replaceAll("-", "")) >= 20190101 && allPlannerSet.contains(worker_id)) {
//                    donePlannerMap.put(worker_id + "===" + wedding_id, wedding_date);
//                }
//            }
//        }
//
//        Set<String> plannerSet = new HashSet<>();
//        for (String plannerAndWId : donePlannerMap.keySet()) {
//            String planner = plannerAndWId.split("===")[0];
//            plannerSet.add(planner);
//        }
//        System.out.println("(策划师===婚礼,婚期)数量: " + donePlannerMap.size()); // 1567
//        System.out.println("2019.07.01之后的下单策划师数量: " + plannerSet.size()); // 125
//
//        // 3.过滤策划师需求和供给
//        Map<String, Integer> yanzhengMap = new HashMap<>();
//        ResultScanner demandScanner = JavaHBaseUtils.getScanner("tb_planner_demand");
//        int ss = 0;
//        for (Result result : demandScanner) {
//            String planner_id = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("planner_id")));
//            String wedding_date = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("wedding_date")));
//            String wedding_id = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("wedding_id")));
//            String key = planner_id + "===" + wedding_id;
//            if (donePlannerMap.get(key) != null && donePlannerMap.get(key).equals(wedding_date) && plannerSet.contains(planner_id)) {
//                yanzhengMap.put(key, 1);
//                ss++;
//            }
//        }
//        System.out.println("ss:" + ss);
//        System.out.println("中间验证map的数量: " + yanzhengMap.size());
//
//        ResultScanner supplyScanner = JavaHBaseUtils.getScanner("tb_planner_supply");
//        int ee = 0;
//        for (Result result : supplyScanner) {
//            String planner_id = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("planner_id")));
//            String wedding_date = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("wedding_date")));
//            String wedding_id = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("wedding_id")));
//            String key = planner_id + "===" + wedding_id;
//            if (donePlannerMap.get(key) != null && donePlannerMap.get(key).equals(wedding_date) && plannerSet.contains(planner_id) && yanzhengMap.keySet().contains(key)) {
//                yanzhengMap.put(key, 0);
//                ee++;
//            }
//        }
//        System.out.println("ee:" + ee);
//
//        Set<String> lastPlanner = new HashSet<>();
//        for (String planner : yanzhengMap.keySet()) {
//            if (yanzhengMap.get(planner) == 0) {
//                lastPlanner.add(planner.split("===")[0]);
//            }
//        }
//
//        Set<String> need = new HashSet<>();
//        for (String s : plannerSet) {
//            if (!lastPlanner.contains(s)) {
//                need.add(s);
//            }
//        }
//
//        System.out.println("验证后的map大小: " + yanzhengMap.size());  // 1866
//        System.out.println("最终的last数量: " + need.size()); // 35
//        for (String p : need) {
//            // p为planner_id
//            String name = JavaHBaseUtils.getValue("tb_worker", p, "info", "name");
//            String hotel = "5f0ef965-3b87-4158-af70-f91022ff588c";
//            String rowkey = String.valueOf(MakeHashRow.hashCode(hotel + ":" + p));
//            String province = JavaHBaseUtils.getValue("workers_hotel_distance", rowkey, "info", "p_province");
//            String city = JavaHBaseUtils.getValue("workers_hotel_distance", rowkey, "info", "p_city");
//            System.out.println("策划师:" + p + "," + name + "," + province + "," + city);
//        }
    }
}