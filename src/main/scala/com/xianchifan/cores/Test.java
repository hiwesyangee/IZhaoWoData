package com.xianchifan.cores;

import com.izhaowo.cores.utils.JavaHBaseUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Test {
    public static void main(String[] args) {
        Set<String> allPlannerSet = new HashSet<>();
        ResultScanner plannerScanner = JavaHBaseUtils.getScanner("worker_service_info");
        for (Result result : plannerScanner) {
            String planner_id = Bytes.toString(result.getRow()).split(":::")[0];
            String flag = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("flag")));
            if (flag != null && flag.equals("0")) {
                allPlannerSet.add(planner_id);
            }
        }

        Map<String, Integer> map = new HashMap<>();
        ResultScanner donePlannerResult = JavaHBaseUtils.getScanner("tb_worker_wedding_order");
        for (Result result : donePlannerResult) {
            String worker_id = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("worker_id")));
            String flag = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("flag")));
            if (flag.equals("0") && allPlannerSet.contains(worker_id)) {
                if (map.keySet().contains(worker_id)) {
                    map.put(worker_id, map.get(worker_id) + 1);
                } else {
                    map.put(worker_id, 1);
                }
            }
        }
        System.out.println("------------");
        System.out.println(map.size());
        for (String s : map.keySet()) {
            String name = JavaHBaseUtils.getValue("tb_worker", s, "info", "name");
            System.out.println(s + "," + name + "==" + map.get(s));
        }
    }
}
