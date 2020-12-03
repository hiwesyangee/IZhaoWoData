package com.bangbang.cores.foruse;

import com.izhaowo.cores.utils.JavaHBaseUtils;
import com.izhaowo.cores.utils.MakeHashRow;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

public class JavaDemo4Use {

    /**
     * 返回档期合适的策划师List
     *
     * @return
     */
    public static List<String> getTimeCandoPlannerList(String time) {
        List<String> list = new ArrayList<>();
        return list;
    }

    /**
     * 返回预算合适的策划师List
     *
     * @return
     */
    public static List<String> getBudgetCandoPlannerList(int totelBudget) {
        List<String> list = new ArrayList<>();
        // 1.获取所有策划师id
        ResultScanner scanner = JavaHBaseUtils.getScanner("worker_service_info");
        for (Result result : scanner) {
            String flag = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("flag")));
            if (flag.equals("0")) {
                // 2.通过服务费对比0.1*总预算，添加策划师list
                String service_amount = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("service_amount")));
                String planner_id = Bytes.toString(result.getRow()).split("::")[0];
                Double s_amount = Double.valueOf(service_amount);
                if (s_amount <= ((double) totelBudget / 10) && !list.contains(planner_id)) {
                    list.add(planner_id);
                }
            }
        }
        // 3.返回合适的策划师list
        return list;
    }

    /**
     * 获取距离合适的策划师List
     *
     * @return
     */
    public static List<String> getDistanceCandoPlannerList(String hotelId, List<String> plannerList, int distance) {
        List<String> list = new ArrayList<>();
        // 1.根据已获得策划师，查询酒店到策划师距离
        for (String planner : plannerList) {
            String row = String.valueOf(MakeHashRow.hashCode(hotelId + ":" + planner));
            String dis = JavaHBaseUtils.getValue("workers_hotel_distance", row, "info", "distance");
            // 2.进行数据判断
            if (dis != null) {
                System.out.println("dis = " + dis);
                if (Integer.valueOf(dis) <= distance) {
                    list.add(planner);
                }
            }
        }
        // 3.返回新策划师List
        return list;
    }

    /**
     * 获取在指定酒店做过指定预算范围的策划师List
     *
     * @return
     */
    public static List<String> getHadDoPlannerList(String hotelId, String samount, String eamount) {
        List<String> list = new ArrayList<>();
        return list;
    }

    public static void main(String[] args) {
        List<String> list = getBudgetCandoPlannerList(50000);
        System.out.println(list.size());
        List<String> distanceCandoPlannerList = getDistanceCandoPlannerList("1fa69245-c5bc-4107-9e06-73ebbe0879c0", list, 20000);
        System.out.println(distanceCandoPlannerList.size());
    }
}
