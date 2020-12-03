package com.bangbang.cores.foruse;

import com.izhaowo.cores.utils.JavaHBaseUtils;
import com.izhaowo.cores.utils.JavaSparkUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class JavaTest {
    public static void main(String[] args) {
        String start = "16c3174a-0a36-46b9-bbf7-7fde885932fb=20191123=0";
        String stop = "16c3174a-0a36-46b9-bbf7-7fde885932fb=20191123=999";
        ResultScanner scanner1 = JavaHBaseUtils.getScanner("tb_planner_demand", start, stop);
        ResultScanner scanner2 = JavaHBaseUtils.getScanner("tb_planner_supply", start, stop);

        System.out.println("打印demand:");
        for (Result re : scanner1) {
            // "wedding_id", "planner_id", "province", "city", "zone", "hotel_name", "wedding_date", "budget_min", "budget_max", "done_num"
            String wedding_id = Bytes.toString(re.getRow());
            System.out.println(wedding_id);
        }
        System.out.println("打印supply:");
        for (Result re : scanner2) {
            // "wedding_id", "planner_id", "province", "city", "zone", "hotel_name", "wedding_date", "budget_min", "budget_max", "done_num"
            String wedding_id = Bytes.toString(re.getRow());
            System.out.println(wedding_id);
        }

    }
}
