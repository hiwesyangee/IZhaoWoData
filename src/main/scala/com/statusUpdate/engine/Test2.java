package com.statusUpdate.engine;

import com.izhaowo.cores.utils.JavaHBaseUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;
import java.util.Map;

public class Test2 {

    public static Map<String, String> getAllHotelIdAndSMid() {
        Map<String, String> map = new HashMap<>();
        ResultScanner results = JavaHBaseUtils.getScanner("v2_rp_tb_hotel");
        for (Result res : results) {
            String hotel_id = Bytes.toString(res.getRow());
            String smap = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("amap_id")));
            map.put(smap, hotel_id);
        }
        return map;
    }
}
