package com.realleaderAndyanjing.engine;

import com.yanjing.engine.SomeUtils;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class JavaSomeUtils2 {
    public static void main(String[] args) {

        long start = System.currentTimeMillis();
        List<Double> dataArr = new ArrayList<>();
        dataArr.add(1d);
        dataArr.add(-0.76);
        dataArr.add(0.923192319231923);
        dataArr.add(0d);

        List<Double> otherInterval = new ArrayList<>();
        Double ff = -1.00d;
        for (int f = 0; f < 200; f++) {
            ff = ff + 0.01d;
            BigDecimal bd = new BigDecimal(ff);
            Double resultData = bd.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
            System.out.println(resultData);
            otherInterval.add(resultData);
        }

        for (int i = 0; i < 10; i++) {
            Map<Double, Integer> map = getRightIntervalBySetting(dataArr, otherInterval);

            for (Double key : map.keySet()) {
                System.out.println(key + "====" + map.get(key));
            }
        }
        long stop = System.currentTimeMillis();

        System.out.println("time = " + (stop - start) / 1000 + "s.");

    }

    public static Map<Double, Integer> getRightIntervalBySetting(List<Double> dataArr, List<Double> inArr) {
        Map<Double, Integer> map = new LinkedHashMap<>();
        for (Double in : inArr) {
            map.put(in, 0);
        }

        for (Double data : dataArr) {
            BigDecimal bd = new BigDecimal(data);
            Double resultData = bd.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
            int newValue = map.get(resultData) + 1;
            map.put(resultData, newValue);
        }
        return map;
    }
}
