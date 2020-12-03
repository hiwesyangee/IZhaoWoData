package com.yanjing.engine;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 一些速写方法，java版本
 */
public class JavaSomeUtils {

    /**
     * 均分数值区间，返回List
     *
     * @param first       起始值
     * @param last        终止值
     * @param granularity 区间个数
     * @param ttype       类型，包含：[],[),(],()
     * @return 区间List
     */
    public static List<Double> getSplitIntervalSetting(double first, double last, int granularity, String ttype) {
        List<Double> list = new ArrayList<>();
        for (double i = first; i <= last; i += ((last - first) / (double) granularity)) {
            list.add(i);
        }
        if ("[]".equals(ttype)) {
        } else if ("[)".equals(ttype)) {
            list.remove(list.size() - 1);
        } else if ("(]".equals(ttype)) {
            list.remove(0);
        } else if ("()".equals(ttype)) {
            list.remove(list.size() - 1);
            list.remove(0);
        } else {
            try {
                throw new Exception("Illegal type...");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return list;
    }

    /**
     * 统计区间内元素个数，指定保留两位.
     *
     * @param dataArr 待统计元素list
     * @param inArr   区间list
     * @return
     */
    public static Map<String, Integer> getRightIntervalBySetting(List<Double> dataArr, List<Double> inArr) {
        Map<String, Integer> map = new HashMap<>();
        for (double inData : inArr) {
            for (double data : dataArr) {
                int index = getIndex(inArr, inData);
                if (index > 0) {
                    if (data <= inData && data > inArr.get(index - 1)) {
                        if (!map.keySet().contains(String.valueOf(inData))) {
                            map.put(String.format("%.2f", inData), 1);
                        } else {
                            map.put(String.format("%.2f", inData), map.get(String.format("%.2f", inData)) + 1);
                        }
                    }
                } else if (index == 0) {
                    if (data <= inData) {
                        if (!map.keySet().contains(String.valueOf(inData))) {
                            map.put(String.format("%.2f", inData), 1);
                        } else {
                            map.put(String.format("%.2f", inData), map.get(String.format("%.2f", inData)) + 1);
                        }
                    }
                }
            }
        }
        for (double inData : inArr) {
            if (!map.keySet().contains(String.format("%.2f", inData))) {
                map.put(String.format("%.2f", inData), 0);
            }
        }
        return map;
    }

    /**
     * 统计区间内元素个数，指定保留n位.
     *
     * @param dataArr 待统计元素list
     * @param inArr   区间list
     * @param digit   保留位数
     * @return
     */
    public static Map<String, Integer> getRightIntervalBySetting(List<Double> dataArr, List<Double> inArr, int digit) {
        Map<String, Integer> map = new HashMap<>();
        for (double inData : inArr) {
            for (double data : dataArr) {
                int index = getIndex(inArr, inData);
                if (index > 0) {
                    if (data <= inData && data > inArr.get(index - 1)) {
                        if (!map.keySet().contains(String.valueOf(inData))) {
                            map.put(String.format("%." + digit + "f", inData), 1);
                        } else {
                            map.put(String.format("%." + digit + "f", inData), map.get(String.format("%." + digit + "f", inData)) + 1);
                        }
                    }
                } else if (index == 0) {
                    if (data <= inData) {
                        if (!map.keySet().contains(String.valueOf(inData))) {
                            map.put(String.format("%." + digit + "f", inData), 1);
                        } else {
                            map.put(String.format("%." + digit + "f", inData), map.get(String.format("%." + digit + "f", inData)) + 1);
                        }
                    }
                }
            }
        }
        for (double inData : inArr) {
            if (!map.keySet().contains(String.format("%." + digit + "f", inData))) {
                map.put(String.format("%." + digit + "f", inData), 0);
            }
        }
        return map;
    }


    public static int getIndex(List<Double> arr, double value) {
        for (int i = 0; i < arr.size(); i++) {
            if (arr.get(i) == value) {
                return i;
            }
        }
        return -1;//如果未找到返回-1
    }

    public static double get2Double(double a) {
        DecimalFormat df = new DecimalFormat("0.00");
        return new Double(df.format(a).toString());
    }

    public static void main(String[] args) {
//        List<Double> dataArr = new ArrayList<>();
////        dataArr.add(1d);
//        dataArr.add(-0.775616974541047d);
////        dataArr.add(2d);
//        List<Double> inArr = getSplitIntervalSetting(-1d, 0d, 100, "(]");
//        Map<String, Integer> map = getRightIntervalBySetting(dataArr, inArr);
//        for (String s : map.keySet()) {
//            System.out.println(s + "====" + map.get(s));
//        }

    }


}
