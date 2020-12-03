package com.izhaowo.cores.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 自用工具类
 *
 * @version 1.0
 * @since 2019/05/17 by hiwes
 */
public class MyUtils {
    private static List<String> originalList;
    private static Set<String> originalSet;

    /**
     * 获取数组中不重复的n个元素
     */
    public static String[] getRandomArray(String[] paramArray, int count) {
        if (paramArray.length < count) {
            return paramArray;
        }
        String[] newArray = new String[count];
        Random random = new Random();
        int temp;//接收产生的随机数
        List<Integer> list = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            temp = random.nextInt(paramArray.length);//将产生的随机数作为被抽数组的索引
            if (!(list.contains(temp))) {
                newArray[i - 1] = paramArray[temp];
                list.add(temp);
            } else {
                i--;
            }
        }
        return newArray;
    }

    /**
     * 获取列表中不重复的n个元素
     */
    public static List<String> getRandomList(List<String> top100, int count) {
        if (top100.size() < count) {
            return top100;
        }
        Random random = new Random();
        List<Integer> tempList = new ArrayList<>();
        List<String> newList = new ArrayList<>();
        int temp;
        for (int i = 0; i < count; i++) {
            temp = random.nextInt(top100.size());// 将产生的随机数作为被抽list的索引
            if (!tempList.contains(temp)) {
                tempList.add(temp);
                newList.add(top100.get(temp));
            } else {
                i--;
            }
        }
        return newList;
    }

    /**
     * 获取数组的重新随机排序结果
     */
    public static String[] getRandomSequenceArray(String[] paramArray) {
        String[] tempArr = new String[paramArray.length];
        Random random = new Random(new Date().getTime());
        int randomIndex = -1;
        for (int i = 0; i < tempArr.length; i++) {
            while (tempArr[i] == null) {
                randomIndex = random.nextInt(paramArray.length);
                if (paramArray[randomIndex] != null) {
                    tempArr[i] = paramArray[randomIndex];
                    paramArray[randomIndex] = null;
                }
            }
        }
        return tempArr;
    }

    // 获取去重String数组
    public static String[] getDistinctArray(String[] arr) {
        originalList = new ArrayList<>();
        for (String s : arr) {
            if (!originalList.contains(s)) {
                originalList.add(s);
            }
        }
        String[] newArr = new String[originalList.size()];
        return originalList.toArray(newArr);
    }

    // 获取指定数目1000的Set，不足1000就按之前的算
    public static Set<String> getThousandSet(Set<String> readSet) {
        originalSet = new HashSet<>();
        if (readSet.size() <= 1000) {
            originalSet = readSet;
        } else {
            List<String> readlist = new ArrayList<>(readSet);
            List<String> end = getRandomList(readlist, 1000);
            for (String read : end) {
                originalSet.add(read);
            }
        }

        return originalSet;

    }

    // 释放List资源
    public static void releaseList() {
        originalList.removeAll(originalList);
    }

    /**
     * 使用 Map按value进行排序
     *
     * @param
     * @return
     */
    public static Map<String, String> sortMapByValue(Map<String, String> oriMap) {
        if (oriMap == null || oriMap.isEmpty()) {
            return null;
        }
        Map<String, String> sortedMap = new LinkedHashMap<>();
        List<Map.Entry<String, String>> entryList = new ArrayList<>(oriMap.entrySet());
        Collections.sort(entryList, new MapValueComparator());

        Iterator<Map.Entry<String, String>> iter = entryList.iterator();
        Map.Entry<String, String> tmpEntry;
        while (iter.hasNext()) {
            tmpEntry = iter.next();
            sortedMap.put(tmpEntry.getKey(), tmpEntry.getValue());
        }
        return sortedMap;
    }

    /**
     * 获取两个时间中的所有天
     *
     * @param time1
     * @param time2
     * @return
     */
    public static List<String> checkTime(String time1, String time2) {
        SimpleDateFormat fmt = new SimpleDateFormat("yyyyMMdd");
        Date da1 = null;
        Date da2 = null;
        try {
            da1 = fmt.parse(time1);
            da2 = fmt.parse(time2);
        } catch (ParseException pe) {
            pe.printStackTrace();
        }
        //定义一个接受时间的集合
        List<Date> lDate = new ArrayList<Date>();
        lDate.add(da1);
        Calendar calBegin = Calendar.getInstance();
        // 使用给定的 Date 设置此 Calendar 的时间
        calBegin.setTime(da1);
        Calendar calEnd = Calendar.getInstance();
        // 使用给定的 Date 设置此 Calendar 的时间
        calEnd.setTime(da2);
        // 测试此日期是否在指定日期之后
        while (da2.after(calBegin.getTime())) {
            // 根据日历的规则，为给定的日历字段添加或减去指定的时间量
            calBegin.add(Calendar.DAY_OF_MONTH, 1);
            lDate.add(calBegin.getTime());
        }

        List<String> strList = new ArrayList<>();
        for (Date d : lDate) {
            SimpleDateFormat fmt2 = new SimpleDateFormat("yyyyMMdd");
            String need = fmt2.format(d);
            strList.add(need);
        }
        return strList;
    }

    /**
     * 获取时间所在年的目前为止所有月
     *
     * @param time2
     * @return
     */
    public static List<String> checkMonth(String time2) {
        List<String> strList = new ArrayList<>();
        String year = time2.substring(0, 4);
        int month = Integer.valueOf(time2.substring(4, 6));
        for (int i = 1; i <= month; i++) {
            String monthTime;
            if (i < 10) {
                monthTime = year + "0" + i;
            } else {
                monthTime = year + i;
            }
            strList.add(monthTime);
        }
        return strList;
    }

    /**
     * 返回月时间
     *
     * @param sourceDate
     * @param month
     * @return
     */
    public static Date stepMonth(Date sourceDate, int month) {
        Calendar c = Calendar.getInstance();
        c.setTime(sourceDate);
        c.add(Calendar.MONTH, month);
        return c.getTime();
    }

    private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMM");

    /**
     * 获取包含这个月在内的过去一年的月份
     *
     * @return
     */
    public static List<String> getPastYearMonth() {
        Date now = new Date();
        List<String> monthList = new ArrayList<>();
        for (int i = 11; i >= 0; i--) {
            Date monthDate = stepMonth(now, -i);
            String time = DATE_FORMAT.format(monthDate);
            monthList.add(time);
        }

        return monthList;
    }

    /**
     * 按value对Map进行排序，通用版
     *
     * @param map
     * @param <K>
     * @param <V>
     * @return
     */
    public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {
        List<Map.Entry<K, V>> list = new LinkedList<>(map.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                return (o2.getValue()).compareTo(o1.getValue());
            }
        });

        Map<K, V> result = new LinkedHashMap<K, V>();
        for (Map.Entry<K, V> entry : list) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

    /**
     * 获取某个时间的前几个天
     */
    public static String getLastDay(String time, int amount) {
        Calendar c = Calendar.getInstance();
        Date date = null;
        try {
            date = new SimpleDateFormat("yyyyMMdd").parse(time);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        c.setTime(date);
        int day1 = c.get(Calendar.DATE);
        c.set(Calendar.DATE, day1 - amount);

        String dayAfter = new SimpleDateFormat("yyyyMMdd").format(c.getTime());
        return dayAfter;
    }

    /**
     * 获取某个时间的前几个月
     */
    public static String getLastMonth(String time, int amount) {
        Calendar c = Calendar.getInstance();
        Date date = null;
        try {
            date = new SimpleDateFormat("yyyyMM").parse(time);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        c.setTime(date);
        int month1 = c.get(Calendar.MONTH);
        c.set(Calendar.MONTH, month1 - amount);

        String monthAfter = new SimpleDateFormat("yyyyMM").format(c.getTime());
        return monthAfter;
    }

    /**
     * 获取距离今天的时间
     */
    public static String getFromToday(int day) {
        Date date = new Date();//取时间
        Calendar calendar = new GregorianCalendar();
        calendar.setTime(date);
        calendar.add(calendar.DATE, day);//把日期往后增加一天.整数往后推,负数往前移动
        date = calendar.getTime(); //这个时间就是日期往后推一天的结果
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
        String dateString = formatter.format(date);
        return dateString;
    }


    /**
     * 将list按行写入到txt文件中
     *
     * @param strings
     * @param path
     * @throws Exception
     */
    public static void writeFileContext(List<String> strings, String path) throws Exception {
        File file = new File(path);
        //如果没有文件就创建
        if (!file.isFile()) {
            file.createNewFile();
        }
        BufferedWriter writer = new BufferedWriter(new FileWriter(path));
        for (String l : strings) {
            writer.write(l + "\r\n");
        }
        writer.close();
    }

    /**
     * 判断字符串是否为空
     *
     * @param str
     * @return
     */
    public static boolean strIsEmpty(String str) {
        if (str == null) {
            return true;
        } else if (str.equals("null")) {
            return true;
        }
        return false;
    }

    /**
     * 对两个数组进行去重
     *
     * @param arr1
     * @param arr2
     * @return
     */
    public static String[] distinctTwoArray(String[] arr1, String[] arr2) {
        if (arr1 == null) {
            return null;
        } else {
            if (arr2 == null) {
                return arr1;
            } else {
                List<String> list1 = new ArrayList<>();
                for (String s : arr1) {
                    list1.add(s);
                }
                List<String> list2 = new ArrayList<>();
                for (String s : arr2) {
                    list2.add(s);
                }
                list1.removeAll(list2);
                String[] arr = list1.toArray(new String[list1.size()]);
                return arr;
            }
        }
    }


    /**
     * 求两个数组的并集
     *
     * @param arr1
     * @param arr2
     * @return
     */
    public static String[] getTwoArrayUnion(String[] arr1, String[] arr2) {
        String[] need = null;
        if (arr1 != null && arr2 != null) {
            Set<String> hs = new HashSet<>();
            for (String str : arr1) {
                hs.add(str);
            }
            for (String str : arr2) {
                hs.add(str);
            }
            String[] result = {};
            need = hs.toArray(result);
            return need;
        } else if (arr1 == null) {
            if (arr2 != null) {
                return arr2;
            }
        } else if (arr2 == null) {
            if (arr1 != null) {
                return arr1;
            }
        }
        return need;

    }

    /**
     * 获取人民币数量，精确到分
     *
     * @return
     */
    public static String getYuan(double money) {
        try {
            if (money >= 0d) {
                double rmb = money; //0.01
                String rmbStr;
                if (rmb > 1d) {
                    DecimalFormat df = new DecimalFormat("#.00");
                    rmbStr = df.format(rmb);
                } else if (rmb < 1d) {
                    DecimalFormat df = new DecimalFormat("#.00");
                    rmbStr = "0" + df.format(rmb);
                } else {
                    DecimalFormat df = new DecimalFormat("#.00");
                    rmbStr = df.format(rmb);
                }
                return rmbStr;
            } else {
                return "0.00";
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "0.01";
    }

    /**
     * 获取两个数的和，结果保留两位小数
     */
    public static String getYuanHe(double a1, double b1) {
        try {
            double result = a1 + b1;
            String rmbStr;
            if (result >= 1d) { // 大于一元
                DecimalFormat df = new DecimalFormat("#.00");
                rmbStr = df.format(result);
            } else if (result < 1d) { // 小于一元
                DecimalFormat df = new DecimalFormat("#.00");
                rmbStr = "0" + df.format(result);
            } else {
                DecimalFormat df = new DecimalFormat("#.00");
                rmbStr = df.format(result);
            }
            return rmbStr;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }


    /**
     * 获取两个时间戳之间的所有小时str
     */
    public static List<String> getTwoTSHours(String startTime, String endTime) {
        List<String> list = new ArrayList<>();
        String start = JavaDateUtils.stamp2DateYMD(startTime).substring(0, 10) + "0000";
        String stop = JavaDateUtils.stamp2DateYMD(endTime).substring(0, 10) + "0000";

        DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");

        try {
            Date startDate = dateFormat.parse(start);  // 转为Date类型
            Date endDate = dateFormat.parse(stop);  // 转为Date类型

            Calendar tempStart = Calendar.getInstance();
            tempStart.setTime(startDate);  // 开始日历

            Calendar tempEnd = Calendar.getInstance();
            tempEnd.setTime(endDate);  // 结束日历时间
//            tempEnd.add(Calendar.HOUR, +1);// 小时加1(包含结束)

            while (tempStart.before(tempEnd) || tempStart.equals(tempEnd)) {  // 当小时还在之前，
                String shifenmiao = dateFormat.format(tempStart.getTime());
                String hour = shifenmiao.substring(0, 10);
                list.add(hour);
                tempStart.add(Calendar.HOUR, 1);
            }

        } catch (ParseException e) {
            e.printStackTrace();
        }

        return list;
    }

}
