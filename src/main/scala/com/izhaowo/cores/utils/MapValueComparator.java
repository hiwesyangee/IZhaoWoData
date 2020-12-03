package com.izhaowo.cores.utils;

import java.util.Comparator;
import java.util.Map;

/**
 * Map排序辅助类
 *
 * @author Hiwes
 * @version 3.0.0
 * @since 2018/11/15
 */
public class MapValueComparator implements Comparator<Map.Entry<String, String>> {
    @Override
    public int compare(Map.Entry<String, String> me1, Map.Entry<String, String> me2) {
        return me2.getValue().compareTo(me1.getValue());
    }
}