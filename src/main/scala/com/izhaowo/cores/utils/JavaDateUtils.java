package com.izhaowo.cores.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * 爱找我实时流1.0.0版本时间类Java版本
 *
 * @version 1.0
 * @since 2019/05/17 by Hiwes
 */
public class JavaDateUtils {
    /**
     * 将时间戳转换为时间
     */
    public static String stamp2Date(String s) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Long lt = Long.parseLong(s);
        Date date = new Date(lt);
        String result = sdf.format(date);
        return result;
    }

    /**
     * 将时间转换为时间戳
     */
    public static String date2Stamp(String s) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = sdf.parse(s);
        Long ts = date.getTime();
        String result = ts.toString();
        return result;
    }

    /**
     * 获取当前时间的" 年 月 日 时 分 秒 "的字符串
     */
    public static String getDateYMD() {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Long lt = new Date().getTime();
        String res = simpleDateFormat.format(lt);
        String[] time1 = res.split(" ")[0].split("-");
        String[] time2 = res.split(" ")[1].split(":");
        String result = time1[0] + time1[1] + time1[2] + time2[0] + time2[1] + time2[2];
        return result;
    }

    /**
     * 获取当前时间的" 年 月 日 时 分 秒 "的字符串
     */
    public static String getDateYMDNew() {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        Long lt = new Date().getTime();
        String res = simpleDateFormat.format(lt);
        return res;
    }


    public static String getDate() {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Long lt = new Date().getTime();
        String res = simpleDateFormat.format(lt);
        return res;
    }

    public static String getDateOnlyYMD() {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Long lt = new Date().getTime();
        String res = simpleDateFormat.format(lt);
        return res;
    }

    /**
     * 将时间戳转换为"yyMMddHHmmss/年 月 日 时 分 秒 "的字符串
     */
    public static String stamp2DateYMD(String s) {
        String need = stamp2Date(s);
        String[] time1 = need.split(" ")[0].split("-");
        String[] time2 = need.split(" ")[1].split(":");
        String result = time1[0] + time1[1] + time1[2] + time2[0] + time2[1] + time2[2];
        return result;
    }

    /**
     * 将日志格式时间 "dd/MMM/yy:HH:mm:ss Z" 转换为 "yy-MM-dd HH:mm:ss"
     */
    static SimpleDateFormat YYYMMDDHHMM_TIME_FORMAT = new SimpleDateFormat("ydd/MMM/yy:HH:mm:ss Z", Locale.ENGLISH);
    static SimpleDateFormat TARGET_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static String logTime2Date(String logTime) {
        return TARGET_FORMAT.format(new Date(getLogTime(logTime)));
    }

    public static Long getLogTime(String logTime) {
        try {
            return YYYMMDDHHMM_TIME_FORMAT.parse(logTime.substring(logTime.indexOf("[") + 1, logTime.lastIndexOf("]"))).getTime();
        } catch (Exception e) {
            return 0l;
        }
    }

    public static void main(String[] args) {
        System.out.println(getDateOnlyYMD());
    }
}
