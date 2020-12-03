package com.izhaowo.cores.utils;

import com.izhaowo.cores.properties.JavaXianChiFanProperties;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

/**
 * 全新SparkUtils连接类，Java版本
 *
 * @version 2.0
 * @since 2019/06/17 by Hiwes
 */
public class JavaSparkUtils {
    private SparkConf conf;
    private SparkSession spark;

    private JavaSparkUtils() {
        conf = new SparkConf();
        conf.setAppName(JavaXianChiFanProperties.APPNAME);
        conf.setMaster(JavaXianChiFanProperties.MASRTERNAME);
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer"); // 优化序列化类型
        conf.set("spark.default.parallelism", JavaXianChiFanProperties.PARALLELISM);  // 优化并行度————在处理RDD时才会起作用，对Spark SQL的无效
        conf.set("spark.debug.maxToStringFields", "100");
//        conf.set("spark.streaming.kafka.maxRatePerPartition", "100"); // 控制Kafka吞吐量，默认=吞吐量/(kafka分区数*秒数)

        spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();
    }

    private volatile static JavaSparkUtils instance = null;

    // 双重校验锁，保证线程安全
    public static synchronized JavaSparkUtils getInstance() {
        if (instance == null) {
            synchronized (JavaSparkUtils.class) {
                if (instance == null) {
                    instance = new JavaSparkUtils();
                }
            }
        }
        return instance;
    }

    /**
     * 获取单例对象SparkConf实例
     */
    public SparkConf getSparkConf() {
        return conf;
    }

    /**
     * 获取单例对象SparkSession实例
     */
    public SparkSession getSparkSession() {
        return spark;
    }

}
