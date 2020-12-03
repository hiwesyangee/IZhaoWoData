package com.izhaowo.cores.utils;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class JavaMongoDBUtils {

    private MongoDatabase mondoDatabase;
    private String userName = "smallMarketWriter";
    private String database = "production_database";
    private String password = "110120119";

    private JavaMongoDBUtils() {
        ServerAddress serverAddress = new ServerAddress("139.129.230.5", 27017);
        List<ServerAddress> adds = new ArrayList<>();
        adds.add(serverAddress);
        //用户名，连接的数据库名，用户密码
        MongoCredential credential = MongoCredential.createCredential(userName, database, password.toCharArray());
        List<MongoCredential> credentials = new ArrayList();
        credentials.add(credential);
        //通过验证连接到MongoDB客户端
        MongoClient mongoClient = new MongoClient(adds, credentials);
        //连接数据库
        mondoDatabase = mongoClient.getDatabase("production_database");
    }

    private static JavaMongoDBUtils instance = null;

    // 双重校验锁，保证线程安全
    public static synchronized JavaMongoDBUtils getInstance() {
        if (instance == null) {
            synchronized (JavaMongoDBUtils.class) {
                if (instance == null) {
                    instance = new JavaMongoDBUtils();
                }
            }
        }
        return instance;
    }

    /**
     * 获取单例对象MongoDB实例
     */
    public MongoDatabase getMongoDB() {
        return mondoDatabase;
    }

    /**
     * 清空所有的collection内数据
     */
    public static void truncateSpecifiedColl(MongoDatabase mondoDatabase, String collection_name) {
        try {
            MongoCollection<Document> collection = mondoDatabase.getCollection(collection_name);
            collection.deleteMany(new Document()); // 删除所有的文档
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 写入数据到指定collection
     */
    public static void insertData2SpecofoedColl(MongoDatabase mondoDatabase, String collection_name, Document document) {
        try {
            MongoCollection<Document> collection = mondoDatabase.getCollection(collection_name);
            collection.insertOne(document);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 更新数据到指定collection
     */
    public static void updateData2SpecofoedColl(MongoDatabase mondoDatabase, String collection_name, Document document, String field, String value) {
        try {
            MongoCollection<Document> collection = mondoDatabase.getCollection(collection_name);
            collection.updateOne(Filters.eq(field, value), new Document("$set", document)); // inc:对指定字段进行增量增加，当字段不存在时，则在该文档中添加字段并赋值, set:只修改指定字段值，当字段不存在时，则在该文档中添加一个新的字段并赋值
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void upsertData2SpecofoedColl(MongoDatabase mondoDatabase, String collection_name, Document document, String field, String value) {
        try {
            MongoCollection<Document> collection = mondoDatabase.getCollection(collection_name);
            if (upsertDataSMIDIsExist(collection, value)) {
                System.out.println("存在。");
                collection.updateOne(Filters.eq(field, value), new Document("$set", document)); // inc:对指定字段进行增量增加，当字段不存在时，则在该文档中添加字段并赋值, set:只修改指定字段值，当字段不存在时，则在该文档中添加一个新的字段并赋值
            } else {
                System.out.println("为空。");
                collection.insertOne(document);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static boolean upsertDataSMIDIsExist(MongoCollection<Document> collection, String value) {
        Document bson = new Document();
        bson.put("smallMarketId", 1);
        MongoCursor<Document> ite = collection.find(Filters.eq("smallMarketId", value)).projection(bson).iterator();
        while (ite.hasNext()) {
            Document in = ite.next();
            if (in.get("smallMarketId").equals(value)) {
                return true;
            } else {
                return false;
            }
        }
        return false;
    }

    /**
     * 删除数据到指定collection
     */
    public static void deleteData2SpecofoedColl(MongoDatabase mondoDatabase, String collection_name, Document document) {
        try {
            MongoCollection<Document> collection = mondoDatabase.getCollection(collection_name);
            collection.deleteOne(document);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 打印指定collection数据
     */
    public static void showData(MongoDatabase mondoDatabase, String collection_name) {
        MongoCollection<Document> collection = mondoDatabase.getCollection(collection_name);
        //检索所有文档
        //获取迭代器
        FindIterable<Document> findInterable = collection.find();
        //获取游标
        MongoCursor<Document> mongoCursor = findInterable.iterator();
        //开始遍历
        while (mongoCursor.hasNext()) {
            Document a = mongoCursor.next();
            System.out.println(a);
        }
    }

    public static boolean smallMarketIdIsExist(String smallMarketId) {
        MongoDatabase mondoDatabase = getInstance().getMongoDB();
        MongoCollection<Document> collection = mondoDatabase.getCollection("small_market_production");
        Document bson = new Document();
        bson.put("_id", 1);
        bson.put("status", 1);
        MongoCursor<Document> ite = collection.find(Filters.eq("_id", smallMarketId)).projection(bson).iterator();
        while (ite.hasNext()) {
            Document in = ite.next();
            if (in.get("_id").equals(smallMarketId) && in.get("status").equals(1)) {
                return true;
            } else {
                return false;
            }
        }
        return false;
    }

    public static boolean smallMarketIdIsExistOld(String smallMarketId) {
        MongoDatabase mondoDatabase = getInstance().getMongoDB();
        MongoCollection<Document> collection = mondoDatabase.getCollection("small_market_production");
        Document bson = new Document();
        bson.put("_id", 1);
        bson.put("status2", 1);
        MongoCursor<Document> ite = collection.find(Filters.eq("_id", smallMarketId)).projection(bson).iterator();
//        int i = 0;
        while (ite.hasNext()) {
            Document in = ite.next();
            try {
                if (in.get("_id").equals(smallMarketId) && in.get("status2") != null && in.get("status2").equals(1)) {
//                    i++;
                    return true;
                } else {
                    return false;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
//        System.out.println("in i = " + i);
        return false;
    }

    /**
     * 释放mongodb资源
     */
    public static void releaseDBResources(MongoDatabase mondoDatabase) {
        try {
            if (mondoDatabase != null) {
                mondoDatabase.drop();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        MongoDatabase mondoDatabase = JavaMongoDBUtils.getInstance().getMongoDB();
////        JavaMongoDBUtils.getInstance().showData(mondoDatabase, "small_market_production");
        MongoCollection<Document> collection = mondoDatabase.getCollection("small_market_production");
//        //检索所有文档
//        //获取迭代器
        FindIterable<Document> findInterable = collection.find(
//                Filters.eq("_id", "B001C95CXN_2")
                Filters.eq("status2", 1)
        );
//        //获取游标
        MongoCursor<Document> mongoCursor = findInterable.iterator();
        //开始遍历
        int i = 0;
        while (mongoCursor.hasNext()) {
//            Document a = mongoCursor.next();
//            String b = a.toJson();
//            System.out.println(b);
            i++;
        }
        System.out.println("i = " + i);

//        Document outBson = new Document();
//        outBson.put("status2", 1);
//
//        JavaMongoDBUtils.updateData2SpecofoedColl(mondoDatabase, "small_market_production", outBson, "_id", "B001C7WA5B_3");


//
////        System.out.println(smallMarketIdIsExist("B001C7WA5B_3"));
//        System.out.println(smallMarketIdIsExist("B001C7WA5B_3"));
//        System.out.println(smallMarketIdIsExistOld("B001C80D1F_5"));
//
//        Document bson = new Document();
//        Document inBson = new Document();
//        inBson.put("value", "测试2");
//        inBson.put("timestamp", System.currentTimeMillis());
//        bson.put("v91", inBson);
//        updateData2SpecofoedColl(mondoDatabase, "small_market_production", bson, "_id", "B001C7WA5B_3");
//        System.out.println("写入数据");
//        JavaMongoDBUtils.getInstance().showData(mondoDatabase, "small_market_production");
//        System.out.println(smallMarketIdIsExist("B001C7WA5B_3"));

    }
}
