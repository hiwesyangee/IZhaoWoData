package com.yanjing.client

import java.sql.Statement

import com.izhaowo.cores.utils.{JavaHBaseUtils, JavaSQLServerConn}
import com.realleader.properties.JavaRealLeaderProperties
import com.yanjing.engine.{JavaSomeUtils, SomeUtils, YanjingEngine}
import com.yanjing.engine.YanjingEngine.spark
import org.apache.spark.sql.DataFrame

object YanJingUse {
  def main(args: Array[String]): Unit = {
    val start = System.currentTimeMillis()
    updateOldDataNow()
    //    updateOldDataNow()
    //    updateNewDataNow()
    val stop = System.currentTimeMillis()
    println("本次录入策略多面体用时: " + (stop - start) / 1000 + " s.")
  }

  def updateNewData(): Unit = {
    YanjingEngine.saveDfTotalData("1fa69245-c5bc-4107-9e06-73ebbe0879c0", "世外桃源酒店", 3) // 直接调用方法，对数据进行获取
    // 第二波策略多面体酒店
    YanjingEngine.saveDfTotalData("43c7e2bf-4c7a-4af1-94bc-8ed553a23ddb", "正熙雅居酒店", 1) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalData("f59f8c98-5270-4c45-804b-3bcca78efa00", "艺朗酒店", 1) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalData("18b2bd26-a06b-4abd-a8a5-b4f1d3f5a8f5", "厚院庄园", 2) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalData("7d477695-a2e2-4010-9e64-0b420245596b", "亿臣国际酒店", 2) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalData("917ead54-441e-444c-8927-8257f07b824d", "简阳城市名人酒店·宴会厅", 3) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalData("e1520dbe-8fb0-430c-b2e4-7931309541de", "诺亚方舟(东南门)", 3) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalData("8a19bcfb-909d-4388-9d1f-e59ef9b989c1", "南昌万达文华酒店", 4) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalData("01bb057e-5b65-4a1f-b211-ce8692aa85d7", "巴国布衣(神仙树店)", 4) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalData("427b89cc-795d-4899-8fea-4a14d8f4bd01", "春生", 5) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalData("d280ab29-ee5f-4a3c-a7a4-01468167d5a0", "重庆金陵大饭店", 5) // 直接调用方法，对数据进行获取

    // 第三波策略多面体酒店
    YanjingEngine.saveDfTotalData("dd99abe5-d2f2-4ae0-8c22-579251ed1d7d", "成都金韵酒店", 1) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalData("a335c367-4d9a-4d6b-90c3-fc369703f147", "成都大鼎戴斯大酒店", 1) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalData("97c6330d-26f1-4c67-ae4f-5bbddfb79f9e", "世茂成都茂御酒店", 2) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalData("a9fe4b14-5f45-47c2-bc35-4d36fcdaae05", "交子国际酒店", 2) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalData("7f7ded13-8cf2-44c3-aed0-491fea28a77c", "天邑国际酒店", 3) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalData("d3ede457-bdee-43d5-aac8-15b57a72db7c", "华府金座", 3) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalData("0346b938-2a7b-4b70-bbc4-1bd500547497", "林道假日酒店", 4) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalData("16786afa-f9bc-4e74-848f-bc0fc79e6bf3", "艾克美雅阁大酒店", 4) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalData("462aee34-67b5-4851-aa2e-002aafc2a22c", "西苑半岛", 5) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalData("be621581-25d8-4dbd-847b-da7c72dbfefc", "怡东国际酒店", 5) // 直接调用方法，对数据进行获取

    // 第四波策略多面体酒店
    YanjingEngine.saveDfTotalData("ef0c39be-9675-49dc-a32e-23a31f5db258", "成都希尔顿酒店", 1) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalData("8072ab05-5e85-4276-9ace-ac2851f7a103", "蓉南国际酒店", 1) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalData("a335c367-4d9a-4d6b-90c3-fc369703f147", "成都大鼎戴斯大酒店", 2) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalData("ed8da82f-5aec-40a8-9b24-7ba3c289c0b8", "南堂馆(天府三街店)", 2) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalData("5849ad9a-f16d-417a-a58f-ec51e971cb47", "航宸国际酒店", 3) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalData("8ede682c-8144-4eb7-8050-1b76e5181210", "路易·珀泰夏堡", 3) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalData("9ee77461-0e82-418e-ba27-4bfa7cf2f242", "瑞升·芭富丽大酒店", 4) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalData("792d3a3a-9a15-4846-bd84-b3c480de08ea", "成都新东方千禧大酒店", 4) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalData("d3484e83-4408-41eb-923b-e009672afe55", "成都首座万丽酒店", 5) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalData("a835f3df-4a27-4c2f-8456-fbab4ccea61b", "成都百悦希尔顿逸林酒店", 5) // 直接调用方法，对数据进行获取

    // 第五波策略多面体酒店
    YanjingEngine.saveDfTotalData("8bda29bb-c63e-4631-a465-e928ccf7525e", "峨眉山大酒店", 1)
    YanjingEngine.saveDfTotalData("e812e6b3-d881-45e2-b5b3-e168bca74ff7", "尚成十八步岛酒店", 1)
    YanjingEngine.saveDfTotalData("a67ad81c-61b3-467a-a162-c7aedf177b46", "金领·莲花大酒店", 2)
    YanjingEngine.saveDfTotalData("852d1616-5c9c-4ca3-8b27-54fa5362f191", "南昌瑞颐大酒店(东门)", 2)
    YanjingEngine.saveDfTotalData("fa409e82-ac70-497c-9d3d-88345e5712ff", "圣瑞云台营山宴会中心", 3)
    YanjingEngine.saveDfTotalData("7c8f6e40-bd80-42ee-9289-a57acd840801", "今日东坡", 3)
    YanjingEngine.saveDfTotalData("f2c3aadf-5530-4d22-921d-ddb206543dbc", "滨江大会堂", 4)
    YanjingEngine.saveDfTotalData("8bda29bb-c63e-4631-a465-e928ccf7525e", "峨眉山大酒店	", 4)
    YanjingEngine.saveDfTotalData("fa409e82-ac70-497c-9d3d-88345e5712ff", "圣瑞云台营山宴会中心", 5)
    YanjingEngine.saveDfTotalData("852d1616-5c9c-4ca3-8b27-54fa5362f191", "南昌瑞颐大酒店(东门)", 5)

    // 第六波策略多面体酒店
    YanjingEngine.saveDfTotalData("4fb48ad2-fcaa-41bd-b148-fde3ddf2bbbd", "宽亭酒楼", 1)
    YanjingEngine.saveDfTotalData("a335c367-4d9a-4d6b-90c3-fc369703f147", "成都大鼎戴斯大酒店", 1)
    YanjingEngine.saveDfTotalData("3e7e916c-2631-4d4f-862a-5edb80859fc5", "成都龙之梦大酒店", 2)
    YanjingEngine.saveDfTotalData("8f0e43e3-42c3-4d5f-821f-b6250332d85d", "安泰安蓉大酒店", 2)
    YanjingEngine.saveDfTotalData("7992b693-5d0b-4c64-9463-e1f22c50805e", "第一江南酒店", 3)
    YanjingEngine.saveDfTotalData("fc2e2086-bd35-4580-a5aa-91257b4116fb", "天和缘", 3)
    YanjingEngine.saveDfTotalData("eccee365-a536-4d6f-be71-64401f7c59c8", "绿洲大酒店", 4)
    YanjingEngine.saveDfTotalData("d7a2d62d-73ab-43c0-aa7e-a28d21a01046", "林恩国际酒店	", 4)
    YanjingEngine.saveDfTotalData("64e6c112-1e4d-4d3d-b3a7-a49344840302", "成都棕榈泉费尔蒙酒店", 5)
    YanjingEngine.saveDfTotalData("e7bd0545-82ce-4c1a-a0c2-0b6ed6307d8c", "成都首座万豪酒店", 5)

    // 第7波策略多面体酒店
    // section1
    YanjingEngine.saveDfTotalData("4a3b30ac-73a9-4c5c-81f4-94be28fbeeec", "不二山房", 1)
    YanjingEngine.saveDfTotalData("57290e8c-adab-48c6-ae2f-96c397a8aa28", "西蜀森林酒店(西南1门)", 1)
    YanjingEngine.saveDfTotalData("5b426558-5264-4749-87ac-cc697ba0cc0a", "红杏酒家(明珠店)", 1)
    YanjingEngine.saveDfTotalData("cc4ee06f-bfbf-4768-a179-c1fda8ef4808", "大蓉和·卓锦酒楼(鹭岛路店)", 1)
    YanjingEngine.saveDfTotalData("98127048-a928-405f-af1e-1501e9be5192", "巴国布衣紫荆店-宴会厅", 1)
    YanjingEngine.saveDfTotalData("a82f0f18-daf8-4f8d-93c1-e26de1b65eb7", "成都合江亭翰文大酒店", 1)
    YanjingEngine.saveDfTotalData("3e7e916c-2631-4d4f-862a-5edb80859fc5", "成都龙之梦大酒店", 1)
    YanjingEngine.saveDfTotalData("2d8043cd-00a0-40a0-b939-53663c409853", "泰合索菲特大饭店", 1)
    YanjingEngine.saveDfTotalData("454f80e9-4ca9-48e9-b0c4-1eac095bcda0", "月圆霖", 1)
    YanjingEngine.saveDfTotalData("5fd54d5a-403a-4c75-9da4-c4e687ef07cf", "迎宾一号", 1)
    // section2
    YanjingEngine.saveDfTotalData("01d3d0fc-a47e-47dc-bf9f-8c1f5492ad24", "郦湾国际酒店", 2)
    YanjingEngine.saveDfTotalData("392359e6-b6eb-4098-bad8-9fc4c43110df", "寅生国际酒店", 2)
    YanjingEngine.saveDfTotalData("02d6ccf1-46e0-4d28-83dd-769bfa8ec4b0", "大蓉和(一品天下旗舰店)", 2)
    YanjingEngine.saveDfTotalData("e22c5f92-70a8-4a81-a6ff-cb3c7a7d87db", "林海山庄(环港路)", 2)
    YanjingEngine.saveDfTotalData("3e47de5d-e076-454d-b0f8-d555064a96c3", "川西人居", 2)
    YanjingEngine.saveDfTotalData("710629f0-31e1-45fd-903c-d670055fb327", "西蜀人家", 2)
    YanjingEngine.saveDfTotalData("29672fa4-4640-4a2f-b0a8-f868d54e67a7", "漫花庄园", 2)
    YanjingEngine.saveDfTotalData("c461e101-23db-4cf0-8824-9fdfb442271c", "成都牧山沁园酒店", 2)
    YanjingEngine.saveDfTotalData("d9105038-f307-431e-bb51-4c25e5335d45", "洁惠花园饭店", 2)
    YanjingEngine.saveDfTotalData("6af69637-105e-4647-b592-937a8742e208", "老房子(毗河店)", 2)
    // section3
    YanjingEngine.saveDfTotalData("bd64ef3a-40ee-42fc-8289-fc57e2f177a4", "映月湖酒店", 3)
    YanjingEngine.saveDfTotalData("057e09d5-9c3d-48c2-8d85-1338ef133625", "川投国际酒店", 3)
    YanjingEngine.saveDfTotalData("da844c8f-c413-4733-8ad8-b3f644a93a98", "友豪·罗曼大酒店", 3)
    YanjingEngine.saveDfTotalData("2dd8454a-16cb-434f-aff7-6eba8454c341", "鹿归国际酒店", 3)
    YanjingEngine.saveDfTotalData("11dcc500-2cef-44d0-bfcd-a70014090031", "桂湖国际大酒店", 3)
    YanjingEngine.saveDfTotalData("3cb310bd-c2bf-4c83-82ab-40280f232e24", "喜鹊先生花园餐厅", 3)
    YanjingEngine.saveDfTotalData("c058f54d-8c4e-4fa7-978e-412f0536c816", "菁华园", 3)
    YanjingEngine.saveDfTotalData("787eaa99-7092-4593-9db6-39353f1e38cd", "顺兴老(世纪城店)", 3)
    YanjingEngine.saveDfTotalData("4032eb26-2b39-4fea-bb9b-fc4491b0b171", "蒋排骨顺湖园", 3)
    YanjingEngine.saveDfTotalData("90c25b75-9ac0-4c8e-a4bb-5c898753859a", "喜馆精品酒店", 3)
    // section4
    YanjingEngine.saveDfTotalData("8996068b-0ceb-46e1-a366-d70282b5eb1d", "智汇堂枫泽大酒店", 4)
    YanjingEngine.saveDfTotalData("c3dc2c80-4571-4ecc-a5f0-9b380cb8a163", "中胜大酒店", 4)
    YanjingEngine.saveDfTotalData("29672fa4-4640-4a2f-b0a8-f868d54e67a7", "漫花庄园", 4)
    YanjingEngine.saveDfTotalData("057e09d5-9c3d-48c2-8d85-1338ef133625", "川投国际酒店", 4)
    YanjingEngine.saveDfTotalData("c6234b4f-aa8e-4949-be1a-075e639f2157", "锦亦缘新派川菜", 4)
    YanjingEngine.saveDfTotalData("16c3174a-0a36-46b9-bbf7-7fde885932fb", "嘉莱特精典国际酒店", 4)
    YanjingEngine.saveDfTotalData("af155b48-3149-45af-8625-16c42d5570e3", "明宇豪雅饭店(科华店)", 4)
    YanjingEngine.saveDfTotalData("724ee82d-57ba-4e70-913a-d868508322a5", "老房子华粹元年食府(天府三街店)", 4)
    YanjingEngine.saveDfTotalData("9ff71e0b-b881-4718-9319-f87f5ce82f40", "重庆澳维酒店(澳维酒店港式茶餐厅)", 4)
    YanjingEngine.saveDfTotalData("3e3e86d6-3b2a-4446-8ae0-bdc4ec158489", "香城竹韵(斑竹园店)", 4)
    // section5
    YanjingEngine.saveDfTotalData("97c6330d-26f1-4c67-ae4f-5bbddfb79f9e", "世茂成都茂御酒店", 5)
    YanjingEngine.saveDfTotalData("5b967168-f07f-4f09-b7c0-55ada1d9dec1", "明宇丽雅悦酒店", 5)
    YanjingEngine.saveDfTotalData("ebb0bdaf-214c-4633-b121-4ebd2da5fc85", "星宸航都国际酒店", 5)
    YanjingEngine.saveDfTotalData("8936bf11-c356-4115-9f5f-c7b6c828b46b", "成都凯宾斯基饭店", 5)
    YanjingEngine.saveDfTotalData("a9fe4b14-5f45-47c2-bc35-4d36fcdaae05", "交子国际酒店", 5)
    YanjingEngine.saveDfTotalData("1fa69245-c5bc-4107-9e06-73ebbe0879c0", "世外桃源酒店", 5)
    YanjingEngine.saveDfTotalData("724ee82d-57ba-4e70-913a-d868508322a5", "老房子华粹元年食府(天府三街店)", 5)
    YanjingEngine.saveDfTotalData("7ccfdc8f-7237-4d8f-a002-f6f235880334", "和淦·香城竹韵", 5)
    YanjingEngine.saveDfTotalData("1158cc09-daa5-4942-a84d-5b2e02238a3c", "家园国际酒店", 5)
    YanjingEngine.saveDfTotalData("b50fcf50-9cfb-4c48-8115-9cc47e89a521", "成都雅居乐豪生大酒店", 5)

  }

  def updateOldData(): Unit = {
    // 第一波策略多面体酒店
    YanjingEngine.saveDfTotalDataOld("1fa69245-c5bc-4107-9e06-73ebbe0879c0", "世外桃源酒店", 3) // 直接调用方法，对数据进行获取
    // 第二波策略多面体酒店
    YanjingEngine.saveDfTotalDataOld("43c7e2bf-4c7a-4af1-94bc-8ed553a23ddb", "正熙雅居酒店", 1) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalDataOld("f59f8c98-5270-4c45-804b-3bcca78efa00", "艺朗酒店", 1) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalDataOld("18b2bd26-a06b-4abd-a8a5-b4f1d3f5a8f5", "厚院庄园", 2) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalDataOld("7d477695-a2e2-4010-9e64-0b420245596b", "亿臣国际酒店", 2) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalDataOld("917ead54-441e-444c-8927-8257f07b824d", "简阳城市名人酒店·宴会厅", 3) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalDataOld("e1520dbe-8fb0-430c-b2e4-7931309541de", "诺亚方舟(东南门)", 3) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalDataOld("8a19bcfb-909d-4388-9d1f-e59ef9b989c1", "南昌万达文华酒店", 4) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalDataOld("01bb057e-5b65-4a1f-b211-ce8692aa85d7", "巴国布衣(神仙树店)", 4) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalDataOld("427b89cc-795d-4899-8fea-4a14d8f4bd01", "春生", 5) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalDataOld("d280ab29-ee5f-4a3c-a7a4-01468167d5a0", "重庆金陵大饭店", 5) // 直接调用方法，对数据进行获取

    // 第三波策略多面体酒店
    YanjingEngine.saveDfTotalDataOld("dd99abe5-d2f2-4ae0-8c22-579251ed1d7d", "成都金韵酒店", 1) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalDataOld("a335c367-4d9a-4d6b-90c3-fc369703f147", "成都大鼎戴斯大酒店", 1) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalDataOld("97c6330d-26f1-4c67-ae4f-5bbddfb79f9e", "世茂成都茂御酒店", 2) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalDataOld("a9fe4b14-5f45-47c2-bc35-4d36fcdaae05", "交子国际酒店", 2) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalDataOld("7f7ded13-8cf2-44c3-aed0-491fea28a77c", "天邑国际酒店", 3) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalDataOld("d3ede457-bdee-43d5-aac8-15b57a72db7c", "华府金座", 3) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalDataOld("0346b938-2a7b-4b70-bbc4-1bd500547497", "林道假日酒店", 4) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalDataOld("16786afa-f9bc-4e74-848f-bc0fc79e6bf3", "艾克美雅阁大酒店", 4) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalDataOld("462aee34-67b5-4851-aa2e-002aafc2a22c", "西苑半岛", 5) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalDataOld("be621581-25d8-4dbd-847b-da7c72dbfefc", "怡东国际酒店", 5) // 直接调用方法，对数据进行获取

    // 第四波策略多面体酒店
    YanjingEngine.saveDfTotalDataOld("ef0c39be-9675-49dc-a32e-23a31f5db258", "成都希尔顿酒店", 1) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalDataOld("8072ab05-5e85-4276-9ace-ac2851f7a103", "蓉南国际酒店", 1) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalDataOld("a335c367-4d9a-4d6b-90c3-fc369703f147", "成都大鼎戴斯大酒店", 2) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalDataOld("ed8da82f-5aec-40a8-9b24-7ba3c289c0b8", "南堂馆(天府三街店)", 2) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalDataOld("5849ad9a-f16d-417a-a58f-ec51e971cb47", "航宸国际酒店", 3) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalDataOld("8ede682c-8144-4eb7-8050-1b76e5181210", "路易·珀泰夏堡", 3) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalDataOld("9ee77461-0e82-418e-ba27-4bfa7cf2f242", "瑞升·芭富丽大酒店", 4) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalDataOld("792d3a3a-9a15-4846-bd84-b3c480de08ea", "成都新东方千禧大酒店", 4) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalDataOld("d3484e83-4408-41eb-923b-e009672afe55", "成都首座万丽酒店", 5) // 直接调用方法，对数据进行获取
    YanjingEngine.saveDfTotalDataOld("a835f3df-4a27-4c2f-8456-fbab4ccea61b", "成都百悦希尔顿逸林酒店", 5) // 直接调用方法，对数据进行获取

    // 第五波策略多面体酒店
    YanjingEngine.saveDfTotalDataOld("8bda29bb-c63e-4631-a465-e928ccf7525e", "峨眉山大酒店", 1)
    YanjingEngine.saveDfTotalDataOld("e812e6b3-d881-45e2-b5b3-e168bca74ff7", "尚成十八步岛酒店", 1)
    YanjingEngine.saveDfTotalDataOld("a67ad81c-61b3-467a-a162-c7aedf177b46", "金领·莲花大酒店", 2)
    YanjingEngine.saveDfTotalDataOld("852d1616-5c9c-4ca3-8b27-54fa5362f191", "南昌瑞颐大酒店(东门)", 2)
    YanjingEngine.saveDfTotalDataOld("fa409e82-ac70-497c-9d3d-88345e5712ff", "圣瑞云台营山宴会中心", 3)
    YanjingEngine.saveDfTotalDataOld("7c8f6e40-bd80-42ee-9289-a57acd840801", "今日东坡", 3)
    YanjingEngine.saveDfTotalDataOld("f2c3aadf-5530-4d22-921d-ddb206543dbc", "滨江大会堂", 4)
    YanjingEngine.saveDfTotalDataOld("8bda29bb-c63e-4631-a465-e928ccf7525e", "峨眉山大酒店	", 4)
    YanjingEngine.saveDfTotalDataOld("fa409e82-ac70-497c-9d3d-88345e5712ff", "圣瑞云台营山宴会中心", 5)
    YanjingEngine.saveDfTotalDataOld("852d1616-5c9c-4ca3-8b27-54fa5362f191", "南昌瑞颐大酒店(东门)", 5)

    // 第六波策略多面体酒店
    YanjingEngine.saveDfTotalDataOld("4fb48ad2-fcaa-41bd-b148-fde3ddf2bbbd", "宽亭酒楼", 1)
    YanjingEngine.saveDfTotalDataOld("a335c367-4d9a-4d6b-90c3-fc369703f147", "成都大鼎戴斯大酒店", 1)
    YanjingEngine.saveDfTotalDataOld("3e7e916c-2631-4d4f-862a-5edb80859fc5", "成都龙之梦大酒店", 2)
    YanjingEngine.saveDfTotalDataOld("8f0e43e3-42c3-4d5f-821f-b6250332d85d", "安泰安蓉大酒店", 2)
    YanjingEngine.saveDfTotalDataOld("7992b693-5d0b-4c64-9463-e1f22c50805e", "第一江南酒店", 3)
    YanjingEngine.saveDfTotalDataOld("fc2e2086-bd35-4580-a5aa-91257b4116fb", "天和缘", 3)
    YanjingEngine.saveDfTotalDataOld("eccee365-a536-4d6f-be71-64401f7c59c8", "绿洲大酒店", 4)
    YanjingEngine.saveDfTotalDataOld("d7a2d62d-73ab-43c0-aa7e-a28d21a01046", "林恩国际酒店	", 4)
    YanjingEngine.saveDfTotalDataOld("64e6c112-1e4d-4d3d-b3a7-a49344840302", "成都棕榈泉费尔蒙酒店", 5)
    YanjingEngine.saveDfTotalDataOld("e7bd0545-82ce-4c1a-a0c2-0b6ed6307d8c", "成都首座万豪酒店", 5)

    // 第7波策略多面体酒店
    // section1
    YanjingEngine.saveDfTotalDataOld("4a3b30ac-73a9-4c5c-81f4-94be28fbeeec", "不二山房", 1)
    YanjingEngine.saveDfTotalDataOld("57290e8c-adab-48c6-ae2f-96c397a8aa28", "西蜀森林酒店(西南1门)", 1)
    YanjingEngine.saveDfTotalDataOld("5b426558-5264-4749-87ac-cc697ba0cc0a", "红杏酒家(明珠店)", 1)
    YanjingEngine.saveDfTotalDataOld("cc4ee06f-bfbf-4768-a179-c1fda8ef4808", "大蓉和·卓锦酒楼(鹭岛路店)", 1)
    YanjingEngine.saveDfTotalDataOld("98127048-a928-405f-af1e-1501e9be5192", "巴国布衣紫荆店-宴会厅", 1)
    YanjingEngine.saveDfTotalDataOld("a82f0f18-daf8-4f8d-93c1-e26de1b65eb7", "成都合江亭翰文大酒店", 1)
    YanjingEngine.saveDfTotalDataOld("3e7e916c-2631-4d4f-862a-5edb80859fc5", "成都龙之梦大酒店", 1)
    YanjingEngine.saveDfTotalDataOld("2d8043cd-00a0-40a0-b939-53663c409853", "泰合索菲特大饭店", 1)
    YanjingEngine.saveDfTotalDataOld("454f80e9-4ca9-48e9-b0c4-1eac095bcda0", "月圆霖", 1)
    YanjingEngine.saveDfTotalDataOld("5fd54d5a-403a-4c75-9da4-c4e687ef07cf", "迎宾一号", 1)
    // section2
    YanjingEngine.saveDfTotalDataOld("01d3d0fc-a47e-47dc-bf9f-8c1f5492ad24", "郦湾国际酒店", 2)
    YanjingEngine.saveDfTotalDataOld("392359e6-b6eb-4098-bad8-9fc4c43110df", "寅生国际酒店", 2)
    YanjingEngine.saveDfTotalDataOld("02d6ccf1-46e0-4d28-83dd-769bfa8ec4b0", "大蓉和(一品天下旗舰店)", 2)
    YanjingEngine.saveDfTotalDataOld("e22c5f92-70a8-4a81-a6ff-cb3c7a7d87db", "林海山庄(环港路)", 2)
    YanjingEngine.saveDfTotalDataOld("3e47de5d-e076-454d-b0f8-d555064a96c3", "川西人居", 2)
    YanjingEngine.saveDfTotalDataOld("710629f0-31e1-45fd-903c-d670055fb327", "西蜀人家", 2)
    YanjingEngine.saveDfTotalDataOld("29672fa4-4640-4a2f-b0a8-f868d54e67a7", "漫花庄园", 2)
    YanjingEngine.saveDfTotalDataOld("c461e101-23db-4cf0-8824-9fdfb442271c", "成都牧山沁园酒店", 2)
    YanjingEngine.saveDfTotalDataOld("d9105038-f307-431e-bb51-4c25e5335d45", "洁惠花园饭店", 2)
    YanjingEngine.saveDfTotalDataOld("6af69637-105e-4647-b592-937a8742e208", "老房子(毗河店)", 2)
    // section3
    YanjingEngine.saveDfTotalDataOld("bd64ef3a-40ee-42fc-8289-fc57e2f177a4", "映月湖酒店", 3)
    YanjingEngine.saveDfTotalDataOld("057e09d5-9c3d-48c2-8d85-1338ef133625", "川投国际酒店", 3)
    YanjingEngine.saveDfTotalDataOld("da844c8f-c413-4733-8ad8-b3f644a93a98", "友豪·罗曼大酒店", 3)
    YanjingEngine.saveDfTotalDataOld("2dd8454a-16cb-434f-aff7-6eba8454c341", "鹿归国际酒店", 3)
    YanjingEngine.saveDfTotalDataOld("11dcc500-2cef-44d0-bfcd-a70014090031", "桂湖国际大酒店", 3)
    YanjingEngine.saveDfTotalDataOld("3cb310bd-c2bf-4c83-82ab-40280f232e24", "喜鹊先生花园餐厅", 3)
    YanjingEngine.saveDfTotalDataOld("c058f54d-8c4e-4fa7-978e-412f0536c816", "菁华园", 3)
    YanjingEngine.saveDfTotalDataOld("787eaa99-7092-4593-9db6-39353f1e38cd", "顺兴老(世纪城店)", 3)
    YanjingEngine.saveDfTotalDataOld("4032eb26-2b39-4fea-bb9b-fc4491b0b171", "蒋排骨顺湖园", 3)
    YanjingEngine.saveDfTotalDataOld("90c25b75-9ac0-4c8e-a4bb-5c898753859a", "喜馆精品酒店", 3)
    // section4
    YanjingEngine.saveDfTotalDataOld("8996068b-0ceb-46e1-a366-d70282b5eb1d", "智汇堂枫泽大酒店", 4)
    YanjingEngine.saveDfTotalDataOld("c3dc2c80-4571-4ecc-a5f0-9b380cb8a163", "中胜大酒店", 4)
    YanjingEngine.saveDfTotalDataOld("29672fa4-4640-4a2f-b0a8-f868d54e67a7", "漫花庄园", 4)
    YanjingEngine.saveDfTotalDataOld("057e09d5-9c3d-48c2-8d85-1338ef133625", "川投国际酒店", 4)
    YanjingEngine.saveDfTotalDataOld("c6234b4f-aa8e-4949-be1a-075e639f2157", "锦亦缘新派川菜", 4)
    YanjingEngine.saveDfTotalDataOld("16c3174a-0a36-46b9-bbf7-7fde885932fb", "嘉莱特精典国际酒店", 4)
    YanjingEngine.saveDfTotalDataOld("af155b48-3149-45af-8625-16c42d5570e3", "明宇豪雅饭店(科华店)", 4)
    YanjingEngine.saveDfTotalDataOld("724ee82d-57ba-4e70-913a-d868508322a5", "老房子华粹元年食府(天府三街店)", 4)
    YanjingEngine.saveDfTotalDataOld("9ff71e0b-b881-4718-9319-f87f5ce82f40", "重庆澳维酒店(澳维酒店港式茶餐厅)", 4)
    YanjingEngine.saveDfTotalDataOld("3e3e86d6-3b2a-4446-8ae0-bdc4ec158489", "香城竹韵(斑竹园店)", 4)
    // section5
    YanjingEngine.saveDfTotalDataOld("97c6330d-26f1-4c67-ae4f-5bbddfb79f9e", "世茂成都茂御酒店", 5)
    YanjingEngine.saveDfTotalDataOld("5b967168-f07f-4f09-b7c0-55ada1d9dec1", "明宇丽雅悦酒店", 5)
    YanjingEngine.saveDfTotalDataOld("ebb0bdaf-214c-4633-b121-4ebd2da5fc85", "星宸航都国际酒店", 5)
    YanjingEngine.saveDfTotalDataOld("8936bf11-c356-4115-9f5f-c7b6c828b46b", "成都凯宾斯基饭店", 5)
    YanjingEngine.saveDfTotalDataOld("a9fe4b14-5f45-47c2-bc35-4d36fcdaae05", "交子国际酒店", 5)
    YanjingEngine.saveDfTotalDataOld("1fa69245-c5bc-4107-9e06-73ebbe0879c0", "世外桃源酒店", 5)
    YanjingEngine.saveDfTotalDataOld("724ee82d-57ba-4e70-913a-d868508322a5", "老房子华粹元年食府(天府三街店)", 5)
    YanjingEngine.saveDfTotalDataOld("7ccfdc8f-7237-4d8f-a002-f6f235880334", "和淦·香城竹韵", 5)
    YanjingEngine.saveDfTotalDataOld("1158cc09-daa5-4942-a84d-5b2e02238a3c", "家园国际酒店", 5)
    YanjingEngine.saveDfTotalDataOld("b50fcf50-9cfb-4c48-8115-9cc47e89a521", "成都雅居乐豪生大酒店", 5)

    // 第8波策略多面体酒店
    // section1
    YanjingEngine.saveDfTotalDataOld("20c2a7e2-f124-495a-9569-9ba2e3914d37", "大蓉和拉德方斯(精品店)", 1)
    YanjingEngine.saveDfTotalDataOld("37a6faab-3a18-4e89-b68d-cdc756a88da6", "阿斯牛牛·凉山菜(新会展店)", 1)
    YanjingEngine.saveDfTotalDataOld("d3892737-7236-4c7e-9318-7dfd12b53d63", "贵府酒楼(蜀辉路)", 1)
    YanjingEngine.saveDfTotalDataOld("8996068b-0ceb-46e1-a366-d70282b5eb1d", "智汇堂枫泽大酒店", 1)
    YanjingEngine.saveDfTotalDataOld("749d1dff-f0fb-4780-b183-d89c2b1519c1", "上层名人酒店", 1)
    YanjingEngine.saveDfTotalDataOld("c8258e5b-9e19-4573-b63d-6cf59016762c", "文杏酒楼(一品天下店)", 1)
    YanjingEngine.saveDfTotalDataOld("c94f43f4-fbb9-4ab6-8668-bc8678f333b7", "蔚然花海", 1)
    YanjingEngine.saveDfTotalDataOld("09f8e3bd-0053-44c0-85ff-3939690ecb09", "望江宾馆", 1)
    YanjingEngine.saveDfTotalDataOld("02ebfc9f-9180-4a0d-a74f-2e1c13e2fad0", "锦峰大酒店", 1)
    YanjingEngine.saveDfTotalDataOld("724ee82d-57ba-4e70-913a-d868508322a5", "老房子华粹元年食府(天府三街店)", 1)
    // section2
    YanjingEngine.saveDfTotalDataOld("9c4eb1af-4d9f-4bbf-b823-ce2fb08827c9", "红高粱海鲜量贩酒楼(红光店)", 2)
    YanjingEngine.saveDfTotalDataOld("889004a8-70bd-4b99-b669-81167105560d", "智谷云尚丽呈华廷酒店", 2)
    YanjingEngine.saveDfTotalDataOld("7992b693-5d0b-4c64-9463-e1f22c50805e", "第一江南酒店", 2)
    YanjingEngine.saveDfTotalDataOld("3a1baa7f-17f7-4a66-946a-2bf0061c1f20", "红杏酒家(羊西店)", 2)
    YanjingEngine.saveDfTotalDataOld("dc4bb742-6e35-4b9a-9631-58bb035de764", "广都国际酒店", 2)
    YanjingEngine.saveDfTotalDataOld("fd985b07-db6a-4203-a8ef-b4389773b695", "水香雅舍(三圣乡店)", 2)
    YanjingEngine.saveDfTotalDataOld("92905055-163e-4081-a074-727fb00b5cc6", "红杏酒家(万达广场金牛店)", 2)
    YanjingEngine.saveDfTotalDataOld("d6928f86-cdc8-47bb-aa8e-117b1e8a008b", "呈祥·东馆", 2)
    YanjingEngine.saveDfTotalDataOld("ec523414-19bc-499e-9090-64460d76bc77", "杰恩酒店(旗舰店)", 2)
    YanjingEngine.saveDfTotalDataOld("c6ff560b-9819-4321-88a6-5b034b62ce89", "红杏酒家·宴会厅(锦华店)", 2)
    YanjingEngine.saveDfTotalDataOld("e58f3c73-d4a3-4103-a49a-508d2dadd2c5", "红杏酒家(金府店)", 2)
    // section3
    YanjingEngine.saveDfTotalDataOld("1158cc09-daa5-4942-a84d-5b2e02238a3c", "家园国际酒店", 3)
    YanjingEngine.saveDfTotalDataOld("3cb310bd-c2bf-4c83-82ab-40280f232e24", "喜鹊先生花园餐厅", 3)
    YanjingEngine.saveDfTotalDataOld("ccf94653-eaa9-4e07-97ae-9d7da264134e", "新皇城大酒店", 3)
    YanjingEngine.saveDfTotalDataOld("c6234b4f-aa8e-4949-be1a-075e639f2157", "锦亦缘新派川菜", 3)
    YanjingEngine.saveDfTotalDataOld("f6597d6f-9125-4367-b685-2fb3e5510c0a", "友豪锦江酒店", 3)
    YanjingEngine.saveDfTotalDataOld("8936bf11-c356-4115-9f5f-c7b6c828b46b", "成都凯宾斯基饭店", 3)
    YanjingEngine.saveDfTotalDataOld("33d34c3c-a184-48d8-bb30-dd78925d27e3", "闲亭(峨影店)", 3)
    YanjingEngine.saveDfTotalDataOld("5b967168-f07f-4f09-b7c0-55ada1d9dec1", "明宇丽雅悦酒店", 3)
    YanjingEngine.saveDfTotalDataOld("b96db01c-ecd6-4f9a-8cd0-23ff4e0d7512", "应龙湾澜岸酒店", 3)
    YanjingEngine.saveDfTotalDataOld("b32317c0-2613-4a75-aeee-6ac4dc726f83", "玉瑞酒店", 3)
    // section4
    YanjingEngine.saveDfTotalDataOld("5900742b-4290-4bc3-ae8d-4db7b1115557", "泰清锦宴", 4)
    YanjingEngine.saveDfTotalDataOld("ae5d93e3-481a-42fe-9643-c0387321d9ae", "博瑞花园酒店", 4)
    YanjingEngine.saveDfTotalDataOld("c0aa9212-9aed-4801-800d-8c412e972c49", "首席·1956", 4)
    YanjingEngine.saveDfTotalDataOld("72fd3bdd-69d6-4dca-b7cf-d48644ac8e58", "明宇尚雅饭店", 4)
    YanjingEngine.saveDfTotalDataOld("3e0921fb-4957-4eeb-9640-334455227c6e", "西苑半岛酒店", 4)
    YanjingEngine.saveDfTotalDataOld("f59f8c98-5270-4c45-804b-3bcca78efa00", "艺朗酒店", 4)
    YanjingEngine.saveDfTotalDataOld("0346b938-2a7b-4b70-bbc4-1bd500547497", "林道假日酒店", 4)
    YanjingEngine.saveDfTotalDataOld("d889a8d4-c60a-405c-a94d-b783c223560c", "明宇豪雅·怡品堂(东大街店)", 4)
    YanjingEngine.saveDfTotalDataOld("16c90a2c-fd6c-4166-95c6-f1cff3c178e5", "岷江新濠酒店宴会厅", 4)
    YanjingEngine.saveDfTotalDataOld("dd99abe5-d2f2-4ae0-8c22-579251ed1d7d", "成都金韵酒店", 4)
    // section5
    YanjingEngine.saveDfTotalDataOld("09f8e3bd-0053-44c0-85ff-3939690ecb09", "望江宾馆", 5)
    YanjingEngine.saveDfTotalDataOld("39b08285-d287-46ba-abca-70acb0c58898", "南昌万达嘉华酒店", 5)
    YanjingEngine.saveDfTotalDataOld("9d4e835b-5865-4627-bfda-be56199cbcb2", "禧悦酒店", 5)
    YanjingEngine.saveDfTotalDataOld("a0234282-d242-45e3-a5a7-55653be2d667", "南昌力高皇冠假日酒店", 5)
    YanjingEngine.saveDfTotalDataOld("d6a014e2-de76-4285-bb27-b31b1cf339c1", "豪雅东方花园酒店", 5)
    YanjingEngine.saveDfTotalDataOld("8a19bcfb-909d-4388-9d1f-e59ef9b989c1", "南昌万达文华酒店", 5)
    YanjingEngine.saveDfTotalDataOld("ebb0bdaf-214c-4633-b121-4ebd2da5fc85", "星宸航都国际酒店", 5)
    YanjingEngine.saveDfTotalDataOld("1158cc09-daa5-4942-a84d-5b2e02238a3c", "家园国际酒店", 5)
    YanjingEngine.saveDfTotalDataOld("8fe4d72d-8c16-422c-80aa-2c70453e9ca1", "南昌喜来登酒店", 5)
    YanjingEngine.saveDfTotalDataOld("f6597d6f-9125-4367-b685-2fb3e5510c0a", "友豪锦江酒店", 5)
  }

  def updateNewDataNow(): Unit = {
    // 第9波策略多面体酒店
    // section1
    YanjingEngine.saveDfTotalData("b2b26408-4b8c-49e4-98a3-122ce40d9888", "保利国际高尔夫花园", 1)
    YanjingEngine.saveDfTotalData("d3b26bad-787c-4e1e-92f2-1620c1e1733f", "南昌喜来登酒店-大宴会厅", 1)
    YanjingEngine.saveDfTotalData("d6a014e2-de76-4285-bb27-b31b1cf339c1", "豪雅东方花园酒店", 1)
    YanjingEngine.saveDfTotalData("c3d64db1-995f-46ef-a32a-e4acad7469d7", "东方豪景花园酒店", 1)
    YanjingEngine.saveDfTotalData("020f5fef-d0b5-48da-8e03-dee0c61adeea", "银桦半岛酒店", 1)
    YanjingEngine.saveDfTotalData("39b08285-d287-46ba-abca-70acb0c58898", "南昌万达嘉华酒店", 1)
    YanjingEngine.saveDfTotalData("965f750b-bcda-4a51-aedd-94563652c0fe", "成都茂业JW万豪酒店", 1)
    YanjingEngine.saveDfTotalData("20c2a7e2-f124-495a-9569-9ba2e3914d37", "大蓉和拉德方斯(精品店)", 1)
    YanjingEngine.saveDfTotalData("403bc1b9-d66d-4d6a-872c-c9fe736de3f0", "岷山饭店", 1)
    YanjingEngine.saveDfTotalData("d72ca7a2-74d6-4780-9a5b-51ba1284fccf", "诺亚方舟(羊犀店)", 1)
    // section2
    YanjingEngine.saveDfTotalData("c4dfa2b8-a627-46d7-b139-e0d40d952110", "彭州信一雅阁酒店", 2)
    YanjingEngine.saveDfTotalData("85d91ae3-dda3-4d20-bfcb-2e4de544879c", "东篱翠湖", 2)
    YanjingEngine.saveDfTotalData("c86348e9-cfe5-4ccd-961e-bcc7e1b7240f", "成飞宾馆", 2)
    YanjingEngine.saveDfTotalData("51f8208c-2d64-42fa-8ead-705a4fae8f26", "刘家花园", 2)
    YanjingEngine.saveDfTotalData("4e496ec5-9234-4d2c-a448-a1f82ab1d915", "赣江宾馆", 2)
    YanjingEngine.saveDfTotalData("ea98d4eb-f3a5-4685-8984-cad046b720b1", "欢聚一堂", 2)
    YanjingEngine.saveDfTotalData("9fc5ac91-e027-4025-8a5e-7b024670acad", "西蜀森林酒店", 2)
    YanjingEngine.saveDfTotalData("749d1dff-f0fb-4780-b183-d89c2b1519c1", "上层名人酒店", 2)
    YanjingEngine.saveDfTotalData("c0e20114-bf80-426d-8817-a40e6c5853dc", "俏巴渝(爱琴海购物公园店)", 2)
    YanjingEngine.saveDfTotalData("4f0ce7c0-7c40-4ae8-acb9-90ea1a47b6ff", "新华国际酒店", 2)
    // section3
    YanjingEngine.saveDfTotalData("c4dfa2b8-a627-46d7-b139-e0d40d952110", "彭州信一雅阁酒店", 3)
    YanjingEngine.saveDfTotalData("462aee34-67b5-4851-aa2e-002aafc2a22c", "西苑半岛", 3)
    YanjingEngine.saveDfTotalData("43264973-17ad-4ce4-ac2b-15e2a02b7642", "金河宾馆", 3)
    YanjingEngine.saveDfTotalData("85d91ae3-dda3-4d20-bfcb-2e4de544879c", "东篱翠湖", 3)
    YanjingEngine.saveDfTotalData("b22d46ba-a7f4-4da6-9977-0a314778c9e3", "南昌江景假日酒店", 3)
    YanjingEngine.saveDfTotalData("4a3b30ac-73a9-4c5c-81f4-94be28fbeeec", "不二山房", 3)
    YanjingEngine.saveDfTotalData("852d1616-5c9c-4ca3-8b27-54fa5362f191", "南昌瑞颐大酒店(东门)", 3)
    YanjingEngine.saveDfTotalData("9d4e835b-5865-4627-bfda-be56199cbcb2", "禧悦酒店", 3)
    YanjingEngine.saveDfTotalData("a909a7fb-2b8d-4643-a507-6896ea9faf0e", "西藏饭店", 3)
    YanjingEngine.saveDfTotalData("f102bd3f-f02f-4452-8e17-38a409a7b257", "金阳尚城酒店", 3)
    // section4
    YanjingEngine.saveDfTotalData("e812e6b3-d881-45e2-b5b3-e168bca74ff7", "尚成十八步岛酒店", 4)
    YanjingEngine.saveDfTotalData("c5b139d3-57d9-4095-b422-81452d82d158", "南昌香格里拉大酒店", 4)
    YanjingEngine.saveDfTotalData("c0aa9212-9aed-4801-800d-8c412e972c49", "首席·1956", 4)
    YanjingEngine.saveDfTotalData("f6def674-452b-4a16-9030-33d88daf6756", "星辰航都国际酒店销售中心", 4)
    YanjingEngine.saveDfTotalData("92bd6809-7105-4952-8de8-77bae6c896eb", "南昌绿地华邑酒店", 4)
    YanjingEngine.saveDfTotalData("d764c9de-22bc-4c67-a262-3e2ac2b39cb7", "成都空港大酒店", 4)
    YanjingEngine.saveDfTotalData("87395129-729f-46ec-8e22-7322b129c54a", "南昌凯美开元名都大酒店", 4)
    YanjingEngine.saveDfTotalData("c0b134fb-493d-4b07-bed4-c48df9def143", "席锦酒家", 4)
    YanjingEngine.saveDfTotalData("9ee77461-0e82-418e-ba27-4bfa7cf2f242", "瑞升·芭富丽大酒店", 4)
    YanjingEngine.saveDfTotalData("0346b938-2a7b-4b70-bbc4-1bd500547497", "林道假日酒店", 4)
  }

  def updateOldDataNow(): Unit = {
    // 第9波策略多面体酒店
    //    // section1
    //    YanjingEngine.saveDfTotalDataOld("b2b26408-4b8c-49e4-98a3-122ce40d9888", "保利国际高尔夫花园", 1)
    //    YanjingEngine.saveDfTotalDataOld("d3b26bad-787c-4e1e-92f2-1620c1e1733f", "南昌喜来登酒店-大宴会厅", 1)
    //    YanjingEngine.saveDfTotalDataOld("d6a014e2-de76-4285-bb27-b31b1cf339c1", "豪雅东方花园酒店", 1)
    //    YanjingEngine.saveDfTotalDataOld("c3d64db1-995f-46ef-a32a-e4acad7469d7", "东方豪景花园酒店", 1)
    //    YanjingEngine.saveDfTotalDataOld("020f5fef-d0b5-48da-8e03-dee0c61adeea", "银桦半岛酒店", 1)
    //    YanjingEngine.saveDfTotalDataOld("39b08285-d287-46ba-abca-70acb0c58898", "南昌万达嘉华酒店", 1)
    //    YanjingEngine.saveDfTotalDataOld("965f750b-bcda-4a51-aedd-94563652c0fe", "成都茂业JW万豪酒店", 1)
    //    YanjingEngine.saveDfTotalDataOld("20c2a7e2-f124-495a-9569-9ba2e3914d37", "大蓉和拉德方斯(精品店)", 1)
    //    YanjingEngine.saveDfTotalDataOld("403bc1b9-d66d-4d6a-872c-c9fe736de3f0", "岷山饭店", 1)
    //    YanjingEngine.saveDfTotalDataOld("d72ca7a2-74d6-4780-9a5b-51ba1284fccf", "诺亚方舟(羊犀店)", 1)
    //    // section2
    //    YanjingEngine.saveDfTotalDataOld("c4dfa2b8-a627-46d7-b139-e0d40d952110", "彭州信一雅阁酒店", 2)
    //    YanjingEngine.saveDfTotalDataOld("85d91ae3-dda3-4d20-bfcb-2e4de544879c", "东篱翠湖", 2)
    //    YanjingEngine.saveDfTotalDataOld("c86348e9-cfe5-4ccd-961e-bcc7e1b7240f", "成飞宾馆", 2)
    //    YanjingEngine.saveDfTotalDataOld("51f8208c-2d64-42fa-8ead-705a4fae8f26", "刘家花园", 2)
    //    YanjingEngine.saveDfTotalDataOld("4e496ec5-9234-4d2c-a448-a1f82ab1d915", "赣江宾馆", 2)
    //    YanjingEngine.saveDfTotalDataOld("ea98d4eb-f3a5-4685-8984-cad046b720b1", "欢聚一堂", 2)
    //    YanjingEngine.saveDfTotalDataOld("9fc5ac91-e027-4025-8a5e-7b024670acad", "西蜀森林酒店", 2)
    //    YanjingEngine.saveDfTotalDataOld("749d1dff-f0fb-4780-b183-d89c2b1519c1", "上层名人酒店", 2)
    //    YanjingEngine.saveDfTotalDataOld("c0e20114-bf80-426d-8817-a40e6c5853dc", "俏巴渝(爱琴海购物公园店)", 2)
    //    YanjingEngine.saveDfTotalDataOld("4f0ce7c0-7c40-4ae8-acb9-90ea1a47b6ff", "新华国际酒店", 2)
    //    // section3
    //    YanjingEngine.saveDfTotalDataOld("c4dfa2b8-a627-46d7-b139-e0d40d952110", "彭州信一雅阁酒店", 3)
    //    YanjingEngine.saveDfTotalDataOld("462aee34-67b5-4851-aa2e-002aafc2a22c", "西苑半岛", 3)
    //    YanjingEngine.saveDfTotalDataOld("43264973-17ad-4ce4-ac2b-15e2a02b7642", "金河宾馆", 3)
    //    YanjingEngine.saveDfTotalDataOld("85d91ae3-dda3-4d20-bfcb-2e4de544879c", "东篱翠湖", 3)
    //    YanjingEngine.saveDfTotalDataOld("b22d46ba-a7f4-4da6-9977-0a314778c9e3", "南昌江景假日酒店", 3)
    //    YanjingEngine.saveDfTotalDataOld("4a3b30ac-73a9-4c5c-81f4-94be28fbeeec", "不二山房", 3)
    //    YanjingEngine.saveDfTotalDataOld("852d1616-5c9c-4ca3-8b27-54fa5362f191", "南昌瑞颐大酒店(东门)", 3)
    //    YanjingEngine.saveDfTotalDataOld("9d4e835b-5865-4627-bfda-be56199cbcb2", "禧悦酒店", 3)
    //    YanjingEngine.saveDfTotalDataOld("a909a7fb-2b8d-4643-a507-6896ea9faf0e", "西藏饭店", 3)
    //    YanjingEngine.saveDfTotalDataOld("f102bd3f-f02f-4452-8e17-38a409a7b257", "金阳尚城酒店", 3)
    //    // section4
    //    YanjingEngine.saveDfTotalDataOld("e812e6b3-d881-45e2-b5b3-e168bca74ff7", "尚成十八步岛酒店", 4)
    //    YanjingEngine.saveDfTotalDataOld("c5b139d3-57d9-4095-b422-81452d82d158", "南昌香格里拉大酒店", 4)
    //    YanjingEngine.saveDfTotalDataOld("c0aa9212-9aed-4801-800d-8c412e972c49", "首席·1956", 4)
    //    YanjingEngine.saveDfTotalDataOld("f6def674-452b-4a16-9030-33d88daf6756", "星辰航都国际酒店销售中心", 4)
    //    YanjingEngine.saveDfTotalDataOld("92bd6809-7105-4952-8de8-77bae6c896eb", "南昌绿地华邑酒店", 4)
    //    YanjingEngine.saveDfTotalDataOld("d764c9de-22bc-4c67-a262-3e2ac2b39cb7", "成都空港大酒店", 4)
    //    YanjingEngine.saveDfTotalDataOld("87395129-729f-46ec-8e22-7322b129c54a", "南昌凯美开元名都大酒店", 4)
    //    YanjingEngine.saveDfTotalDataOld("c0b134fb-493d-4b07-bed4-c48df9def143", "席锦酒家", 4)
    //    YanjingEngine.saveDfTotalDataOld("9ee77461-0e82-418e-ba27-4bfa7cf2f242", "瑞升·芭富丽大酒店", 4)
    //    YanjingEngine.saveDfTotalDataOld("0346b938-2a7b-4b70-bbc4-1bd500547497", "林道假日酒店", 4)
    YanjingEngine.saveDfTotalDataOld("f12fe4aa-84bc-45bb-b1e0-68a7367b6828", "麓山国际社区", 5)

  }

  /**
    * 03.20新增。眼睛数据读写优化方法
    */
  def readAndUpdateYanJingData(): Unit = {
    val allDataDF = getYanJingOriginalData()
    allDataDF.cache()
    //提供SQLServer连接
    val conn = JavaSQLServerConn.getConnection
    val st: Statement = conn.createStatement()

    // 第一波策略多面体酒店
    updateOldData2SQLServer(allDataDF, st: Statement, "1fa69245-c5bc-4107-9e06-73ebbe0879c0", "世外桃源酒店", 3) // 直接调用方法，对数据进行获取
    // 第二波策略多面体酒店
    updateOldData2SQLServer(allDataDF, st: Statement, "43c7e2bf-4c7a-4af1-94bc-8ed553a23ddb", "正熙雅居酒店", 1) // 直接调用方法，对数据进行获取
    updateOldData2SQLServer(allDataDF, st: Statement, "f59f8c98-5270-4c45-804b-3bcca78efa00", "艺朗酒店", 1) // 直接调用方法，对数据进行获取
    updateOldData2SQLServer(allDataDF, st: Statement, "18b2bd26-a06b-4abd-a8a5-b4f1d3f5a8f5", "厚院庄园", 2) // 直接调用方法，对数据进行获取
    updateOldData2SQLServer(allDataDF, st: Statement, "7d477695-a2e2-4010-9e64-0b420245596b", "亿臣国际酒店", 2) // 直接调用方法，对数据进行获取
    updateOldData2SQLServer(allDataDF, st: Statement, "917ead54-441e-444c-8927-8257f07b824d", "简阳城市名人酒店·宴会厅", 3) // 直接调用方法，对数据进行获取
    updateOldData2SQLServer(allDataDF, st: Statement, "e1520dbe-8fb0-430c-b2e4-7931309541de", "诺亚方舟(东南门)", 3) // 直接调用方法，对数据进行获取
    updateOldData2SQLServer(allDataDF, st: Statement, "8a19bcfb-909d-4388-9d1f-e59ef9b989c1", "南昌万达文华酒店", 4) // 直接调用方法，对数据进行获取
    updateOldData2SQLServer(allDataDF, st: Statement, "01bb057e-5b65-4a1f-b211-ce8692aa85d7", "巴国布衣(神仙树店)", 4) // 直接调用方法，对数据进行获取
    updateOldData2SQLServer(allDataDF, st: Statement, "427b89cc-795d-4899-8fea-4a14d8f4bd01", "春生", 5) // 直接调用方法，对数据进行获取
    updateOldData2SQLServer(allDataDF, st: Statement, "d280ab29-ee5f-4a3c-a7a4-01468167d5a0", "重庆金陵大饭店", 5) // 直接调用方法，对数据进行获取

    // 第三波策略多面体酒店
    updateOldData2SQLServer(allDataDF, st: Statement, "dd99abe5-d2f2-4ae0-8c22-579251ed1d7d", "成都金韵酒店", 1) // 直接调用方法，对数据进行获取
    updateOldData2SQLServer(allDataDF, st: Statement, "a335c367-4d9a-4d6b-90c3-fc369703f147", "成都大鼎戴斯大酒店", 1) // 直接调用方法，对数据进行获取
    updateOldData2SQLServer(allDataDF, st: Statement, "97c6330d-26f1-4c67-ae4f-5bbddfb79f9e", "世茂成都茂御酒店", 2) // 直接调用方法，对数据进行获取
    updateOldData2SQLServer(allDataDF, st: Statement, "a9fe4b14-5f45-47c2-bc35-4d36fcdaae05", "交子国际酒店", 2) // 直接调用方法，对数据进行获取
    updateOldData2SQLServer(allDataDF, st: Statement, "7f7ded13-8cf2-44c3-aed0-491fea28a77c", "天邑国际酒店", 3) // 直接调用方法，对数据进行获取
    updateOldData2SQLServer(allDataDF, st: Statement, "d3ede457-bdee-43d5-aac8-15b57a72db7c", "华府金座", 3) // 直接调用方法，对数据进行获取
    updateOldData2SQLServer(allDataDF, st: Statement, "0346b938-2a7b-4b70-bbc4-1bd500547497", "林道假日酒店", 4) // 直接调用方法，对数据进行获取
    updateOldData2SQLServer(allDataDF, st: Statement, "16786afa-f9bc-4e74-848f-bc0fc79e6bf3", "艾克美雅阁大酒店", 4) // 直接调用方法，对数据进行获取
    updateOldData2SQLServer(allDataDF, st: Statement, "462aee34-67b5-4851-aa2e-002aafc2a22c", "西苑半岛", 5) // 直接调用方法，对数据进行获取
    updateOldData2SQLServer(allDataDF, st: Statement, "be621581-25d8-4dbd-847b-da7c72dbfefc", "怡东国际酒店", 5) // 直接调用方法，对数据进行获取

    // 第四波策略多面体酒店
    updateOldData2SQLServer(allDataDF, st: Statement, "ef0c39be-9675-49dc-a32e-23a31f5db258", "成都希尔顿酒店", 1) // 直接调用方法，对数据进行获取
    updateOldData2SQLServer(allDataDF, st: Statement, "8072ab05-5e85-4276-9ace-ac2851f7a103", "蓉南国际酒店", 1) // 直接调用方法，对数据进行获取
    updateOldData2SQLServer(allDataDF, st: Statement, "a335c367-4d9a-4d6b-90c3-fc369703f147", "成都大鼎戴斯大酒店", 2) // 直接调用方法，对数据进行获取
    updateOldData2SQLServer(allDataDF, st: Statement, "ed8da82f-5aec-40a8-9b24-7ba3c289c0b8", "南堂馆(天府三街店)", 2) // 直接调用方法，对数据进行获取
    updateOldData2SQLServer(allDataDF, st: Statement, "5849ad9a-f16d-417a-a58f-ec51e971cb47", "航宸国际酒店", 3) // 直接调用方法，对数据进行获取
    updateOldData2SQLServer(allDataDF, st: Statement, "8ede682c-8144-4eb7-8050-1b76e5181210", "路易·珀泰夏堡", 3) // 直接调用方法，对数据进行获取
    updateOldData2SQLServer(allDataDF, st: Statement, "9ee77461-0e82-418e-ba27-4bfa7cf2f242", "瑞升·芭富丽大酒店", 4) // 直接调用方法，对数据进行获取
    updateOldData2SQLServer(allDataDF, st: Statement, "792d3a3a-9a15-4846-bd84-b3c480de08ea", "成都新东方千禧大酒店", 4) // 直接调用方法，对数据进行获取
    updateOldData2SQLServer(allDataDF, st: Statement, "d3484e83-4408-41eb-923b-e009672afe55", "成都首座万丽酒店", 5) // 直接调用方法，对数据进行获取
    updateOldData2SQLServer(allDataDF, st: Statement, "a835f3df-4a27-4c2f-8456-fbab4ccea61b", "成都百悦希尔顿逸林酒店", 5) // 直接调用方法，对数据进行获取

    // 第五波策略多面体酒店
    updateOldData2SQLServer(allDataDF, st: Statement, "8bda29bb-c63e-4631-a465-e928ccf7525e", "峨眉山大酒店", 1)
    updateOldData2SQLServer(allDataDF, st: Statement, "e812e6b3-d881-45e2-b5b3-e168bca74ff7", "尚成十八步岛酒店", 1)
    updateOldData2SQLServer(allDataDF, st: Statement, "a67ad81c-61b3-467a-a162-c7aedf177b46", "金领·莲花大酒店", 2)
    updateOldData2SQLServer(allDataDF, st: Statement, "852d1616-5c9c-4ca3-8b27-54fa5362f191", "南昌瑞颐大酒店(东门)", 2)
    updateOldData2SQLServer(allDataDF, st: Statement, "fa409e82-ac70-497c-9d3d-88345e5712ff", "圣瑞云台营山宴会中心", 3)
    updateOldData2SQLServer(allDataDF, st: Statement, "7c8f6e40-bd80-42ee-9289-a57acd840801", "今日东坡", 3)
    updateOldData2SQLServer(allDataDF, st: Statement, "f2c3aadf-5530-4d22-921d-ddb206543dbc", "滨江大会堂", 4)
    updateOldData2SQLServer(allDataDF, st: Statement, "8bda29bb-c63e-4631-a465-e928ccf7525e", "峨眉山大酒店	", 4)
    updateOldData2SQLServer(allDataDF, st: Statement, "fa409e82-ac70-497c-9d3d-88345e5712ff", "圣瑞云台营山宴会中心", 5)
    updateOldData2SQLServer(allDataDF, st: Statement, "852d1616-5c9c-4ca3-8b27-54fa5362f191", "南昌瑞颐大酒店(东门)", 5)

    // 第六波策略多面体酒店
    updateOldData2SQLServer(allDataDF, st: Statement, "4fb48ad2-fcaa-41bd-b148-fde3ddf2bbbd", "宽亭酒楼", 1)
    updateOldData2SQLServer(allDataDF, st: Statement, "a335c367-4d9a-4d6b-90c3-fc369703f147", "成都大鼎戴斯大酒店", 1)
    updateOldData2SQLServer(allDataDF, st: Statement, "3e7e916c-2631-4d4f-862a-5edb80859fc5", "成都龙之梦大酒店", 2)
    updateOldData2SQLServer(allDataDF, st: Statement, "8f0e43e3-42c3-4d5f-821f-b6250332d85d", "安泰安蓉大酒店", 2)
    updateOldData2SQLServer(allDataDF, st: Statement, "7992b693-5d0b-4c64-9463-e1f22c50805e", "第一江南酒店", 3)
    updateOldData2SQLServer(allDataDF, st: Statement, "fc2e2086-bd35-4580-a5aa-91257b4116fb", "天和缘", 3)
    updateOldData2SQLServer(allDataDF, st: Statement, "eccee365-a536-4d6f-be71-64401f7c59c8", "绿洲大酒店", 4)
    updateOldData2SQLServer(allDataDF, st: Statement, "d7a2d62d-73ab-43c0-aa7e-a28d21a01046", "林恩国际酒店	", 4)
    updateOldData2SQLServer(allDataDF, st: Statement, "64e6c112-1e4d-4d3d-b3a7-a49344840302", "成都棕榈泉费尔蒙酒店", 5)
    updateOldData2SQLServer(allDataDF, st: Statement, "e7bd0545-82ce-4c1a-a0c2-0b6ed6307d8c", "成都首座万豪酒店", 5)

    // 第7波策略多面体酒店
    // section1
    updateOldData2SQLServer(allDataDF, st: Statement, "4a3b30ac-73a9-4c5c-81f4-94be28fbeeec", "不二山房", 1)
    updateOldData2SQLServer(allDataDF, st: Statement, "57290e8c-adab-48c6-ae2f-96c397a8aa28", "西蜀森林酒店(西南1门)", 1)
    updateOldData2SQLServer(allDataDF, st: Statement, "5b426558-5264-4749-87ac-cc697ba0cc0a", "红杏酒家(明珠店)", 1)
    updateOldData2SQLServer(allDataDF, st: Statement, "cc4ee06f-bfbf-4768-a179-c1fda8ef4808", "大蓉和·卓锦酒楼(鹭岛路店)", 1)
    updateOldData2SQLServer(allDataDF, st: Statement, "98127048-a928-405f-af1e-1501e9be5192", "巴国布衣紫荆店-宴会厅", 1)
    updateOldData2SQLServer(allDataDF, st: Statement, "a82f0f18-daf8-4f8d-93c1-e26de1b65eb7", "成都合江亭翰文大酒店", 1)
    updateOldData2SQLServer(allDataDF, st: Statement, "3e7e916c-2631-4d4f-862a-5edb80859fc5", "成都龙之梦大酒店", 1)
    updateOldData2SQLServer(allDataDF, st: Statement, "2d8043cd-00a0-40a0-b939-53663c409853", "泰合索菲特大饭店", 1)
    updateOldData2SQLServer(allDataDF, st: Statement, "454f80e9-4ca9-48e9-b0c4-1eac095bcda0", "月圆霖", 1)
    updateOldData2SQLServer(allDataDF, st: Statement, "5fd54d5a-403a-4c75-9da4-c4e687ef07cf", "迎宾一号", 1)
    // section2
    updateOldData2SQLServer(allDataDF, st: Statement, "01d3d0fc-a47e-47dc-bf9f-8c1f5492ad24", "郦湾国际酒店", 2)
    updateOldData2SQLServer(allDataDF, st: Statement, "392359e6-b6eb-4098-bad8-9fc4c43110df", "寅生国际酒店", 2)
    updateOldData2SQLServer(allDataDF, st: Statement, "02d6ccf1-46e0-4d28-83dd-769bfa8ec4b0", "大蓉和(一品天下旗舰店)", 2)
    updateOldData2SQLServer(allDataDF, st: Statement, "e22c5f92-70a8-4a81-a6ff-cb3c7a7d87db", "林海山庄(环港路)", 2)
    updateOldData2SQLServer(allDataDF, st: Statement, "3e47de5d-e076-454d-b0f8-d555064a96c3", "川西人居", 2)
    updateOldData2SQLServer(allDataDF, st: Statement, "710629f0-31e1-45fd-903c-d670055fb327", "西蜀人家", 2)
    updateOldData2SQLServer(allDataDF, st: Statement, "29672fa4-4640-4a2f-b0a8-f868d54e67a7", "漫花庄园", 2)
    updateOldData2SQLServer(allDataDF, st: Statement, "c461e101-23db-4cf0-8824-9fdfb442271c", "成都牧山沁园酒店", 2)
    updateOldData2SQLServer(allDataDF, st: Statement, "d9105038-f307-431e-bb51-4c25e5335d45", "洁惠花园饭店", 2)
    updateOldData2SQLServer(allDataDF, st: Statement, "6af69637-105e-4647-b592-937a8742e208", "老房子(毗河店)", 2)
    // section3
    updateOldData2SQLServer(allDataDF, st: Statement, "bd64ef3a-40ee-42fc-8289-fc57e2f177a4", "映月湖酒店", 3)
    updateOldData2SQLServer(allDataDF, st: Statement, "057e09d5-9c3d-48c2-8d85-1338ef133625", "川投国际酒店", 3)
    updateOldData2SQLServer(allDataDF, st: Statement, "da844c8f-c413-4733-8ad8-b3f644a93a98", "友豪·罗曼大酒店", 3)
    updateOldData2SQLServer(allDataDF, st: Statement, "2dd8454a-16cb-434f-aff7-6eba8454c341", "鹿归国际酒店", 3)
    updateOldData2SQLServer(allDataDF, st: Statement, "11dcc500-2cef-44d0-bfcd-a70014090031", "桂湖国际大酒店", 3)
    updateOldData2SQLServer(allDataDF, st: Statement, "3cb310bd-c2bf-4c83-82ab-40280f232e24", "喜鹊先生花园餐厅", 3)
    updateOldData2SQLServer(allDataDF, st: Statement, "c058f54d-8c4e-4fa7-978e-412f0536c816", "菁华园", 3)
    updateOldData2SQLServer(allDataDF, st: Statement, "787eaa99-7092-4593-9db6-39353f1e38cd", "顺兴老(世纪城店)", 3)
    updateOldData2SQLServer(allDataDF, st: Statement, "4032eb26-2b39-4fea-bb9b-fc4491b0b171", "蒋排骨顺湖园", 3)
    updateOldData2SQLServer(allDataDF, st: Statement, "90c25b75-9ac0-4c8e-a4bb-5c898753859a", "喜馆精品酒店", 3)
    // section4
    updateOldData2SQLServer(allDataDF, st: Statement, "8996068b-0ceb-46e1-a366-d70282b5eb1d", "智汇堂枫泽大酒店", 4)
    updateOldData2SQLServer(allDataDF, st: Statement, "c3dc2c80-4571-4ecc-a5f0-9b380cb8a163", "中胜大酒店", 4)
    updateOldData2SQLServer(allDataDF, st: Statement, "29672fa4-4640-4a2f-b0a8-f868d54e67a7", "漫花庄园", 4)
    updateOldData2SQLServer(allDataDF, st: Statement, "057e09d5-9c3d-48c2-8d85-1338ef133625", "川投国际酒店", 4)
    updateOldData2SQLServer(allDataDF, st: Statement, "c6234b4f-aa8e-4949-be1a-075e639f2157", "锦亦缘新派川菜", 4)
    updateOldData2SQLServer(allDataDF, st: Statement, "16c3174a-0a36-46b9-bbf7-7fde885932fb", "嘉莱特精典国际酒店", 4)
    updateOldData2SQLServer(allDataDF, st: Statement, "af155b48-3149-45af-8625-16c42d5570e3", "明宇豪雅饭店(科华店)", 4)
    updateOldData2SQLServer(allDataDF, st: Statement, "724ee82d-57ba-4e70-913a-d868508322a5", "老房子华粹元年食府(天府三街店)", 4)
    updateOldData2SQLServer(allDataDF, st: Statement, "9ff71e0b-b881-4718-9319-f87f5ce82f40", "重庆澳维酒店(澳维酒店港式茶餐厅)", 4)
    updateOldData2SQLServer(allDataDF, st: Statement, "3e3e86d6-3b2a-4446-8ae0-bdc4ec158489", "香城竹韵(斑竹园店)", 4)
    // section5
    updateOldData2SQLServer(allDataDF, st: Statement, "97c6330d-26f1-4c67-ae4f-5bbddfb79f9e", "世茂成都茂御酒店", 5)
    updateOldData2SQLServer(allDataDF, st: Statement, "5b967168-f07f-4f09-b7c0-55ada1d9dec1", "明宇丽雅悦酒店", 5)
    updateOldData2SQLServer(allDataDF, st: Statement, "ebb0bdaf-214c-4633-b121-4ebd2da5fc85", "星宸航都国际酒店", 5)
    updateOldData2SQLServer(allDataDF, st: Statement, "8936bf11-c356-4115-9f5f-c7b6c828b46b", "成都凯宾斯基饭店", 5)
    updateOldData2SQLServer(allDataDF, st: Statement, "a9fe4b14-5f45-47c2-bc35-4d36fcdaae05", "交子国际酒店", 5)
    updateOldData2SQLServer(allDataDF, st: Statement, "1fa69245-c5bc-4107-9e06-73ebbe0879c0", "世外桃源酒店", 5)
    updateOldData2SQLServer(allDataDF, st: Statement, "724ee82d-57ba-4e70-913a-d868508322a5", "老房子华粹元年食府(天府三街店)", 5)
    updateOldData2SQLServer(allDataDF, st: Statement, "7ccfdc8f-7237-4d8f-a002-f6f235880334", "和淦·香城竹韵", 5)
    updateOldData2SQLServer(allDataDF, st: Statement, "1158cc09-daa5-4942-a84d-5b2e02238a3c", "家园国际酒店", 5)
    updateOldData2SQLServer(allDataDF, st: Statement, "b50fcf50-9cfb-4c48-8115-9cc47e89a521", "成都雅居乐豪生大酒店", 5)

    // 第8波策略多面体酒店
    // section1
    updateOldData2SQLServer(allDataDF, st: Statement, "20c2a7e2-f124-495a-9569-9ba2e3914d37", "大蓉和拉德方斯(精品店)", 1)
    updateOldData2SQLServer(allDataDF, st: Statement, "37a6faab-3a18-4e89-b68d-cdc756a88da6", "阿斯牛牛·凉山菜(新会展店)", 1)
    updateOldData2SQLServer(allDataDF, st: Statement, "d3892737-7236-4c7e-9318-7dfd12b53d63", "贵府酒楼(蜀辉路)", 1)
    updateOldData2SQLServer(allDataDF, st: Statement, "8996068b-0ceb-46e1-a366-d70282b5eb1d", "智汇堂枫泽大酒店", 1)
    updateOldData2SQLServer(allDataDF, st: Statement, "749d1dff-f0fb-4780-b183-d89c2b1519c1", "上层名人酒店", 1)
    updateOldData2SQLServer(allDataDF, st: Statement, "c8258e5b-9e19-4573-b63d-6cf59016762c", "文杏酒楼(一品天下店)", 1)
    updateOldData2SQLServer(allDataDF, st: Statement, "c94f43f4-fbb9-4ab6-8668-bc8678f333b7", "蔚然花海", 1)
    updateOldData2SQLServer(allDataDF, st: Statement, "09f8e3bd-0053-44c0-85ff-3939690ecb09", "望江宾馆", 1)
    updateOldData2SQLServer(allDataDF, st: Statement, "02ebfc9f-9180-4a0d-a74f-2e1c13e2fad0", "锦峰大酒店", 1)
    updateOldData2SQLServer(allDataDF, st: Statement, "724ee82d-57ba-4e70-913a-d868508322a5", "老房子华粹元年食府(天府三街店)", 1)
    // section2
    updateOldData2SQLServer(allDataDF, st: Statement, "9c4eb1af-4d9f-4bbf-b823-ce2fb08827c9", "红高粱海鲜量贩酒楼(红光店)", 2)
    updateOldData2SQLServer(allDataDF, st: Statement, "889004a8-70bd-4b99-b669-81167105560d", "智谷云尚丽呈华廷酒店", 2)
    updateOldData2SQLServer(allDataDF, st: Statement, "7992b693-5d0b-4c64-9463-e1f22c50805e", "第一江南酒店", 2)
    updateOldData2SQLServer(allDataDF, st: Statement, "3a1baa7f-17f7-4a66-946a-2bf0061c1f20", "红杏酒家(羊西店)", 2)
    updateOldData2SQLServer(allDataDF, st: Statement, "dc4bb742-6e35-4b9a-9631-58bb035de764", "广都国际酒店", 2)
    updateOldData2SQLServer(allDataDF, st: Statement, "fd985b07-db6a-4203-a8ef-b4389773b695", "水香雅舍(三圣乡店)", 2)
    updateOldData2SQLServer(allDataDF, st: Statement, "92905055-163e-4081-a074-727fb00b5cc6", "红杏酒家(万达广场金牛店)", 2)
    updateOldData2SQLServer(allDataDF, st: Statement, "d6928f86-cdc8-47bb-aa8e-117b1e8a008b", "呈祥·东馆", 2)
    updateOldData2SQLServer(allDataDF, st: Statement, "ec523414-19bc-499e-9090-64460d76bc77", "杰恩酒店(旗舰店)", 2)
    updateOldData2SQLServer(allDataDF, st: Statement, "c6ff560b-9819-4321-88a6-5b034b62ce89", "红杏酒家·宴会厅(锦华店)", 2)
    updateOldData2SQLServer(allDataDF, st: Statement, "e58f3c73-d4a3-4103-a49a-508d2dadd2c5", "红杏酒家(金府店)", 2)
    // section3
    updateOldData2SQLServer(allDataDF, st: Statement, "1158cc09-daa5-4942-a84d-5b2e02238a3c", "家园国际酒店", 3)
    updateOldData2SQLServer(allDataDF, st: Statement, "3cb310bd-c2bf-4c83-82ab-40280f232e24", "喜鹊先生花园餐厅", 3)
    updateOldData2SQLServer(allDataDF, st: Statement, "ccf94653-eaa9-4e07-97ae-9d7da264134e", "新皇城大酒店", 3)
    updateOldData2SQLServer(allDataDF, st: Statement, "c6234b4f-aa8e-4949-be1a-075e639f2157", "锦亦缘新派川菜", 3)
    updateOldData2SQLServer(allDataDF, st: Statement, "f6597d6f-9125-4367-b685-2fb3e5510c0a", "友豪锦江酒店", 3)
    updateOldData2SQLServer(allDataDF, st: Statement, "8936bf11-c356-4115-9f5f-c7b6c828b46b", "成都凯宾斯基饭店", 3)
    updateOldData2SQLServer(allDataDF, st: Statement, "33d34c3c-a184-48d8-bb30-dd78925d27e3", "闲亭(峨影店)", 3)
    updateOldData2SQLServer(allDataDF, st: Statement, "5b967168-f07f-4f09-b7c0-55ada1d9dec1", "明宇丽雅悦酒店", 3)
    updateOldData2SQLServer(allDataDF, st: Statement, "b96db01c-ecd6-4f9a-8cd0-23ff4e0d7512", "应龙湾澜岸酒店", 3)
    updateOldData2SQLServer(allDataDF, st: Statement, "b32317c0-2613-4a75-aeee-6ac4dc726f83", "玉瑞酒店", 3)
    // section4
    updateOldData2SQLServer(allDataDF, st: Statement, "5900742b-4290-4bc3-ae8d-4db7b1115557", "泰清锦宴", 4)
    updateOldData2SQLServer(allDataDF, st: Statement, "ae5d93e3-481a-42fe-9643-c0387321d9ae", "博瑞花园酒店", 4)
    updateOldData2SQLServer(allDataDF, st: Statement, "c0aa9212-9aed-4801-800d-8c412e972c49", "首席·1956", 4)
    updateOldData2SQLServer(allDataDF, st: Statement, "72fd3bdd-69d6-4dca-b7cf-d48644ac8e58", "明宇尚雅饭店", 4)
    updateOldData2SQLServer(allDataDF, st: Statement, "3e0921fb-4957-4eeb-9640-334455227c6e", "西苑半岛酒店", 4)
    updateOldData2SQLServer(allDataDF, st: Statement, "f59f8c98-5270-4c45-804b-3bcca78efa00", "艺朗酒店", 4)
    updateOldData2SQLServer(allDataDF, st: Statement, "0346b938-2a7b-4b70-bbc4-1bd500547497", "林道假日酒店", 4)
    updateOldData2SQLServer(allDataDF, st: Statement, "d889a8d4-c60a-405c-a94d-b783c223560c", "明宇豪雅·怡品堂(东大街店)", 4)
    updateOldData2SQLServer(allDataDF, st: Statement, "16c90a2c-fd6c-4166-95c6-f1cff3c178e5", "岷江新濠酒店宴会厅", 4)
    updateOldData2SQLServer(allDataDF, st: Statement, "dd99abe5-d2f2-4ae0-8c22-579251ed1d7d", "成都金韵酒店", 4)
    // section5
    updateOldData2SQLServer(allDataDF, st: Statement, "09f8e3bd-0053-44c0-85ff-3939690ecb09", "望江宾馆", 5)
    updateOldData2SQLServer(allDataDF, st: Statement, "39b08285-d287-46ba-abca-70acb0c58898", "南昌万达嘉华酒店", 5)
    updateOldData2SQLServer(allDataDF, st: Statement, "9d4e835b-5865-4627-bfda-be56199cbcb2", "禧悦酒店", 5)
    updateOldData2SQLServer(allDataDF, st: Statement, "a0234282-d242-45e3-a5a7-55653be2d667", "南昌力高皇冠假日酒店", 5)
    updateOldData2SQLServer(allDataDF, st: Statement, "d6a014e2-de76-4285-bb27-b31b1cf339c1", "豪雅东方花园酒店", 5)
    updateOldData2SQLServer(allDataDF, st: Statement, "8a19bcfb-909d-4388-9d1f-e59ef9b989c1", "南昌万达文华酒店", 5)
    updateOldData2SQLServer(allDataDF, st: Statement, "ebb0bdaf-214c-4633-b121-4ebd2da5fc85", "星宸航都国际酒店", 5)
    updateOldData2SQLServer(allDataDF, st: Statement, "1158cc09-daa5-4942-a84d-5b2e02238a3c", "家园国际酒店", 5)
    updateOldData2SQLServer(allDataDF, st: Statement, "8fe4d72d-8c16-422c-80aa-2c70453e9ca1", "南昌喜来登酒店", 5)
    updateOldData2SQLServer(allDataDF, st: Statement, "f6597d6f-9125-4367-b685-2fb3e5510c0a", "友豪锦江酒店", 5)

    // 第9波策略多面体酒店
    // section1
    updateOldData2SQLServer(allDataDF, st: Statement, "b2b26408-4b8c-49e4-98a3-122ce40d9888", "保利国际高尔夫花园", 1)
    updateOldData2SQLServer(allDataDF, st: Statement, "b2b26408-4b8c-49e4-98a3-122ce40d9888", "保利国际高尔夫花园", 1)
    updateOldData2SQLServer(allDataDF, st: Statement, "d3b26bad-787c-4e1e-92f2-1620c1e1733f", "南昌喜来登酒店-大宴会厅", 1)
    updateOldData2SQLServer(allDataDF, st: Statement, "d6a014e2-de76-4285-bb27-b31b1cf339c1", "豪雅东方花园酒店", 1)
    updateOldData2SQLServer(allDataDF, st: Statement, "c3d64db1-995f-46ef-a32a-e4acad7469d7", "东方豪景花园酒店", 1)
    updateOldData2SQLServer(allDataDF, st: Statement, "020f5fef-d0b5-48da-8e03-dee0c61adeea", "银桦半岛酒店", 1)
    updateOldData2SQLServer(allDataDF, st: Statement, "39b08285-d287-46ba-abca-70acb0c58898", "南昌万达嘉华酒店", 1)
    updateOldData2SQLServer(allDataDF, st: Statement, "965f750b-bcda-4a51-aedd-94563652c0fe", "成都茂业JW万豪酒店", 1)
    updateOldData2SQLServer(allDataDF, st: Statement, "20c2a7e2-f124-495a-9569-9ba2e3914d37", "大蓉和拉德方斯(精品店)", 1)
    updateOldData2SQLServer(allDataDF, st: Statement, "403bc1b9-d66d-4d6a-872c-c9fe736de3f0", "岷山饭店", 1)
    updateOldData2SQLServer(allDataDF, st: Statement, "d72ca7a2-74d6-4780-9a5b-51ba1284fccf", "诺亚方舟(羊犀店)", 1)
    // section2
    updateOldData2SQLServer(allDataDF, st: Statement, "c4dfa2b8-a627-46d7-b139-e0d40d952110", "彭州信一雅阁酒店", 2)
    updateOldData2SQLServer(allDataDF, st: Statement, "85d91ae3-dda3-4d20-bfcb-2e4de544879c", "东篱翠湖", 2)
    updateOldData2SQLServer(allDataDF, st: Statement, "c86348e9-cfe5-4ccd-961e-bcc7e1b7240f", "成飞宾馆", 2)
    updateOldData2SQLServer(allDataDF, st: Statement, "51f8208c-2d64-42fa-8ead-705a4fae8f26", "刘家花园", 2)
    updateOldData2SQLServer(allDataDF, st: Statement, "4e496ec5-9234-4d2c-a448-a1f82ab1d915", "赣江宾馆", 2)
    updateOldData2SQLServer(allDataDF, st: Statement, "ea98d4eb-f3a5-4685-8984-cad046b720b1", "欢聚一堂", 2)
    updateOldData2SQLServer(allDataDF, st: Statement, "9fc5ac91-e027-4025-8a5e-7b024670acad", "西蜀森林酒店", 2)
    updateOldData2SQLServer(allDataDF, st: Statement, "749d1dff-f0fb-4780-b183-d89c2b1519c1", "上层名人酒店", 2)
    updateOldData2SQLServer(allDataDF, st: Statement, "c0e20114-bf80-426d-8817-a40e6c5853dc", "俏巴渝(爱琴海购物公园店)", 2)
    updateOldData2SQLServer(allDataDF, st: Statement, "4f0ce7c0-7c40-4ae8-acb9-90ea1a47b6ff", "新华国际酒店", 2)
    // section3
    updateOldData2SQLServer(allDataDF, st: Statement, "c4dfa2b8-a627-46d7-b139-e0d40d952110", "彭州信一雅阁酒店", 3)
    updateOldData2SQLServer(allDataDF, st: Statement, "462aee34-67b5-4851-aa2e-002aafc2a22c", "西苑半岛", 3)
    updateOldData2SQLServer(allDataDF, st: Statement, "43264973-17ad-4ce4-ac2b-15e2a02b7642", "金河宾馆", 3)
    updateOldData2SQLServer(allDataDF, st: Statement, "85d91ae3-dda3-4d20-bfcb-2e4de544879c", "东篱翠湖", 3)
    updateOldData2SQLServer(allDataDF, st: Statement, "b22d46ba-a7f4-4da6-9977-0a314778c9e3", "南昌江景假日酒店", 3)
    updateOldData2SQLServer(allDataDF, st: Statement, "4a3b30ac-73a9-4c5c-81f4-94be28fbeeec", "不二山房", 3)
    updateOldData2SQLServer(allDataDF, st: Statement, "852d1616-5c9c-4ca3-8b27-54fa5362f191", "南昌瑞颐大酒店(东门)", 3)
    updateOldData2SQLServer(allDataDF, st: Statement, "9d4e835b-5865-4627-bfda-be56199cbcb2", "禧悦酒店", 3)
    updateOldData2SQLServer(allDataDF, st: Statement, "a909a7fb-2b8d-4643-a507-6896ea9faf0e", "西藏饭店", 3)
    updateOldData2SQLServer(allDataDF, st: Statement, "f102bd3f-f02f-4452-8e17-38a409a7b257", "金阳尚城酒店", 3)
    // section4
    updateOldData2SQLServer(allDataDF, st: Statement, "e812e6b3-d881-45e2-b5b3-e168bca74ff7", "尚成十八步岛酒店", 4)
    updateOldData2SQLServer(allDataDF, st: Statement, "c5b139d3-57d9-4095-b422-81452d82d158", "南昌香格里拉大酒店", 4)
    updateOldData2SQLServer(allDataDF, st: Statement, "c0aa9212-9aed-4801-800d-8c412e972c49", "首席·1956", 4)
    updateOldData2SQLServer(allDataDF, st: Statement, "f6def674-452b-4a16-9030-33d88daf6756", "星辰航都国际酒店销售中心", 4)
    updateOldData2SQLServer(allDataDF, st: Statement, "92bd6809-7105-4952-8de8-77bae6c896eb", "南昌绿地华邑酒店", 4)
    updateOldData2SQLServer(allDataDF, st: Statement, "d764c9de-22bc-4c67-a262-3e2ac2b39cb7", "成都空港大酒店", 4)
    updateOldData2SQLServer(allDataDF, st: Statement, "87395129-729f-46ec-8e22-7322b129c54a", "南昌凯美开元名都大酒店", 4)
    updateOldData2SQLServer(allDataDF, st: Statement, "c0b134fb-493d-4b07-bed4-c48df9def143", "席锦酒家", 4)
    updateOldData2SQLServer(allDataDF, st: Statement, "9ee77461-0e82-418e-ba27-4bfa7cf2f242", "瑞升·芭富丽大酒店", 4)
    updateOldData2SQLServer(allDataDF, st: Statement, "0346b938-2a7b-4b70-bbc4-1bd500547497", "林道假日酒店", 4)

    // section5
    updateOldData2SQLServer(allDataDF, st: Statement, "f12fe4aa-84bc-45bb-b1e0-68a7367b6828", "麓山国际社区", 5)

    allDataDF.unpersist()

    //    断开SQLServer连接
    JavaSQLServerConn.closeConnection(conn)
  }

  // 03.20附带方法。读取原始数据，并进行缓存
  def getYanJingOriginalData(): DataFrame = {
    val tbLowBudgetTypeDF = spark.read.format("jdbc")
      .option("url", JavaRealLeaderProperties.MYSQLURL)
      .option("dbtable", "tb_low_budget_type")
      .option("user", JavaRealLeaderProperties.MYSQLUSER)
      .option("password", JavaRealLeaderProperties.MYSQLPASSWORD)
      .load()
      .withColumnRenamed("到店签单率", "to_store_rate")
      .withColumnRenamed("策划师文字评价率", "text_rating_rate")
    tbLowBudgetTypeDF.createOrReplaceTempView("bbb")

    val dfTotalDF = spark.read.format("jdbc")
      .option("url", JavaRealLeaderProperties.MYSQLURL)
      .option("dbtable", "df_total")
      .option("user", JavaRealLeaderProperties.MYSQLUSER)
      .option("password", JavaRealLeaderProperties.MYSQLPASSWORD)
      .load()
    dfTotalDF.createOrReplaceTempView("ddd")

    // todo 从这一步开始，就会出现对section进行限定的查询
    val allNeedDf = spark.sql("select bbb.worker_id,ddd.section,bbb.case_dot,bbb.reorder_rate,bbb.communication_level,bbb.design_sense,bbb.case_rate,bbb.all_score_final,bbb.number,bbb.to_store_rate,bbb.text_rating_rate,ddd.display_amount from bbb left join ddd on bbb.worker_id = ddd.worker_id").distinct()
    allNeedDf
  }

  // 03.20附带方法。针对每个小市场进行数据列读取和数据统计并写入到SQLServer。
  def updateOldData2SQLServer(allDataDF: DataFrame, st: Statement, hotel_id: String, hotel_name: String, section: Int): Unit = {
    val needDf = allDataDF.filter(row => {
      row.getAs[Long]("section") == section
    }).filter(row => {
      val worker_id = row.getAs[String]("worker_id")
      val distance = JavaHBaseUtils.getValue("v2_rp_workers_hotel_distance", hotel_id + ":" + worker_id, "info", "distance")
      val distanceKM = (distance.toFloat / 1000)
      distanceKM <= 20
    })

    needDf.cache()

    // 1.todo A	方案点：                case_dot
    val caseDotList: List[Double] = needDf.select("case_dot").rdd.map(row => row.getAs[Double]("case_dot")).collect().toList
    val allInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
    //    val caseDotInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
    val caseDotMap = SomeUtils.getRightIntervalBySetting(caseDotList, allInterval)
    //    caseDotMap.toArray.sortBy(_._1.toDouble).toMap  // 针对数值进行排序

    caseDotMap.foreach(a => {
      val case_dot = JavaSomeUtils.get2Double(a._1.toDouble)
      val num = a._2
      val other = "Vj ≠ 1"
      val sql = s"insert into canNum_v1_case_dot(hotel_id,hotel_name,section,case_dot,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$case_dot,'$other',$num,getdate());"
      st.executeUpdate(sql)
    })

    // 2.todo B 回单率：                reorder_rate
    val reorderRateList: List[Double] = needDf.select("reorder_rate").rdd.map(row => row.getAs[Double]("reorder_rate")).collect().toList
    //    val reorderRateInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
    val reorderRateMap = SomeUtils.getRightIntervalBySetting(reorderRateList, allInterval)

    reorderRateMap.foreach(a => {
      val case_dot = JavaSomeUtils.get2Double(a._1.toDouble)
      val num = a._2
      val other = "Vj ≠ 2"
      val sql = s"insert into canNum_v2_reorder_rate(hotel_id,hotel_name,section,reorder_rate,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$case_dot,'$other',$num,getdate());"
      st.executeUpdate(sql)
    })

    // 3.todo C 沟通水平：                communication_level
    val communicationLevelList: List[Double] = needDf.select("communication_level").rdd.map(row => row.getAs[Double]("communication_level")).collect().toList
    //    val communicationLevelInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
    val communicationLevelMap = SomeUtils.getRightIntervalBySetting(communicationLevelList, allInterval)

    communicationLevelMap.foreach(a => {
      val communication_level = JavaSomeUtils.get2Double(a._1.toDouble)
      val num = a._2
      val other = "Vj ≠ 3"
      val sql = s"insert into canNum_v3_communication_level(hotel_id,hotel_name,section,communication_level,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$communication_level,'$other',$num,getdate());"
      st.executeUpdate(sql)
    })

    // 4.todo D 设计感：                design_sense
    val designSenseList: List[Double] = needDf.select("design_sense").rdd.map(row => row.getAs[Double]("design_sense")).collect().toList
    //    val designSenseInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
    val designSenseMap = SomeUtils.getRightIntervalBySetting(designSenseList, allInterval)

    designSenseMap.foreach(a => {
      val design_sense = JavaSomeUtils.get2Double(a._1.toDouble)
      val num = a._2
      val other = "Vj ≠ 4"
      val sql = s"insert into canNum_v4_design_sense(hotel_id,hotel_name,section,design_sense,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$design_sense,'$other',$num,getdate());"
      st.executeUpdate(sql)
    })

    // 5.todo E 设计感：                case_rate
    val caseRateList: List[Double] = needDf.select("case_rate").rdd.map(row => row.getAs[Double]("case_rate")).collect().toList
    //    val caseRateInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
    val caseRateMap = SomeUtils.getRightIntervalBySetting(caseRateList, allInterval)

    caseRateMap.foreach(a => {
      val case_rate = JavaSomeUtils.get2Double(a._1.toDouble)
      val num = a._2
      val other = "Vj ≠ 5"
      val sql = s"insert into canNum_v5_case_rate(hotel_id,hotel_name,section,case_rate,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$case_rate,'$other',$num,getdate());"
      st.executeUpdate(sql)
    })

    // 6.todo F 设计感：                all_score_final
    val allScoreFinalList: List[Double] = needDf.select("all_score_final").rdd.map(row => row.getAs[Double]("all_score_final")).collect().toList
    //    val allScoreFinalInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
    val allScoreFinalMap = SomeUtils.getRightIntervalBySetting(allScoreFinalList, allInterval)

    allScoreFinalMap.foreach(a => {
      val all_score_final = JavaSomeUtils.get2Double(a._1.toDouble)
      val num = a._2
      val other = "Vj ≠ 6"
      val sql = s"insert into canNum_v6_all_score_final(hotel_id,hotel_name,section,all_score_final,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$all_score_final,'$other',$num,getdate());"
      st.executeUpdate(sql)
    })

    // 7.todo G 设计感：                number
    val numberList: List[Double] = needDf.select("number").rdd.map(row => row.getAs[Double]("number")).collect().toList
    //    val numberInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
    val numberMap = SomeUtils.getRightIntervalBySetting(numberList, allInterval)

    numberMap.foreach(a => {
      val number = JavaSomeUtils.get2Double(a._1.toDouble)
      val num = a._2
      val other = "Vj ≠ 7"
      val sql = s"insert into canNum_v7_number(hotel_id,hotel_name,section,number,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$number,'$other',$num,getdate());"
      st.executeUpdate(sql)
    })

    // 8.todo H 文字评价率：                text_rating_rate
    val textRatingRateList: List[Double] = needDf.select("text_rating_rate").rdd.map(row => row.getAs[Double]("text_rating_rate")).collect().toList
    //    val textRatingRateInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
    val textRatingRateMap = SomeUtils.getRightIntervalBySetting(textRatingRateList, allInterval)

    textRatingRateMap.foreach(a => {
      val text_rating_rate = JavaSomeUtils.get2Double(a._1.toDouble)
      val num = a._2
      val other = "Vj ≠ 8"
      val sql = s"insert into canNum_v8_text_rating_rate(hotel_id,hotel_name,section,text_rating_rate,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$text_rating_rate,'$other',$num,getdate());"
      st.executeUpdate(sql)
    })

    // 9.todo I 服务费：                display_amount
    val displayAmountList: List[Double] = needDf.select("display_amount").rdd.map(row => row.getAs[Double]("display_amount")).collect().toList
    val displayAmountInterval = SomeUtils.getSplitIntervalSetting(0, 5000, 100, "[]")
    val displayAmountMap = SomeUtils.getRightIntervalBySetting(displayAmountList, displayAmountInterval)

    displayAmountMap.foreach(a => {
      val display_amount = JavaSomeUtils.get2Double(a._1.toDouble)
      val num = a._2
      val other = "Vj ≠ 9"
      val sql = s"insert into canNum_v9_display_amount(hotel_id,hotel_name,section,display_amount,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$display_amount,'$other',$num,getdate());"
      st.executeUpdate(sql)
    })

    // 10.todo J 到店率：                to_store_rate
    val toStoreRateList: List[Double] = needDf.select("to_store_rate").rdd.map(row => row.getAs[Double]("to_store_rate")).collect().toList
    //    val toStoreRateInterval = SomeUtils.getSplitIntervalSetting(-1, 1, 200, "[]")
    val toStoreRateMap = SomeUtils.getRightIntervalBySetting(toStoreRateList, allInterval)

    toStoreRateMap.foreach(a => {
      val to_store_rate = JavaSomeUtils.get2Double(a._1.toDouble)
      val num = a._2
      val other = "Vj ≠ 10"
      val sql = s"insert into canNum_v10_to_store_rate(hotel_id,hotel_name,section,to_store_rate,other_condition,can_num,itime) VALUES ('$hotel_id','$hotel_name',$section,$to_store_rate,'$other',$num,getdate());"
      st.executeUpdate(sql)
    })

    needDf.unpersist()
  }
}
