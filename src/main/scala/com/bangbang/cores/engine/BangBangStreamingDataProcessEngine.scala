package com.bangbang.cores.engine

import java.util

import com.bangbang.cores.properties.JavaBangBangProperties
import com.izhaowo.cores.properties.JavaXianChiFanProperties
import com.izhaowo.cores.utils.JavaHBaseUtils
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.util.Bytes

/**
  * 爱找我————棒棒stream数据处理逻辑类
  *
  * @version 1.3
  * @since 2019/06/18 by Hiwes
  */
object BangBangStreamingDataProcessEngine {
  /**
    * 针对不同的数据进行处理
    *
    * @param line
    */
  def DBContentsDispose(line: String): Unit = {
    if (line != null) {
      if (line.equals("ceshi")) {
        println(line)
      } else {
        val arr: Array[String] = line.replaceAll("\"", "").split(",")
        if (arr.length <= 10 && arr(6).equals("0") && arr(2).length > 0) {
          if (tbPlannerRecomRecordDataIsNotExist(arr)) { // 数据不存在则处理
            saveTbPlannerRecomRecordData2HBase(arr)
            println("tb_planner_recom_record数据传输:")
            println(line)
            //            // TODO 删除，推荐结果从策划师推荐接口直接存储。
            //            if (BangBangStreamingDataProcessEngine.getWeddingHotel(arr(2)) != null &&
            //              BangBangStreamingDataProcessEngine.getWeddingInfo(arr(2)).length == 3 &&
            //              isNotTestData(arr(2)) == true) {
            //              saveCanNum4statistical2HBase(arr)
            //            }
          }
        } else if (arr.length > 20) { // 新监控一张表：tb_user_wedding_team_member
          if (arr(7).equals("策划师") && !arr(9).equals("0")) { // 小叶提供的判定条件。必定为策划师，且commit_status不为0
            if (tbUserWeddingTeamMemberDataIsNotExist(arr)) { // 数据不存在则处理
              saveTbUserWeddingTeamMemberData2HBase(arr)
              println("tb_user_wedding_team_member数据传输:")
              println(line)
              // TODO 针对tb_user_wedding_team_member表数据进行统计。影响已预订
              if (BangBangStreamingDataProcessEngine.getWeddingHotel(arr(3)) != null &&
                BangBangStreamingDataProcessEngine.getWeddingInfo(arr(3)).length == 3 &&
                isNotTestData(arr(3)) == true) {
                saveDoneNum4statistical2HBase(arr) // 调已预订统计方法
              }
            }
          }
        }
      }
    }
  }

  /**
    * tb_planner_recom_record数据写入
    * 包含mapid查询
    *
    * @param arr
    */
  def saveTbPlannerRecomRecordData2HBase(arr: Array[String]): Unit = {
    val put = new Put(Bytes.toBytes(arr(0)))
    for (i <- 1 to arr.length - 1) {
      if (arr(i) == null) {
        arr(i) = ""
      }
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(JavaBangBangProperties.columnsOfTBPLANNERRECOMRECORD(i - 1)), Bytes.toBytes(arr(i)));
    }
    val wedding_id = arr(2)
    var hotel_id = ""
    if (wedding_id != null && !wedding_id.equals("null")) {
      hotel_id = getWeddingHotel(wedding_id)
    }
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("hotel_id"), Bytes.toBytes(hotel_id))
    val list = new util.ArrayList[Put]()
    list.add(put)
    JavaHBaseUtils.putRows(JavaBangBangProperties.TBPLANNERRECOMRECORD, list)
  }

  /**
    * tb_user_wedding_team_member数据写入____只存储部分数据
    * 包含mapid查询
    *
    * @param arr
    */
  def saveTbUserWeddingTeamMemberData2HBase(arr: Array[String]): Unit = {
    val put = new Put(Bytes.toBytes(arr(0)))
    if (arr(7) == null) {
      arr(7) = ""
    }
    if (arr(8) == null) {
      arr(8) = ""
    }
    if (arr(10) == null) {
      arr(10) = ""
    }
    if (arr(11) == null) {
      arr(11) = ""
    }
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(JavaBangBangProperties.columnsOfTBUSERWEDDINGTEAMMEMBER(0)), Bytes.toBytes(arr(3))); // "wedding_id"
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(JavaBangBangProperties.columnsOfTBUSERWEDDINGTEAMMEMBER(2)), Bytes.toBytes(arr(7))); // "vocation"
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(JavaBangBangProperties.columnsOfTBUSERWEDDINGTEAMMEMBER(3)), Bytes.toBytes(arr(8))); // "sort"
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(JavaBangBangProperties.columnsOfTBUSERWEDDINGTEAMMEMBER(4)), Bytes.toBytes(arr(10))); // "ctime"
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(JavaBangBangProperties.columnsOfTBUSERWEDDINGTEAMMEMBER(5)), Bytes.toBytes(arr(11))); // "utime"
    val wedding_id = arr(3)
    var hotel_id = ""
    if (wedding_id != null && !wedding_id.equals("null")) {
      hotel_id = getWeddingHotel(wedding_id)
    }
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(JavaBangBangProperties.columnsOfTBUSERWEDDINGTEAMMEMBER(1)), Bytes.toBytes(hotel_id))
    val list = new util.ArrayList[Put]()
    list.add(put)
    JavaHBaseUtils.putRows(JavaBangBangProperties.TBUSERWEDDINGTEAMMEMBER, list)
  }

  /**
    * 存储已预订策划师统计数据到HBase————tb_user_wedding_team_member
    *
    * @param arr
    */
  def saveDoneNum4statistical2HBase(arr: Array[String]): Unit = {
    try {
      val hotelArr = getHotelSSQDataByWedId(arr(3)) // hotel_id, province, city, zone, name
      val weddingArr = getWeddingInfo(arr(3)) // wedding_date, budget_min, budget_max

      val start = hotelArr(0) + "=" + (weddingArr(0).replaceAll("-", "")) + "=0"
      val stop = hotelArr(0) + "=" + (weddingArr(0).replaceAll("-", "")) + "=999"

      val result = JavaHBaseUtils.getScanner(JavaBangBangProperties.TBPLANNERDEMAND, start, stop)
      var len = 1
      var end_done_num = "1"
      var res = result.next()
      while (res != null) {
        val done_num = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("done_num")))
        end_done_num = (done_num.toInt + 1).toString
        res = result.next()
        len += 1
      }

      val rowkey = hotelArr(0) + "=" + (weddingArr(0).replaceAll("-", "")) + "=" + len.toString
      val put = new Put(Bytes.toBytes(rowkey))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(JavaBangBangProperties.columnsOfTBPLANNERDEMAND(0)), Bytes.toBytes(arr(3)))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(JavaBangBangProperties.columnsOfTBPLANNERDEMAND(1)), Bytes.toBytes(arr(2)))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(JavaBangBangProperties.columnsOfTBPLANNERDEMAND(2)), Bytes.toBytes(hotelArr(1)))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(JavaBangBangProperties.columnsOfTBPLANNERDEMAND(3)), Bytes.toBytes(hotelArr(2)))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(JavaBangBangProperties.columnsOfTBPLANNERDEMAND(4)), Bytes.toBytes(hotelArr(3)))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(JavaBangBangProperties.columnsOfTBPLANNERDEMAND(5)), Bytes.toBytes(hotelArr(4)))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(JavaBangBangProperties.columnsOfTBPLANNERDEMAND(6)), Bytes.toBytes(weddingArr(0)))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(JavaBangBangProperties.columnsOfTBPLANNERDEMAND(7)), Bytes.toBytes(weddingArr(1)))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(JavaBangBangProperties.columnsOfTBPLANNERDEMAND(8)), Bytes.toBytes(weddingArr(2)))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(JavaBangBangProperties.columnsOfTBPLANNERDEMAND(9)), Bytes.toBytes(end_done_num))
      val list = new util.ArrayList[Put]()
      list.add(put)
      JavaHBaseUtils.putRows(JavaBangBangProperties.TBPLANNERDEMAND, list)
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  /**
    * 存储可预订策划师统计数据到HBase————tb_planner_recom_record
    *
    * @param arr
    */
  def saveCanNum4statistical2HBase(arr: Array[String]): Unit = {
    try {
      val hotelArr = getHotelSSQDataByWedId(arr(2)) // hotel_id, province, city, zone, name
      val weddingArr = getWeddingInfo(arr(2)) // wedding_date, budget_min, budget_max

      val start = hotelArr(0) + "=" + (weddingArr(0).replaceAll("-", "")) + "=0"
      val stop = hotelArr(0) + "=" + (weddingArr(0).replaceAll("-", "")) + "=999"

      val result = JavaHBaseUtils.getScanner(JavaBangBangProperties.TBPLANNERSUPPLY, start, stop)
      var len = 1
      var end_done_num: String = "1"
      var res = result.next()
      while (res != null) {
        val done_num = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("can_num")))
        end_done_num = (done_num.toInt + 1).toString
        res = result.next()
        len += 1
      }

      val rowkey = hotelArr(0) + "=" + (weddingArr(0).replaceAll("-", "")) + "=" + len.toString
      val put = new Put(Bytes.toBytes(rowkey))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(JavaBangBangProperties.columnsOfTBPLANNERSUPPLY(0)), Bytes.toBytes(arr(2)))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(JavaBangBangProperties.columnsOfTBPLANNERSUPPLY(1)), Bytes.toBytes(arr(1)))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(JavaBangBangProperties.columnsOfTBPLANNERSUPPLY(2)), Bytes.toBytes(hotelArr(1)))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(JavaBangBangProperties.columnsOfTBPLANNERSUPPLY(3)), Bytes.toBytes(hotelArr(2)))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(JavaBangBangProperties.columnsOfTBPLANNERSUPPLY(4)), Bytes.toBytes(hotelArr(3)))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(JavaBangBangProperties.columnsOfTBPLANNERSUPPLY(5)), Bytes.toBytes(hotelArr(4)))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(JavaBangBangProperties.columnsOfTBPLANNERSUPPLY(6)), Bytes.toBytes(weddingArr(0)))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(JavaBangBangProperties.columnsOfTBPLANNERSUPPLY(7)), Bytes.toBytes(weddingArr(1)))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(JavaBangBangProperties.columnsOfTBPLANNERSUPPLY(8)), Bytes.toBytes(weddingArr(2)))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(JavaBangBangProperties.columnsOfTBPLANNERSUPPLY(9)), Bytes.toBytes(end_done_num))
      val list = new util.ArrayList[Put]()
      list.add(put)
      JavaHBaseUtils.putRows(JavaBangBangProperties.TBPLANNERSUPPLY, list)
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  /**
    * 根据婚礼ID,获取酒店ID
    *
    * @param wedding_id
    * @return
    */
  def getWeddingHotel(wedding_id: String): String = JavaHBaseUtils.getValue(JavaXianChiFanProperties.TBUSERWEDDING, wedding_id, "info", "hotel_id")

  /**
    * 根据酒店ID,获取酒店省市区县和名称信息
    *
    * @param hotel_id
    */
  def getHotelSSQData(hotel_id: String) = {
    val res = JavaHBaseUtils.getRow(JavaXianChiFanProperties.TBHOTEL, hotel_id)
    val province = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("province")))
    val city = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("city")))
    val zone = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("zone")))
    val name = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")))
    Array(province, city, zone, name)
  }

  /**
    * 根据婚礼ID,直接获取酒店省市区县和名称信息
    *
    * @param wedding_id
    */
  def getHotelSSQDataByWedId(wedding_id: String) = {
    val hotel_id = JavaHBaseUtils.getValue(JavaXianChiFanProperties.TBUSERWEDDING, wedding_id, "info", "hotel_id")
    val res = JavaHBaseUtils.getRow(JavaXianChiFanProperties.TBHOTEL, hotel_id)
    val province = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("province")))
    val city = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("city")))
    val zone = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("zone")))
    val name = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")))
    Array(hotel_id, province, city, zone, name)
  }

  /**
    * 根据婚礼ID,获取婚礼婚期，最小预算，最大预算
    *
    * @param wedding_id
    * @return
    */
  def getWeddingInfo(wedding_id: String): Array[String] = {
    val res = JavaHBaseUtils.getRow(JavaXianChiFanProperties.TBUSERWEDDING, wedding_id)
    val wedding_date = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("wedding_date")))
    var budget_min = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("budget_min")))
    var budget_max = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("budget_max")))
    if (budget_min != null) {
      budget_min = (budget_min.toInt / 100).toString
    } else {
      budget_min = "0"
    }
    if (budget_max != null) {
      budget_max = (budget_max.toInt / 100).toString
    } else {
      budget_max = "0"
    }

    if (budget_max.toInt < budget_min.toInt) { // 数据异常，最大值 < 最小值
      Array(wedding_date, budget_max, budget_min)
    } else {
      Array(wedding_date, budget_min, budget_max)
    }
  }

  /**
    * 根据wedding_id，判断是否不为测试数据
    *
    * @param wedding_id
    */
  def isNotTestData(wedding_id: String): Boolean = {
    val brokerArr = Array("70a0d230-2b6a-4f32-8afc-a4379bbfca41",
      "864a6295-828b-4e22-9989-bfc2962efc4d",
      "aada4b53-d257-453f-848a-27869bad753a",
      "b411ec25-c2d2-11e7-864b-7cd30ab79bd4",
      "e7ed2c90-dc48-4595-a1bf-3725be1b1a68",
      //      "55d760af-ccbb-4c27-94b7-d555f4542361",     // 07.11后加
      "bc4a7dc9-a1c6-460c-b370-ec3ab7d871a9") // 测试数据
    val statusArr = Array("0", "4", "5")
    val ceshi = "测试"
    val result: Result = JavaHBaseUtils.getRow(JavaXianChiFanProperties.TBUSERWEDDING, wedding_id)
    val status = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("status")))
    val broker_id = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("broker_id")))
    val hotel = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("hotel")))
    val contacts_name = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("contacts_name")))
    if (brokerArr.contains(broker_id) || statusArr.contains(status) || hotel.contains(ceshi) || contacts_name.contains(ceshi)) {
      return false
    } else {
      return true
    }
  }

  /**
    * 查询已预订婚礼数据是否存在
    *
    * @param arr
    * @return
    */
  def tbUserWeddingTeamMemberDataIsNotExist(arr: Array[String]): Boolean = {
    val utime = JavaHBaseUtils.getValue("tb_user_wedding_team_member", arr(0), "info", "utime")
    if (utime == null) return true
    false
  }

  def tbPlannerRecomRecordDataIsNotExist(arr: Array[String]): Boolean = {
    val utime = JavaHBaseUtils.getValue("tb_planner_recom_record", arr(0), "info", "utime")
    if (utime == null) return true
    false
  }


  def main(args: Array[String]): Unit = {
    val te = "测试数据"
    val t = "测试"
    println(te.contains(t))
  }
}
