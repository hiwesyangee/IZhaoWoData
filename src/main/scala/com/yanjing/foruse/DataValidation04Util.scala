package com.yanjing.foruse

import com.alibaba.fastjson.JSON
import com.izhaowo.cores.utils.JavaHBaseUtils
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

/**
  * 数据验证03辅助类。
  */
object DataValidation04Util {

  // case class : 合法策划师。
  case class LegalPlanner(worker_id: String, case_dot: Double, reorder_rate: Double, communication_level: Double,
                          design_sense: Double, case_rate: Double, all_score_final: Double, number: Double,
                          text_rating_rate: Double, display_amount: Double, to_store_rate: Double)

  // 1.过滤数据不符合区间的策划师数据。【判断值是否在区间】
  def legalPlannerFilter(lp: LegalPlanner): LegalPlanner = {
    //        var bool_res = true
    var bool_res = false
    if (lp.case_dot >= -0.8d && lp.case_dot <= 1) { // 1.number=16
      if (lp.reorder_rate >= -1 && lp.reorder_rate <= 0.6) { // 2.number=16
        if (lp.communication_level >= -0.8d && lp.communication_level <= 0.1d) { // 3.number=13
          if (lp.design_sense >= -0.6d && lp.design_sense <= 1) { // 4.number=2
            if (lp.case_rate >= -1 && lp.case_rate <= 0) { // 5.number=1
              if (lp.all_score_final >= 0.7d && lp.all_score_final <= 1) { // 6.number=1
                if (lp.number >= -0.8d && lp.number <= 0.8d) { // 7.number=1
                  if (lp.text_rating_rate >= -0.4d && lp.text_rating_rate <= 1) { // 8.number=1
                    if (lp.display_amount >= 900 && lp.display_amount <= 1600) { // 9.number=0
                      if (lp.to_store_rate >= -0.1d && lp.to_store_rate <= 1) { // 10.number=0[1]
                        bool_res = true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (bool_res == true) {
      lp
    } else {
      null
    }
  }

  // 2.过滤无档期的策划师数据。【判断是否有档期，过滤Set】
  def noSchedulePlannerFilter(plannerSet: Set[LegalPlanner], day: String): Set[LegalPlanner] = {
    var set = Set[LegalPlanner]()
    for (lp <- plannerSet) {
      if (hasSchedule4Planner(lp.worker_id, day: String)) {
        set = set.+(lp)
      }
    }
    set
  }

  // 2.1过滤策划师使用有档期。【具体判断是否有档期，针对单个策划师】
  def hasSchedule4Planner(planner_id: String, day: String): Boolean = {
    val planner_name = getPlannerNameById(planner_id).split(" ")(0)
    if (planner_name != null) {
      val url = s"http://master:7979/FuzzyWorkers?workerName=$planner_name&weddate=$day"
      val end: String = getResponse(url)
      val arr = end.split(",")
      if (arr.length >= 8) {
        if ((arr(7).split(":")(1)).toInt > 0) {
          return true
        }
      }
    }
    // todo 发送http消息，通过调用接口获取到最近的策划师档期。
    return false
  }

  // 3.查询策划师昵称。【根据ID查询策划师name】
  def getPlannerNameById(planner_id: String): String = {
    val name = JavaHBaseUtils.getValue("v2_rp_tb_worker", planner_id, "info", "name")
    if (name != null) {
      name
    } else {
      null
    }
  }

  // 4.get方式直接获取数据
  def getResponse(url: String, header: String = null): String = {
    val httpClient = HttpClients.createDefault() // 创建 client 实例
    val get = new HttpGet(url) // 创建 get 实例

    if (header != null) { // 设置 header
      val json = JSON.parseObject(header)
      json.keySet().toArray.map(_.toString).foreach(key => get.setHeader(key, json.getString(key)))
    }

    val response = httpClient.execute(get) // 发送请求
    EntityUtils.toString(response.getEntity) // 获取返回结果
  }

  // 4.post方式直接获取数据
  def postResponse(url: String, params: String = null, header: String = null): String = {
    val httpClient = HttpClients.createDefault() // 创建 client 实例
    val post = new HttpPost(url) // 创建 post 实例

    // 设置 header
    if (header != null) {
      val json = JSON.parseObject(header)
      json.keySet().toArray.map(_.toString).foreach(key => post.setHeader(key, json.getString(key)))
    }

    if (params != null) {
      post.setEntity(new StringEntity(params, "UTF-8"))
    }

    val response = httpClient.execute(post) // 创建 client 实例
    EntityUtils.toString(response.getEntity, "UTF-8") // 获取返回结果
  }

  // 5.统计Fv。 模型1
  def getTenDataByStatistical(lastPlannerSet: Set[LegalPlanner]): List[String] = {
    var fv1 = 0d
    var fv2 = 0d
    var fv3 = 0d
    var fv4 = 0d
    var fv5 = 0d
    var fv6 = 0d
    var fv7 = 0d
    var fv8 = 0d
    var fv9 = 0d
    var fv10 = 0d
    lastPlannerSet.foreach(lp => {
      val case_dot = lp.case_dot
      fv1 = fv1 + (1 + Math.sin(1.55 * (Math.pow(0.45 - case_dot, 3))))
      val reorder_rate = lp.reorder_rate
      fv2 = fv2 + Math.pow(0.25, 2.45 * (0.25 + reorder_rate))
      val communication_level = lp.communication_level
      fv3 = fv3 + ((1 / (0.02 * Math.sqrt(2 * Math.PI))) * (Math.exp(Math.pow(communication_level + 0.37, 2) / (-0.0008))) + 1)
      val design_sense = lp.design_sense
      fv4 = fv4 + ((1 / (0.4 * Math.sqrt(2 * Math.PI))) * (Math.exp(Math.pow(design_sense - 0.1, 2) / (-0.32))) + 1)
      val case_rate = lp.case_rate
      fv5 = fv5 + (Math.pow(0.15, 5.65 * (case_rate + 0.74)) + 1)
      val all_score_final = lp.all_score_final
      fv6 = fv6 + (Math.pow(5.45, 1.7 * (all_score_final + 0.14)))
      val number = lp.number
      fv7 = fv7 + (Math.pow(0.46, 1.9 * (number - 0.32)) + 1)
      val text_rating_rate = lp.text_rating_rate
      fv8 = fv8 + (4 - Math.pow(0.3, 5.3 * (text_rating_rate + 0.43)))
      val display_amount = lp.display_amount
      fv9 = fv9 + (9 - (0.00003 * (Math.pow(display_amount - 1350, 2))))
      val to_store_rate = lp.to_store_rate
      fv10 = fv10 + ((1 / (Math.sqrt(Math.PI * 2) * (0.013))) * (Math.exp((Math.pow(to_store_rate, 2)) / (-0.000338))))
    })
    List(fv1.formatted("%.4f"), fv2.formatted("%.4f"), fv3.formatted("%.4f"), fv4.formatted("%.4f"), fv5.formatted("%.4f"), fv6.formatted("%.4f"), fv7.formatted("%.4f"), fv8.formatted("%.4f"), fv9.formatted("%.4f"), fv10.formatted("%.4f"))
  }

  def getTenDataByStatistical2(lastPlannerSet: Set[LegalPlanner]): List[String] = {
    var fv1 = 0d
    var fv2 = 0d
    var fv3 = 0d
    var fv4 = 0d
    var fv5 = 0d
    var fv6 = 0d
    var fv7 = 0d
    var fv8 = 0d
    var fv9 = 0d
    var fv10 = 0d
    lastPlannerSet.foreach(lp => {
      val case_dot = lp.case_dot
      fv1 = fv1 + (1 + Math.sin(1.55 * (Math.pow(0.45 - case_dot, 3))))
      val reorder_rate = lp.reorder_rate
      fv2 = fv2 + Math.pow(0.25, 2.45 * (0.25 + reorder_rate))
      val communication_level = lp.communication_level
      fv3 = fv3 + ((1 / (0.02 * Math.sqrt(2 * Math.PI))) * (Math.exp(Math.pow(communication_level + 0.37, 2) / (-0.0008))) + 1)
      val design_sense = lp.design_sense
      fv4 = fv4 + ((1 / (0.4 * Math.sqrt(2 * Math.PI))) * (Math.exp(Math.pow(design_sense - 0.1, 2) / (-0.32))) + 1)
      val case_rate = lp.case_rate
      fv5 = fv5 + (Math.pow(0.15, 5.65 * (case_rate + 0.74)) + 1)
      val all_score_final = lp.all_score_final
      fv6 = fv6 + (Math.pow(5.45, 1.7 * (all_score_final + 0.14)))
      val number = lp.number
      fv7 = fv7 + (Math.pow(0.46, 1.9 * (number - 0.32)) + 1)
      val text_rating_rate = lp.text_rating_rate
      fv8 = fv8 + (4 - Math.pow(0.3, 5.3 * (text_rating_rate + 0.43)))
      val display_amount = lp.display_amount
      fv9 = fv9 + (9 - (0.00003 * (Math.pow(display_amount - 1350, 2))))
      val to_store_rate = lp.to_store_rate
      fv10 = fv10 + ((1 / (Math.sqrt(Math.PI * 2) * (0.013))) * (Math.exp((Math.pow(to_store_rate, 2)) / (-0.000338))))
    })
    List(fv1.toString, fv2.toString, fv3.toString, fv4.toString, fv5.toString, fv6.toString, fv7.toString, fv8.toString, fv9.toString, fv10.toString)
  }


  def main(args: Array[String]): Unit = {
    //    val url = s"http://master:7979/FuzzyWorkers?workerName=啦啦&weddate=20191201"
    //    val end: String = getResponse(url)
    //    println(end)
    //    val arr = end.split(",")
    //    if (arr.length >= 8) {
    //      if ((arr(7).split(":")(1)).toInt > 0) {
    //        println(true)
    //      } else {
    //        println(false)
    //      }
    //    } else {
    //      println(false)
    //    }
  }
}
