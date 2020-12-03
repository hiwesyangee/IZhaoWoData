package com.yanjing.foruse

import com.alibaba.fastjson.JSON
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils


/**
  * 数据验证工具类
  *
  * by hiwes 09/29/2019
  */
object DataValidationUtils {

  /**
    * 主函数做测试用。
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    //    val arr: Array[Any] = getIntegralDataByInterface("2", "[1,2,3]", -1d, 1d)
    //    val arr: Array[Any] = DataValidationUtils.getIntegralDataByInterface("1", "[-16,-16,0]", -0.57d, 1d)
    //        val arr: Array[Any] = DataValidationUtils.getIntegralDataByInterface("2", "[-8.5,-5.5,2.5]", -0.818182d, 1d)
    //        println(arr(0).toString + "=" + arr(1).toString + "=" + arr(2).toString)

    val arr: Array[Any] = DataValidationUtils.getIntegralDataByInterface5001("5", "[0.1]", "1")
    println(arr(0).toString + "=" + arr(1).toString + "=" + arr(2).toString)
  }

  /**
    * 获取积分数组的结果
    *
    * @param arr
    * @return
    */
  def getIntegralResult(arr: Array[Any]): Double = {
    if (!arr.isEmpty && arr.length >= 3) arr(2).toString.toDouble
    else -10000000d
  }

  /**
    * 获取预测数组的结果
    *
    * @param arr
    * @return
    */
  def getPredictNumber(arr: Array[Any]): String = {
    if (!arr.isEmpty) arr(0).toString
    else "-1"
  }

  /**
    * 通过传参，获取积分结果。
    *
    * @param integralType 积分类型
    *                     1: 或者 polynomial  为多项式
    *                     2: 或者 exponential 为指数函数
    *                     3: 或者 reciprocal  为倒数函数
    *                     4: 或者 gaussian    为高斯概率密度函数
    * @param args         系数(用String表示的List，比如”[1,2,3]”)，仅限于多项式、指数函数，和倒数函数
    * @param lowerLimit   积分下限
    * @param upperLimit   积分上限
    */
  def getIntegralDataByInterface(integralType: String, args: String, lowerLimit: Double, upperLimit: Double): Array[Any] = {
    val url = s"http://139.129.230.5:5000/integrate?t=$integralType&args=$args&a=$lowerLimit&b=$upperLimit"
    println("url=" + url)
    //    val jsonResult: String = getResponse(url)
    val jsonResult: String = postResponse(url)
    val parse_str: Map[String, Any] = str_json(jsonResult)
    val message = parse_str("message")
    val t_type = parse_str("type")
    val integration = parse_str("integration")
    Array(message, t_type, integration)
  }

  def getIntegralDataByInterface(integralType: String, args: String, lowerLimit: Double, upperLimit: Double, url0: String): Array[Any] = {
    val paramArr = url0.split(",")
    val mu = paramArr(0)
    val sigma = paramArr(1)
    val url = s"http://139.129.230.5:5000/integrate?t=$integralType&mu=$mu&sigma=$sigma&a=$lowerLimit&b=$upperLimit"
    //    println("url=" + url)
    val jsonResult: String = postResponse(url)
    val parse_str: Map[String, Any] = str_json(jsonResult)
    val message = parse_str("message")
    val t_type = parse_str("type")
    val integration = parse_str("integration")
    Array(message, t_type, integration)
  }

  /**
    * 5001版本接口，对应的是
    *
    * @param integralType V类型
    * @param a            下界
    * @param hotel_id     酒店ID和section【暂定索菲斯锦苑宾馆(武都路4号院南)-3 == 1】
    * @return
    */
  def getIntegralDataByInterface5001(integralType: String, a: String, hotel_id: String): Array[Any] = {
    val url = s"http://139.129.230.5:5001/integrate/market?a=$a&id=$hotel_id&v=$integralType"
    println("url=" + url)
    val jsonResult: String = postResponse(url)
    val parse_str: Map[String, Any] = str_json5001(jsonResult)
    val message = parse_str("message")
    val t_type = parse_str("type")
    val integration = parse_str("integral")
    Array(message, t_type, integration)
  }

  /**
    * 通过python接口，获取预测策划师数量。
    *
    * @param hotel_id 酒店id
    * @param time     时间
    * @param version  版本
    */
  def getPredictNumberFromPythonInterface(hotel_id: String, time: String, version: String): String = {
    val F_integral: Array[Any] = DataValidationUtils.getPredictNumberData(hotel_id, time, version) // 数值作为下限，进行计算积分
    val end: String = DataValidationUtils.getPredictNumber(F_integral)
    end
  }

  /**
    * 获取python接口的预测策划师数量。
    *
    * @param hotel_id
    * @param time
    * @param version
    */
  def getPredictNumberData(hotel_id: String, time: String, version: String): Array[Any] = {
    val url = s"http://139.129.230.5:5001/integrate/market_id?id=$hotel_id"
    println("url=" + url)
    val jsonResult: String = getResponse(url)
    //    val jsonResult: String = (url)
    val parse_str: Map[String, Any] = str_jsonPredict(jsonResult)
    val F_integral = parse_str("F_integral")
    Array(F_integral)
  }

  // 通过匹配，获取Json
  def regJson(json: Option[Any]) = json match {
    //转换类型
    case Some(map: collection.immutable.Map[String, Any]) => map
  }

  // String转Json
  def str_json(string_json: String): collection.immutable.Map[String, Any] = {
    var first: collection.immutable.Map[String, Any] = collection.immutable.Map()
    val jsonS = scala.util.parsing.json.JSON.parseFull(string_json)
    //不确定数据的类型时，此处加异常判断
    if (jsonS.isInstanceOf[Option[Any]]) {
      first = regJson(jsonS)
    }
    first
  }

  // String转Json
  def str_json5001(string_json: String): collection.immutable.Map[String, Any] = {
    var first: collection.immutable.Map[String, Any] = collection.immutable.Map()
    val jsonS = scala.util.parsing.json.JSON.parseFull(string_json)
    //不确定数据的类型时，此处加异常判断
    if (jsonS.isInstanceOf[Option[Any]]) {
      first = regJson5001(jsonS)
    }
    first
  }

  // 通过匹配，获取Json
  def regJson5001(json: Option[Any]) = json match {
    //转换类型
    case Some(List(map: collection.immutable.Map[String, Any])) => map
  }

  // String转Json
  def str_jsonPredict(string_json: String): collection.immutable.Map[String, Any] = {
    var first: collection.immutable.Map[String, Any] = collection.immutable.Map()
    val jsonS = scala.util.parsing.json.JSON.parseFull(string_json)
    //不确定数据的类型时，此处加异常判断
    if (jsonS.isInstanceOf[Option[Any]]) {
      first = regJsonPredict(jsonS)
    }
    first
  }

  // 通过匹配，获取Json
  def regJsonPredict(json: Option[Any]) = json match {
    //转换类型
    case Some(map: collection.immutable.Map[String, Any]) => map
  }

  /**
    *
    *
    * @param url
    * @param header
    * @return
    */
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

  /**
    * post方式,连接Http接口。
    *
    * @param url
    * @param params
    * @param header
    * @return
    */
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
}
