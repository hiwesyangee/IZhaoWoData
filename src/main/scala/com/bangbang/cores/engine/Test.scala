package com.bangbang.cores.engine

import java.sql.{ResultSet, Statement}

import com.izhaowo.cores.utils.JavaDateUtils

object Test {
  def main(args: Array[String]): Unit = {
    val conn = BangBangDataMySQLUtils.getConnection
    val st: Statement = conn.createStatement()

    val wedding_id = "1a859f11-97a0-4b04-ac6d-8e0dd06720a6"
    val wedding_date = "2019-06-30"

    val ymd: String = JavaDateUtils.getDateOnlyYMD
    val query = s"select * from tb_planner_supply where wedding_id = '$wedding_id' and wedding_date = '$wedding_date' and DateDiff(itime,'$ymd')=0;"
    val end: ResultSet = st.executeQuery(query)
    var bool = false
    if (end.next()) {
      bool = true
    }

    println(bool)


    BangBangDataMySQLUtils.closeStatement(st)
    BangBangDataMySQLUtils.closeConnection(conn)
  }
}
