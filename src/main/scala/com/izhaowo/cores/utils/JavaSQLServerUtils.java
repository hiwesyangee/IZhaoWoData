package com.izhaowo.cores.utils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 爱找我实时流1.0.0版本MySQL工具类Java版本
 *
 * @version 1.0.0
 * @since 2019/05/20 by Hiwes
 */
public class JavaSQLServerUtils {

    /**
     * 创建MySQL数据库表
     *
     * @param createSQL 创建MySQL表的SQL语句
     * @return 是否创建成功
     */
    public static boolean createTable(String createSQL) {
        Connection conn = null;
        Statement st = null;
        try {
            conn = JavaSQLServerConn.getConnection();
            st = conn.createStatement();
            if (0 == st.executeUpdate(createSQL)) {
                return true;
            } else {
                return false;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            try {
                st.close();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 删除MySQL数据库表
     *
     * @param deleteSQL 删除MySQL表的SQL语句
     * @return 是否删除成功
     */
    public static boolean deleteTable(String deleteSQL) {
        Connection conn = null;
        Statement st = null;
        try {
            conn = JavaSQLServerConn.getConnection();
            st = conn.createStatement();
            if (0 == st.executeUpdate(deleteSQL)) {
                return true;
            } else {
                return false;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            try {
                st.close();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 单条查询MySQL数据库表
     *
     * @param querySQL 查询SQL语句
     * @return 返回查询结果，存储到List
     */
    public static List<String> querySingleData(String querySQL, List<String> columnList) {
        List<String> list = new ArrayList<>();

        Connection conn = null;
        PreparedStatement st = null;
        ResultSet rs = null;

        try {
            conn = JavaSQLServerConn.getConnection();
            st = conn.prepareStatement(querySQL);
            rs = st.executeQuery();
            while (rs.next()) {
                for (String column : columnList) {
                    list.add(rs.getString(column));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            JavaSQLServerConn.colseResource(conn, st, rs);
        }
        return list;
    }


    /**
     * 通过SQL语句，操作MySQL数据库，通用方法(包含更新和)
     *
     * @return
     */
    public static boolean executeUpdateSingleData(String normalSQL) {
        Connection conn = null;
        Statement st = null;
        try {
            conn = JavaSQLServerConn.getConnection();
            st = conn.createStatement();
            if (1 == st.executeUpdate(normalSQL)) {
                return true;
            } else {
                return false;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            JavaSQLServerConn.closeStatement(st);
            JavaSQLServerConn.closeConnection(conn);
        }
        return false;

    }

    public static void main(String[] args) {
        
    }

}
