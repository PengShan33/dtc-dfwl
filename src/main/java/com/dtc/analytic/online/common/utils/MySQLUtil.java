package com.dtc.analytic.online.common.utils;

import com.dtc.analytic.online.common.constant.PropertiesConstants;
import org.apache.flink.api.java.utils.ParameterTool;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class MySQLUtil {


    public static Connection getConnection(String driver,String url,String username,String password) {
        Connection connection =null;
        try {
            Class.forName(driver);
            connection = DriverManager.getConnection(url,username,password);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return connection;
    }

    public static Connection getConnection(ParameterTool parameterTool) {
        final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
        Connection connection = null;
        try {
            Class.forName(JDBC_DRIVER);
            // 获取数据库连接
            String userName = parameterTool.get(PropertiesConstants.MYSQL_USERNAME).trim();
            String passWord = parameterTool.get(PropertiesConstants.MYSQL_PASSWORD).trim();
            String host = parameterTool.get(PropertiesConstants.MYSQL_HOST).trim();
            String port = parameterTool.get(PropertiesConstants.MYSQL_PORT).trim();
            String database = parameterTool.get(PropertiesConstants.MYSQL_DATABASE).trim();
            String mysqlUrl = "jdbc:mysql://" + host + ":" + port + "/" + database + "?useUnicode=true&characterEncoding=UTF-8";
            connection = DriverManager.getConnection(mysqlUrl, userName, passWord);//写入mysql数据库
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return connection;
    }
}
