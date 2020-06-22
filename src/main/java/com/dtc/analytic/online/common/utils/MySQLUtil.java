package com.dtc.analytic.online.common.utils;

import java.sql.Connection;
import java.sql.DriverManager;

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
}
