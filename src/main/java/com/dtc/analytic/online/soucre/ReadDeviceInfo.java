package com.dtc.analytic.online.soucre;

import com.dtc.analytic.online.common.constant.PropertiesConstants;
import com.dtc.analytic.online.common.utils.MySQLUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class ReadDeviceInfo extends RichSourceFunction<String> {
    private Connection connection = null;
    private PreparedStatement ps = null;
    private volatile boolean isRunning = true;
    private ParameterTool parameterTool;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        parameterTool = (ParameterTool) (getRuntimeContext().getExecutionConfig().getGlobalJobParameters());

        String driver = "com.mysql.jdbc.Driver";
        String host = parameterTool.get(PropertiesConstants.MYSQL_HOST).trim();
        String port = parameterTool.get(PropertiesConstants.MYSQL_PORT).trim();
        String database = parameterTool.get(PropertiesConstants.MYSQL_DATABASE).trim();
        String username = parameterTool.get(PropertiesConstants.MYSQL_USERNAME).trim();
        String password = parameterTool.get(PropertiesConstants.MYSQL_PASSWORD).trim();
        String url = "jdbc:mysql://" + host + ":" + port + "/" + database + "?useUnicode=true&characterEncoding=UTF-8";
        connection = MySQLUtil.getConnection(driver, url, username, password);

        if (connection != null) {
            String sql = "select device_id from asset_headend_equipment;";
            ps = connection.prepareStatement(sql);
        }
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        while (isRunning) {
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                String device_id = resultSet.getString("device_id");
                sourceContext.collect(device_id);
            }
            Thread.sleep(1000 * 60 * 5);
        }
    }

    @Override
    public void cancel() {
        try {
            super.close();
            if (connection != null) {
                connection.close();
            }
            if (ps != null) {
                ps.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        isRunning = false;
    }
}
