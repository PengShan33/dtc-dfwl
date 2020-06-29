package com.dtc.analytic.online.soucre;

import com.dtc.analytic.online.common.constant.PropertiesConstants;
import com.dtc.analytic.online.common.utils.MySQLUtil;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class ReadAlarmInfo extends RichSourceFunction<Tuple9<String, String, String, String, Double, String, String, String, String>> {
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
            String sql = "select o.device_id,o.asset_id,o.strategy_id,o.trigger_kind,o.trigger_name,o.number,o.unit,o.`code`,o.is_enable,o.alarm_level,o.up_time,o.asset_code,o.`name`,p.id from (select f.device_id,b.asset_id,c.strategy_id,c.trigger_kind,c.trigger_name,c.number,c.unit,c.`code`,d.is_enable,d.alarm_level,d.up_time,e.`code` as asset_code,e.`name` from (select * from strategy_trigger a \n" +
                    "where (a.code!=\" \" and a.comparator='>') or a.trigger_kind = \"事件告警\") c left join strategy_asset_mapping b on c.strategy_id = b. strategy_id left join alarm_strategy d on c.strategy_id = d.id left join asset e on e.id = b.asset_id left join asset_headend_equipment f on f.asset_id = e.id where f.device_id is not null and c.code is not null) o left join asset_indice p on o.code = p.code";
            ps = connection.prepareStatement(sql);
        }
    }

    @Override
    public void run(SourceContext<Tuple9<String, String, String, String, Double, String, String, String, String>> sourceContext) throws Exception {
        while (isRunning) {
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                if ("1".equals(resultSet.getString("is_enable"))) {
                    String device_id = resultSet.getString("device_id");
                    String asset_id = resultSet.getString("asset_id") + "|" + resultSet.getString("id") + "|" + resultSet.getString("strategy_id");
                    String strategy_kind = resultSet.getString("trigger_kind");
                    String triger_name = resultSet.getString("trigger_name");
                    double number = resultSet.getDouble("number");
                    String code = resultSet.getString("code");
                    String alarm_level = resultSet.getString("alarm_level");
                    String asset_code = resultSet.getString("asset_code");
                    String name = resultSet.getString("name");
                    sourceContext.collect(Tuple9.of(asset_id, device_id, strategy_kind, triger_name, number, code, alarm_level, asset_code, name));
                }
            }
            Thread.sleep(1000 * 60);
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
