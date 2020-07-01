package com.dtc.analytic.online.main;

import com.dtc.analytic.online.common.constant.PropertiesConstants;
import com.dtc.analytic.online.common.modle.AlterStruct;
import com.dtc.analytic.online.common.modle.TimesConstants;
import com.dtc.analytic.online.common.utils.*;
import com.dtc.analytic.online.process.mapFunction.AlarmInfoMapFunction;
import com.dtc.analytic.online.process.mapFunction.TestFlatMapFunction;
import com.dtc.analytic.online.process.processFunction.AlarmInfoProcessFunction;
import com.dtc.analytic.online.sink.AlarmDataSinkToMysql;
import com.dtc.analytic.online.sink.IndexResultSinkToOpentsdb;
import com.dtc.analytic.online.sink.OnLineResultSinkToOpentsdb;
import com.dtc.analytic.online.sink.offLineAlarmDataSinkToMysql;
import com.dtc.analytic.online.soucre.ReadAlarmInfo;
import com.dtc.analytic.online.soucre.ReadDeviceInfo;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        MapStateDescriptor<String, String> ALARM_RULES = new MapStateDescriptor<>(
                "alarm_rules",
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO);

        ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        env.getConfig().setGlobalJobParameters(parameterTool);

        String opentsdb_url = parameterTool.get(PropertiesConstants.OPENTSDB_URL).trim();
        int windowSizeMillis = parameterTool.getInt("dtc.windowSizeMillis", 2000);
        TimesConstants build = MainUtils.getSize(parameterTool);

        // 读取mysql中deviceId信息
        DataStreamSource<String> deviceInfoMysql = env.addSource(new ReadDeviceInfo());
        // 读取mysql中的告警配置数据
        DataStreamSource<Tuple9<String, String, String, String, Double, String, String, String, String>> alarmInfoMysql = env.addSource(new ReadAlarmInfo()).setParallelism(1);

        // 处理告警配置数据
        SingleOutputStreamOperator<Map<String, Tuple10<String, String, String, Double, Double, Double, Double, String, String,String>>> process = alarmInfoMysql.keyBy(0, 5).timeWindow(Time.milliseconds(windowSizeMillis)).process(new AlarmInfoProcessFunction());
        SingleOutputStreamOperator<Map<String, String>> alarmDataStream = process.map(new AlarmInfoMapFunction());
        // 广播告警配置数据
        BroadcastStream<Map<String, String>> broadcast = alarmDataStream.broadcast(ALARM_RULES);
        alarmDataStream.print("alarm");

        // 读取kafka数据
        String topic1 = parameterTool.get(PropertiesConstants.KAFKA_TOPIC1).trim();
        String topic2 = parameterTool.get(PropertiesConstants.KAFKA_TOPIC2).trim();
        String topic3 = parameterTool.get(PropertiesConstants.KAFKA_TOPIC3).trim();
        String topic4 = parameterTool.get(PropertiesConstants.KAFKA_TOPIC4).trim();

        DataStreamSource<byte[]> topic1_StreamSource = KafkaUtil.buildByteSource(env, topic1);
        DataStreamSource<byte[]> topic2_StreamSource = KafkaUtil.buildByteSource(env, topic2);
        DataStreamSource<byte[]> topic3_StreamSource = KafkaUtil.buildByteSource(env, topic3);
        DataStreamSource<String> topic4_StreamSource = KafkaUtil.buildStringSource(env, topic4);

        // 处理kafka数据
        DataStream<String> topic1_DataStream = MainUtils.byteDataProcess(topic1_StreamSource, topic1);
        DataStream<String> topic2_DataStream = MainUtils.byteDataProcess(topic2_StreamSource, topic2);
        DataStream<String> topic3_DataStream = MainUtils.byteDataProcess(topic3_StreamSource, topic3);
        DataStream<String> topic4_DataStream = MainUtils.stringDataProcess(topic4_StreamSource, topic4);

        // 合并流
        DataStream<String> all_DataStream = topic1_DataStream.union(topic2_DataStream, topic3_DataStream, topic4_DataStream);

        // left join在线状况判断
        DataStream<Tuple2<String, String>> joinResult = MainUtils.leftJoin(deviceInfoMysql, all_DataStream);

        // leftJoin结果写入opentsdb
        joinResult.addSink(new OnLineResultSinkToOpentsdb(opentsdb_url));

        // 事件告警数据处理
        List<DataStream<AlterStruct>> offLineAlarmData = AlarmUtils.getOffLineAlarm(joinResult, broadcast, build);
        offLineAlarmData.forEach(e -> e.addSink(new offLineAlarmDataSinkToMysql()));

        // 在线时长/累计离线时长/持续离线时长/在线率指标计算
        DataStream<Tuple5<String, Double, Double, Double, Double>> indexResult = IndexUtils.indexCalculate(deviceInfoMysql);

        // 指标计算结果写入opentsdb
        indexResult.addSink(new IndexResultSinkToOpentsdb(opentsdb_url));

        // 指标告警数据处理
        List<DataStream<AlterStruct>> alarmData = AlarmUtils.getAlarm(indexResult, broadcast, build);
        alarmData.forEach(e -> e.addSink(new AlarmDataSinkToMysql()));
        env.execute("dfwl-online-test");
    }
}
