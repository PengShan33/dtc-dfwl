package com.dtc.analytic.online.main;

import com.dtc.analytic.online.common.constant.PropertiesConstants;
import com.dtc.analytic.online.common.utils.ExecutionEnvUtil;
import com.dtc.analytic.online.common.utils.KafkaUtil;
import com.dtc.analytic.online.common.utils.IndexUtils;
import com.dtc.analytic.online.sink.IndexResultSinkToOpentsdb;
import com.dtc.analytic.online.sink.OnLineResultSinkToOpentsdb;
import com.dtc.analytic.online.soucre.ReadDeviceInfo;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        env.getConfig().setGlobalJobParameters(parameterTool);

        String opentsdb_url = parameterTool.get(PropertiesConstants.OPENTSDB_URL).trim();

        // 读取mysql数据
        DataStreamSource<String> deviceInfoMysql = env.addSource(new ReadDeviceInfo());

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

        // left join
        DataStream<Tuple2<String, String>> joinResult = MainUtils.leftJoin(deviceInfoMysql, all_DataStream);

        // leftJoin结果写入opentsdb
        joinResult.addSink(new OnLineResultSinkToOpentsdb(opentsdb_url));

        // 在线时长/离线时长/在线率指标计算
        DataStream<Tuple4<String, Double, Double, Double>> indexResult = IndexUtils.indexCalculate(deviceInfoMysql, opentsdb_url);

        // 指标计算结果写入opentsdb
        indexResult.addSink(new IndexResultSinkToOpentsdb(opentsdb_url));

        env.execute("online-test");
    }

}
