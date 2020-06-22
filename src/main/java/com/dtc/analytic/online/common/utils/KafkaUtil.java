package com.dtc.analytic.online.common.utils;

import com.dtc.analytic.online.common.constant.PropertiesConstants;
import com.dtc.analytic.online.common.schemas.ByteArrayDeserializerSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.ArrayList;
import java.util.Properties;

public class KafkaUtil {

    private static Properties buildKafkaProps(ParameterTool parameterTool,String topic) {
        Properties props = parameterTool.getProperties();

        String bootstrap_Servers = parameterTool.get(PropertiesConstants.KAFKA_BROKERS).trim();
        props.put(PropertiesConstants.BOOTSTRAP_SERVERS,bootstrap_Servers);

        String zookeeper_Connect = parameterTool.get(PropertiesConstants.KAFKA_ZOOKEEPER_CONNECT).trim();
        props.put(PropertiesConstants.ZOOKEEPER_CONNECT, zookeeper_Connect);

        props.put(PropertiesConstants.GROUP_ID, parameterTool.get(PropertiesConstants.KAFKA_GROUP_ID, PropertiesConstants.DEFAULT_KAFKA_GROUP_ID));
        props.put(PropertiesConstants.TOPIC,topic);

        // 设置反序列化方式
        props.put(PropertiesConstants.KEY_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer");
        if (topic.equals(parameterTool.get(PropertiesConstants.KAFKA_TOPIC4))) {
            props.put(PropertiesConstants.VALUE_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer");
        } else {
            props.put(PropertiesConstants.VALUE_DESERIALIZER, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        }

        props.put(PropertiesConstants.AUTO_OFFSET_RESET, "latest");

        return props;
    }

    public static DataStreamSource<byte[]> buildByteSource(StreamExecutionEnvironment env,String topic) {
        ParameterTool parameterTool = (ParameterTool) env.getConfig().getGlobalJobParameters();
        Properties props = buildKafkaProps(parameterTool, topic);

        FlinkKafkaConsumer<byte[]> consumer = new FlinkKafkaConsumer<>(topic, new ByteArrayDeserializerSchema<byte[]>(), props);
        // flink从topic中最新的数据开始消费
        consumer.setStartFromLatest();
        return env.addSource(consumer).setParallelism(10);
    }

    public static DataStreamSource<String> buildStringSource(StreamExecutionEnvironment env,String topic) {
        ParameterTool parameterTool = (ParameterTool) env.getConfig().getGlobalJobParameters();
        Properties props = buildKafkaProps(parameterTool, topic);

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), props);
        // flink从topic中最新的数据开始消费
        consumer.setStartFromLatest();
        return env.addSource(consumer).setParallelism(10);
    }

    public static ArrayList<String> getTopic(StreamExecutionEnvironment env) {
        ParameterTool parameter = (ParameterTool) (env.getConfig().getGlobalJobParameters());
        String topic1 = parameter.get(PropertiesConstants.KAFKA_TOPIC1).trim();
        String topic2 = parameter.get(PropertiesConstants.KAFKA_TOPIC2).trim();
        String topic3 = parameter.get(PropertiesConstants.KAFKA_TOPIC3).trim();
        String topic4 = parameter.get(PropertiesConstants.KAFKA_TOPIC4).trim();

        ArrayList<String> topics = new ArrayList<>();
        topics.add(topic1);
        topics.add(topic2);
        topics.add(topic3);
        topics.add(topic4);

        return topics;
    }
}
