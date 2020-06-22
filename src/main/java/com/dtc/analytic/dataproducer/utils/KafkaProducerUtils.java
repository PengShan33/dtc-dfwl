package com.dtc.analytic.dataproducer.utils;

import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

public class KafkaProducerUtils {

    public static KafkaProducer<String, byte[]> buildByteProducer() {
        Properties properties = buildKafkaProp();
        properties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(properties);
        return producer;
    }

    private static Properties buildKafkaProp() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.3.7.232:9092,10.3.7.233:9092,10.3.6.20:9092");
        props.put("acks", "all");
        props.put("retries", 5);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    public static KafkaProducer<String, String> buildStringProducer() {
        Properties properties = buildKafkaProp();
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        return producer;
    }
}
