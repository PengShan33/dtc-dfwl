package com.dtc.analytic.online.common.constant;

public class PropertiesConstants {

    public static final String PROPERTIES_FILE_NAME = "/online-test.properties";

    /**
     * key
     */
    // kafka
    public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    public static final String ZOOKEEPER_CONNECT = "zookeeper.connect";
    public static final String TOPIC = "topic";
    public static final String GROUP_ID = "group.id";

    public static final String KAFKA_BROKERS = "dtc.bootstrap.servers";
    public static final String KAFKA_ZOOKEEPER_CONNECT="dtc.zookeeper.connect";
    public static final String KAFKA_GROUP_ID="dtc.group.id";
    public static final String KAFKA_GROUP_ID1="dtc.group.id1";
    public static final String KAFKA_GROUP_ID2="dtc.group.id2";
    public static final String KAFKA_GROUP_ID3="dtc.group.id3";
    public static final String KAFKA_GROUP_ID4="dtc.group.id4";
    public static final String KAFKA_TOPIC="dtc.topic";
    public static final String KAFKA_TOPIC1="dtc.topic1";
    public static final String KAFKA_TOPIC2="dtc.topic2";
    public static final String KAFKA_TOPIC3="dtc.topic3";
    public static final String KAFKA_TOPIC4="dtc.topic4";
    public static final String CONSUMER_FROM_TIME = "consumer.from.time";

    public static final String KEY_DESERIALIZER = "key.deserializer";
    public static final String VALUE_DESERIALIZER = "value.deserializer";
    public static final String AUTO_OFFSET_RESET = "auto.offset.reset";

    // flink
    public static final String STREAM_PARALLELISM = "stream.parallelism";
    public static final String STREAM_CHECKPOINT_INTERVAL = "stream.checkpoint.interval";
    public static final String STREAM_CHECKPOINT_ENABLE = "stream.checkpoint.enable";
    public static final String STREAM_DEFAULT_PARALLELISM = "stream.default.parallelism";

    // opentsdb
    public static final String OPENTSDB_URL="dtc.opentsdb.url";


    public static final String METRICS_TOPIC = "metrics.topic";


    public static final String SQL = "mysql.sql";



    /**
     * default value
     */
    public static final String DEFAULT_KAFKA_BROKERS = "localhost:9092";
    public static final String DEFAULT_KAFKA_ZOOKEEPER_CONNECT = "localhost:2181";
    public static final String DEFAULT_KAFKA_GROUP_ID = "test";
    public static final String DEFAULT_TOPIC="dtc";


    //mysql
    public static final String MYSQL_DATABASE = "mysql.database";
    public static final String MYSQL_HOST = "mysql.host";
    public static final String MYSQL_PASSWORD = "mysql.password";
    public static final String MYSQL_PORT = "mysql.port";
    public static final String MYSQL_USERNAME = "mysql.username";
    public static final String MYSQL_ALAEM_TABLE = "mysql.alarm_rule_table";
    public static final String MYSQL_WINDOWS_TABLE = "mysql.windows_disk_sql";

}
