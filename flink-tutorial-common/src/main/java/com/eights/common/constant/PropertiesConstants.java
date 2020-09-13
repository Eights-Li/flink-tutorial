package com.eights.common.constant;

public final class PropertiesConstants {
    public static final String KAFKA_BROKERS = "kafka.brokers";
    public static final String DEFAULT_KAFKA_BROKERS = "localhost:9092";
    public static final String KAFKA_ZOOKEEPER_CONNECT = "kafka.zookeeper.connect";
    public static final String DEFAULT_KAFKA_ZOOKEEPER_CONNECT = "localhost:2181";
    public static final String KAFKA_GROUP_ID = "kafka.group.id";
    public static final String DEFAULT_KAFKA_GROUP_ID = "eights";
    public static final String METRICS_TOPIC = "metrics.topic";
    public static final String CONSUMER_FROM_TIME = "consumer.from.time";
    public static final String STREAM_PARALLELISM = "stream.parallelism";
    public static final String STREAM_SINK_PARALLELISM = "stream.sink.parallelism";
    public static final String STREAM_DEFAULT_PARALLELISM = "stream.default.parallelism";
    public static final String STREAM_CHECKPOINT_ENABLE = "stream.checkpoint.enable";
    public static final String STREAM_CHECKPOINT_DIR = "stream.checkpoint.dir";
    public static final String STREAM_CHECKPOINT_TYPE = "stream.checkpoint.type";
    public static final String STREAM_CHECKPOINT_INTERVAL = "stream.checkpoint.interval";
    public static final String PROPERTIES_FILE_NAME = "config_path";
    public static final String CHECKPOINT_MEMORY = "memory";
    public static final String CHECKPOINT_FS = "fs";
    public static final String CHECKPOINT_ROCKETSDB = "rocksdb";
    public static final String CHECKPOINT_TYPE="checkpoint.type";
    public static final String CHECKPOINT_HDFS_DIR = "checkpoint.hdfs.dir";
    public static final String CHENKPOINT_TIMEOUT = "checkpoint.timeout";

    /**
     * redis连接信息
     */
    public static final String REDIS_HOST="redis.host";
    public static final String REDIS_PORT="redis.port";
    public static final String REDIS_PWD="redis.password";

    /**
     * mysql 连接信息
     */
    public static final String MYSQL_JDBC_DRIVER="com.mysql.jdbc.Driver";
    public static final String MYSQL_URL="mysql.url";
    public static final String MYSQL_USER="mysql.user";
    public static final String MYSQL_PWD="mysql.password";

    /**
     * vertx jdbc
     */
    public static final String VERTX_URL="url";
    public static final String VERTX_DRIVER_CLASS="driver_class";
    public static final String VERTX_USER="user";
    public static final String VERTX_PWD="password";
    public static final String VERTX_MAX_POOL_SIZE="max_pool_size";
    public static final String VERTX_EVENT_LOOP_SIZE="vertx_event_loop_size";
    public static final String VERTX_WORKER_POOL_SIZE="vertx_worker_pool_size";

    /**
     * guava cache
     */
    public static final String GUAVA_CACHE_INIT_CAPACITY="guava_cache_init_capacity";
    public static final String GUAVA_CACHE_MAX_SIZE="guava_cache_max_size";

    public static final String ASYNC_TIMEOUT="async.io.timeout";
    public static final String ASYNC_CAPACITY="async.io.capacity";

    /**
     * kafka sync配置
     */
    public static final String SOURCE_KAFKA_BROKERS="source.kafka.brokers";
    public static final String SOURCE_KAFKA_ZK_CONNECT="source.kafka.zookeeper.connect";
    public static final String SOURCE_KAFKA_SYNC_TOPIC="source.kafka.sync.topic";
    public static final String SOURCE_KAFKA_TOPIC_PARTITIONS="source.kafka.topic.partitions";
    public static final String SOURCE_KAFKA_GROUP_ID="source.kafka.group.id";

    public static final String TARGET_KAFKA_BROKERS="target.kafka.brokers";
    public static final String TARGET_KAFKA_ZK_CONNECT="target.kafka.zookeeper.connect";
    public static final String TARGET_KAFKA_SYNC_TOPIC="target.kafka.sync.topic";
    public static final String TARGET_KAFKA_TOPIC_PARTITIONS="target.kafka.topic.partitions";




}
