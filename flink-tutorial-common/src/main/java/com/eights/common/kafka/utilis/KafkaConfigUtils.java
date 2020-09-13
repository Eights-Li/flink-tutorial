package com.eights.common.kafka.utilis;

import com.eights.common.kafka.constant.PropertiesConstants;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.Properties;

public class KafkaConfigUtils {

    /**
     * build kafka config
     *
     * @param parameterTool kafka parameterTool
     * @return kafka properties
     */
    public static Properties buildKafkaProps(ParameterTool parameterTool) {
        Properties props = parameterTool.getProperties();
        props.put("bootstrap.servers", parameterTool.get(PropertiesConstants.KAFKA_BROKERS, PropertiesConstants.DEFAULT_KAFKA_BROKERS));
        props.put("group.id", parameterTool.get(PropertiesConstants.KAFKA_GROUP_ID, PropertiesConstants.DEFAULT_KAFKA_GROUP_ID));
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");
        return props;
    }

    public static Properties buildKafkaSinkProps(ParameterTool parameterTool) {
        Properties props = new Properties();
        props.put("bootstrap.servers", parameterTool.get(PropertiesConstants.KAFKA_BROKERS, PropertiesConstants.DEFAULT_KAFKA_BROKERS));
        props.put("acks", "all");
        props.put("retries", 3);
        props.put("transaction.timeout.ms", 5 * 60 * 1000);
        props.put("batch.size", 128 * 1024);
        props.put("max.request.size", 10 * 1024 * 1024);
        props.put("buffer.memory", 128 * 1024 * 1024);
        props.put("linger.ms", 10);
        return props;
    }

}
