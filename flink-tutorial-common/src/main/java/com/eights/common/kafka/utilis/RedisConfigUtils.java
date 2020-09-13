package com.eights.common.kafka.utilis;

import com.eights.common.kafka.constant.PropertiesConstants;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;

public class RedisConfigUtils {

    public static FlinkJedisPoolConfig buildRedisConf(ParameterTool params) {

        return new FlinkJedisPoolConfig.Builder()
                .setHost(params.get(PropertiesConstants.REDIS_HOST))
                .setPort(params.getInt(PropertiesConstants.REDIS_PORT))
                .setPassword(params.get(PropertiesConstants.REDIS_PWD))
                .build();
    }

}
