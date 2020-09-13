package com.eights.common.utils;

import com.eights.common.constant.PropertiesConstants;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class GuavaCacheUtils {

    private static final Logger LOG = LoggerFactory.getLogger(GuavaCacheUtils.class);

    public static Cache buildAfterAccessCache(ParameterTool tools, Integer times, TimeUnit timeUnit) {
        return CacheBuilder.newBuilder()
                .initialCapacity(tools.getInt(PropertiesConstants.GUAVA_CACHE_INIT_CAPACITY))
                .maximumSize(tools.getInt(PropertiesConstants.GUAVA_CACHE_MAX_SIZE))
                .expireAfterAccess(times, timeUnit)
                .build();
    }

}
