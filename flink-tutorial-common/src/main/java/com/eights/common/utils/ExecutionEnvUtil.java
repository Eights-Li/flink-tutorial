package com.eights.common.utils;

import com.eights.common.constant.PropertiesConstants;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ExecutionEnvUtil {

    private static final Logger LOG = LoggerFactory.getLogger(ExecutionEnvUtil.class);

    public static ParameterTool mergeParams(ParameterTool parameterTool) {
        try {
            parameterTool = ParameterTool.fromPropertiesFile(parameterTool.get(PropertiesConstants.PROPERTIES_FILE_NAME))
                    .mergeWith(parameterTool);
        } catch (IOException e) {
            LOG.error(String.format("Build Params fail:[%s]", e.getMessage()));
        }

        return parameterTool;
    }

    public static StreamExecutionEnvironment prepare(ParameterTool parameterTool) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parameterTool.getInt(PropertiesConstants.STREAM_PARALLELISM, 5));
        env.getConfig().setRestartStrategy(RestartStrategies.failureRateRestart(5, Time.minutes(5), Time.seconds(30)));
        if (parameterTool.getBoolean(PropertiesConstants.STREAM_CHECKPOINT_ENABLE, true)) {
            env.enableCheckpointing(parameterTool.getLong(PropertiesConstants.STREAM_CHECKPOINT_INTERVAL, 10000));
        }
        env.getConfig().setGlobalJobParameters(parameterTool);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //checkpoint保留策略，默认保留
        CheckpointConfig cpConfig = env.getCheckpointConfig();
        cpConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        cpConfig.setCheckpointTimeout(parameterTool.getInt(PropertiesConstants.CHENKPOINT_TIMEOUT, 60000));
        cpConfig.setMinPauseBetweenCheckpoints(500);
        cpConfig.setMaxConcurrentCheckpoints(1);

        StateBackend stateBackend = null;

        //设置应用的状态后端
        if (parameterTool.get(PropertiesConstants.CHECKPOINT_TYPE).equals(PropertiesConstants.CHECKPOINT_ROCKETSDB)) {
            try {
                stateBackend = new RocksDBStateBackend(parameterTool.get(PropertiesConstants.CHECKPOINT_HDFS_DIR), true);
            } catch (IOException e) {
                LOG.error(String.format("set rocks db backend fail: [%s]", e.getMessage()));
            }
        } else if (parameterTool.get(PropertiesConstants.CHECKPOINT_TYPE).equals(PropertiesConstants.CHECKPOINT_FS)) {
            stateBackend = new FsStateBackend(parameterTool.get(PropertiesConstants.CHECKPOINT_HDFS_DIR), false);
        } else {
            stateBackend = new MemoryStateBackend(false);
        }

        env.setStateBackend(stateBackend);

        //将paramtools读取的配置设置为执行环境的全局参数
        env.getConfig().setGlobalJobParameters(parameterTool);

        return env;
    }

    public static StreamExecutionEnvironment prepareForLocal() {
        //set flink runtime env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        return env;
    }
}
