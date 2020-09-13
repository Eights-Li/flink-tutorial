package com.eights.upgrade;

import com.eights.upgrade.source.MySource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * 设置cp后，flink默认在作业取消时清除cp
 * 可以自行设置cp的保留策略
 */
public class KeepCpForRestore {

    private static final Logger LOG = LoggerFactory.getLogger(KeepCpForRestore.class);

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(3, Time.of(3, TimeUnit.SECONDS))
        );

        env.enableCheckpointing(20);

        //任务取消保留cp
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );

        env.setStateBackend(new FsStateBackend("file:\\F:\\bigdata-coding\\flink-tutorial\\flink-tutorial-upgrade\\src\\main\\resources", false));

        env.addSource(new MySource())
                .map(new MyMapFunction())
                .keyBy(0).sum(1).print();

        env.execute("cp demo");

    }

    private static class MyMapFunction implements MapFunction<Tuple3<String, Integer, Long>,Tuple3<String, Integer, Long>> {

        @Override
        public Tuple3<String, Integer, Long> map(Tuple3<String, Integer, Long> data) throws Exception {

            if (data.f1 % 10 == 0) {
                String msg = String.format("Bad data [%d]...", data.f1);
                LOG.error(msg);
                throw new RuntimeException(msg);
            }

            return new Tuple3<String, Integer, Long>(data.f0, data.f1, System.currentTimeMillis());
        }
    }
}
