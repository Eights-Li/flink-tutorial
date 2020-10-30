package com.eights.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * value state demo
 * 展示state在Rich Function中如何使用
 *
 * @author Eights
 */
public class ValueStateDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Tuple2<Long, Long>> dataSource = env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L),
                Tuple2.of(1L, 7L), Tuple2.of(1L, 4L), Tuple2.of(1L, 2L));

        dataSource.keyBy(new KeySelector<Tuple2<Long, Long>, Long>() {
            @Override
            public Long getKey(Tuple2<Long, Long> elem) throws Exception {
                return elem.f0;
            }
        }).flatMap(new CountWindowAverageFunction())
                .print();

        env.execute("value state demo");
    }

    private static class CountWindowAverageFunction extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

        /**
         * value state handle， 第一个字段是数量，第二个字段是和
         */
        private transient ValueState<Tuple2<Long, Long>> sum;

        @Override
        public void open(Configuration parameters) throws Exception {
            //配置状态TTL,newBuilder必须参数，state过期时间
            StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.seconds(1))
                    //更新类型有两种：1 创建和写入 2 读取和写入
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    //配置是否返回过期值，默认不返回
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();

            ValueStateDescriptor<Tuple2<Long, Long>> sumStateDesc = new ValueStateDescriptor<>("average",
                    TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
                    }));

            sumStateDesc.enableTimeToLive(ttlConfig);

            sum = getRuntimeContext().getState(sumStateDesc);
        }

        @Override
        public void flatMap(Tuple2<Long, Long> input,
                            Collector<Tuple2<Long, Long>> collector) throws Exception {
            //初始化状态
            if (null == sum.value()) {
                sum.update(Tuple2.of(0L, 0L));
            }

            Tuple2<Long, Long> currentSum = sum.value();

            currentSum.f0 += 1;
            currentSum.f1 += input.f1;

            sum.update(currentSum);

            if (currentSum.f0 >= 2) {
                collector.collect(Tuple2.of(input.f0, currentSum.f1 / currentSum.f0));
                sum.clear();
            }

        }
    }
}
