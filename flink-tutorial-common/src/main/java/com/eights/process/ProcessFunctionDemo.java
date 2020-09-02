package com.eights.process;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import sensor.bean.SensorReading;
import sensor.utils.SensorOutOfOrderSource;

import java.util.Iterator;

public class ProcessFunctionDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        SingleOutputStreamOperator<SensorReading> sensorData = env.addSource(new SensorOutOfOrderSource())
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(SensorReading element) {
                        return element.timestamp();
                    }
                });


        sensorData.keyBy(new KeySelector<SensorReading, String>() {
            @Override
            public String getKey(SensorReading sensorReading) throws Exception {
                return sensorReading.id();
            }
        }).window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
                .trigger(ContinuousEventTimeTrigger.of(Time.seconds(10)))
                .evictor(TimeEvictor.of(Time.seconds(0), true))
                .process(new UvProcessFunction())
                .print();


        env.execute("Sensor Reading exec");
    }

    private static class UvProcessFunction extends ProcessWindowFunction<SensorReading,
            Tuple3<String, String, Long>,
            String,
            TimeWindow> {

        private transient MapState<String, Boolean> uvState;

        private transient ValueState<Long> pvState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            uvState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<String, Boolean>(
                            "uv",
                            String.class,
                            Boolean.class));

            pvState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>(
                            "pv",
                            Long.class));
        }

        @Override
        public void process(String key,
                            Context context,
                            Iterable<SensorReading> elements,
                            Collector<Tuple3<String, String, Long>> out) throws Exception {

            Long pv = 0L;
            Iterator<SensorReading> iterator = elements.iterator();
            while (iterator.hasNext()) {
                pv = pv + 1;
                String userId = iterator.next().id();
                uvState.put(userId, null);
            }
            pvState.update(pvState.value() == null ? 0 : pvState.value() + pv);
            Long uv = 0L;
            Iterator<String> uvIterator = uvState.keys().iterator();
            while (uvIterator.hasNext()) {
                String next = uvIterator.next();
                uv = uv + 1;
            }
            Long value = pvState.value();
            if (null == value) {
                pvState.update(pv);
            } else {
                pvState.update(value + pv);
            }
            out.collect(Tuple3.of(key, "uv", uv));
            out.collect(Tuple3.of(key, "pv", pvState.value()));

        }
    }

}
