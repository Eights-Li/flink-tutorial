package com.eights.process;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
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
        }).window(TumblingProcessingTimeWindows.of(Time.seconds(30), Time.seconds(10)))
                .trigger(ContinuousEventTimeTrigger.of(Time.seconds(5)))
                .evictor(TimeEvictor.of(Time.seconds(0), true))
                .process(new UvProcessFunction())
                .print();


        env.execute("Sensor Reading exec");
    }

    private static class UvProcessFunction extends ProcessWindowFunction<SensorReading,
            Tuple2<Long, Long>,
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
                            Collector<Tuple2<Long, Long>> out) throws Exception {

            if (null == pvState.value()) {
                pvState.update(0L);
            }

            Long pv = pvState.value();

            for (SensorReading data : elements) {
                pv += 1;
                String id = data.id();
                if (null == uvState.get(id)) {
                    uvState.put(id, false);
                }
            }

            //pv
            pvState.update(pvState.value() + pv);

            //uv
            Iterator<String> uvIterator = uvState.keys().iterator();

            Long uv = 0L;

            while (uvIterator.hasNext()) {
                uv += 1;
            }

            out.collect(Tuple2.of(pv, uv));


        }
    }

}
