package com.eights.watermark.wmstrategy;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import sensor.bean.SensorReading;
import sensor.utils.SensorOutOfOrderSource;

import java.time.Duration;
import java.util.Iterator;

/**
 * watermark生成策略
 *
 * @author Eights
 */
public class WaterMarkStrategyDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<SensorReading> sensorSource = env.addSource(new SensorOutOfOrderSource());

        SingleOutputStreamOperator<SensorReading> sensorSourceWithWaterMark = sensorSource.assignTimestampsAndWatermarks(WatermarkStrategy.
                <SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withIdleness(Duration.ofMinutes(1))
                .withTimestampAssigner((elem, timestamp) -> elem.timestamp()));

        sensorSourceWithWaterMark.keyBy(SensorReading::id)
                .timeWindow(Time.seconds(5))
                .process(new SensorKeyedProcessWindowFunction())
                .print();

        env.execute("sensor 5s temperature avg");
    }

    private static class SensorKeyedProcessWindowFunction extends ProcessWindowFunction<
            SensorReading,
            Tuple2<String, Double>,
            String,
            TimeWindow> {

        @Override
        public void process(String key,
                            Context context,
                            Iterable<SensorReading> elements,
                            Collector<Tuple2<String, Double>> out) throws Exception {

            Iterator<SensorReading> elems = elements.iterator();

            double sum = 0.0;
            int count = 0;

            while (elems.hasNext()) {
                sum += elems.next().temperature();
                count++;
            }

            double avg = sum / count;

            out.collect(Tuple2.of(key, avg));
        }
    }
}
