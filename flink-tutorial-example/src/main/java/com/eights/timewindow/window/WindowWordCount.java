package com.eights.timewindow.window;

import com.eights.timewindow.source.WordOutOfOrderSource;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

/**
 * 窗口划分元素测试
 * @author Eights
 */
public class WindowWordCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //使用event time处理乱序数据
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

//        DataStreamSource<String> wordDs = env.addSource(new WordSource());
        DataStreamSource<String> wordDs = env.addSource(new WordOutOfOrderSource());



        wordDs.flatMap(new WordFlatMapFunction())
                .assignTimestampsAndWatermarks(new EventTimeExtractor())
                .keyBy(new KeySelector<Tuple4<String, Integer, Long, String>, String>() {
                    @Override
                    public String getKey(Tuple4<String, Integer, Long, String> words) throws Exception {
                        return words.f0;
                    }
                }).timeWindow(Time.seconds(10), Time.seconds(5))
                .process(new WordProcessWindowFunction())
                .print();


        env.execute("Word Window Count");
    }

    private static class WordFlatMapFunction implements FlatMapFunction<String, Tuple4<String, Integer, Long, String>> {
        @Override
        public void flatMap(String s, Collector<Tuple4<String, Integer, Long, String>> collector) throws Exception {
            String[] split = s.split(",");
            collector.collect(Tuple4.of(split[0],
                    1,
                    Long.valueOf(split[1]),
                    DateFormatUtils.format(Long.parseLong(split[1]), "yyyy-MM-dd HH:mm:ss.SSS")));
        }
    }

    /**
     * 处理每个窗口中的数据，累加并输出
     */
    private static class WordProcessWindowFunction extends ProcessWindowFunction<
            Tuple4<String, Integer, Long, String>,
            Tuple4<String, Integer, String, String>,
            String,
            TimeWindow> {

        @Override
        public void process(String key,
                            Context context,
                            Iterable<Tuple4<String, Integer, Long, String>> elements,
                            Collector<Tuple4<String, Integer, String, String>> out) throws Exception {

            Integer sum = 0;

            for (Tuple4<String, Integer, Long, String> element : elements) {
                sum += element.f1;
            }

            String windowStart = DateFormatUtils.format(context.window().getStart(), "yyyy-MM-dd HH:mm:ss");
            String windowEnd = DateFormatUtils.format(context.window().getEnd(), "yyyy-MM-dd HH:mm:ss");

            out.collect(Tuple4.of(key, sum, windowStart, windowEnd));
        }
    }

    private static class EventTimeExtractor implements AssignerWithPeriodicWatermarks<Tuple4<String, Integer, Long, String>> {

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(System.currentTimeMillis() - 5000);
        }

        @Override
        public long extractTimestamp(Tuple4<String, Integer, Long, String> element, long recordTimestamp) {
            return element.f2;
        }
    }

}
