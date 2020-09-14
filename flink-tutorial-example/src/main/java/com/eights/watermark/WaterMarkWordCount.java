package com.eights.watermark;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * 测试watermark
 * 样例数据:
 * flink,1461756862000
 * flink,1461756866000
 * flink,1461756872000
 * flink,1461756873000
 * flink,1461756874000
 * flink,1461756876000
 * flink,1461756877000
 *
 * @author Eights
 */
public class WaterMarkWordCount {

    private static final Logger LOG = LoggerFactory.getLogger(WaterMarkWordCount.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000);

        DataStreamSource<String> socketDs = env.socketTextStream("192.168.127.121", 8888);
        socketDs.map(new WordMapFunction())
                .assignTimestampsAndWatermarks(new EventTimeExtractor())
                .keyBy(new KeySelector<Tuple2<String, Long>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Long> data) throws Exception {
                        return data.f0;
                    }
                }).window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .process(new WordKeyedProcessWindowFunction()).print();

        env.execute("watermark demo");

    }

    private static class WordMapFunction implements MapFunction<String, Tuple2<String, Long>> {
        @Override
        public Tuple2<String, Long> map(String s) throws Exception {
            String[] words = s.split(",");
            return Tuple2.of(words[0], Long.valueOf(words[1]));
        }
    }

    private static class EventTimeExtractor implements AssignerWithPeriodicWatermarks<Tuple2<String, Long>> {
        FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");

        private long currentMaxEventTime = 0L;
        //最大乱序时间
        private long maxOutOfOrderness = 10000;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxEventTime - maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
            long currentElementEventTime = element.f1;
            currentMaxEventTime = Math.max(currentMaxEventTime, currentElementEventTime);

            LOG.info(String.format("record: [%s], event time: [%s], max event time: [%s], current watermark: [%s]", element,
                    dateFormat.format(element.f1),
                    dateFormat.format(currentMaxEventTime),
                    dateFormat.format(getCurrentWatermark().getTimestamp())));

            return currentElementEventTime;
        }
    }

    private static class WordKeyedProcessWindowFunction extends ProcessWindowFunction<
            Tuple2<String, Long>,
            String,
            String,
            TimeWindow> {

        FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");

        @Override
        public void process(String data,
                            Context context,
                            Iterable<Tuple2<String, Long>> elements,
                            Collector<String> out) throws Exception {

            LOG.info(String.format("当前处理时间: [%s]", dateFormat.format(context.currentProcessingTime())));
            LOG.info(String.format("window start time: [%s]", dateFormat.format(context.window().getStart())));
            LOG.info(String.format("window end time: [%s]", dateFormat.format(context.window().getEnd())));

            List<String> list = new ArrayList<>();
            for (Tuple2<String, Long> ele : elements) {
                list.add(ele.toString() + "|" + dateFormat.format(ele.f1));
            }
            out.collect(list.toString());

        }
    }
}
