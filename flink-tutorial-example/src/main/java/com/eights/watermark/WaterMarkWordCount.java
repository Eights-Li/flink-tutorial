package com.eights.watermark;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 1 添加event time + watermark的测试用例
 * 样例数据:
 * flink,1568460862000
 * flink,1568460866000
 * flink,1568460872000
 * flink,1568460873000
 * flink,1568460874000
 * flink,1568460876000
 * flink,1568460877000
 * flink,1568460879000
 *
 * 2 可以为window指定允许迟到数据的时间
 * 如果设置迟到数据的容忍阈值为2秒，会触发两次窗口计算
 * 第一次计算：watermark >= window end time
 * 第二次计算：watermark < window end time + allowedLateness
 * 样例数据：
 * flink,1461756870000
 * flink,1461756883000
 * flink,1461756870000
 * flink,1461756871000
 * flink,1461756872000
 * flink,1461756884000
 * flink,1461756870000
 * flink,1461756871000
 * flink,1461756872000
 * flink,1461756885000
 * flink,1461756870000
 * flink,1461756871000
 * flink,1461756872000
 *
 * 3 迟到的数据可以收集
 * 采用SideOutPut的方式，打tag进行输出
 * 样例数据：
 * flink,1461756870000
 * flink,1461756883000
 * flink,1461756870000
 * flink,1461756871000
 * flink,1461756872000
 * @author Eights
 */
public class WaterMarkWordCount {

    private static final Logger LOG = LoggerFactory.getLogger(WaterMarkWordCount.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000);
        OutputTag<Tuple2<String, Long>> lateTag = new OutputTag<Tuple2<String, Long>>("late-data"){};

        DataStreamSource<String> socketDs = env.socketTextStream("192.168.127.121", 8888);
        socketDs.map(new WordMapFunction())
                .assignTimestampsAndWatermarks(new EventTimeExtractor())
                .keyBy(new KeySelector<Tuple2<String, Long>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Long> data) throws Exception {
                        return data.f0;
                    }
                }).window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(lateTag)
                .process(new WordKeyedProcessWindowFunction()).print();

        //获取迟到数据的侧流
        DataStream<Tuple2<String, Long>> lateDs = socketDs.getSideOutput(lateTag);

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
                    dateFormat.format(Objects.requireNonNull(getCurrentWatermark()).getTimestamp())));

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
