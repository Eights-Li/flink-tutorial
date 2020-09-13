package com.eights.upgrade;

import com.eights.wxutils.MarkdownMessage;
import com.eights.wxutils.WxChatbotClient;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * 来自孙金城 Flink知其然 知其所以然 课程
 * 开启cp后，是否可以根据之前的状态进行续跑
 */
public class EnableCheckpointForFailover {

    private static final Logger LOG = LoggerFactory.getLogger(EnableCheckpointForFailover.class);


    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //设置重启策略
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(3, Time.of(2, TimeUnit.SECONDS))
        );

        //打开cp
        env.enableCheckpointing(20);

        DataStreamSource<Tuple3<String, Integer, Long>> source = env.addSource(new SourceFunction<Tuple3<String, Integer, Long>>() {
            @Override
            public void run(SourceContext<Tuple3<String, Integer, Long>> sourceContext) throws Exception {
                int index = 1;

                while (true) {
                    sourceContext.collect(new Tuple3<String, Integer, Long>("key", index++, System.currentTimeMillis()));
                    Thread.sleep(100);
                }
            }

            @Override
            public void cancel() {

            }
        });

        source.map(new MapFunction<Tuple3<String, Integer, Long>, Tuple3<String, Integer, Long>>() {
            @Override
            public Tuple3<String, Integer, Long> map(Tuple3<String, Integer, Long> elem) throws Exception {
                if (elem.f1 % 10 == 0) {
                    String msg = String.format("Bad Data [%d]...", elem.f1);
                    LOG.error(msg);
                    throw new RuntimeException(msg);
                }
                return new Tuple3<String, Integer, Long>(elem.f0, elem.f1, System.currentTimeMillis());
            }
        }).keyBy(0).sum(1).print();

        try {
            env.execute("cp for failover");
        } catch (Exception e) {
            String errorMsg = e.getMessage();

            MarkdownMessage markdownMessage = new MarkdownMessage();
            markdownMessage.add(MarkdownMessage.getHeaderText(1, "Flink应用名称：Check point for failover"));
            markdownMessage.add(MarkdownMessage.getReferenceText("时间:" + DateFormatUtils.format(System.currentTimeMillis(),"yyyy-MM-dd HH:mm:ss")));
            markdownMessage.add("\n\n");
            markdownMessage.add(MarkdownMessage.getReferenceText("异常信息：" + errorMsg));

            String webHook = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=01845ff6-27c2-4820-8399-136a31ccf8eb";

            try {
                WxChatbotClient.send(webHook, markdownMessage);
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }


    }


}
