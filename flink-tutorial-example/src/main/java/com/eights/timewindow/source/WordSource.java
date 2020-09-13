package com.eights.timewindow.source;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * word source
 * 用于模拟窗口生成的数据
 * @author Eights
 */
public class WordSource implements SourceFunction<String> {

    private static final Logger LOG = LoggerFactory.getLogger(WordSource.class);

    FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");

    @Override
    public void run(SourceContext<String> ctx) throws Exception {

        // 控制大约在 10 秒的倍数的时间点发送事件
        String currTime = String.valueOf(System.currentTimeMillis());
        while (Integer.parseInt(currTime.substring(currTime.length() - 4)) > 100) {
            currTime = String.valueOf(System.currentTimeMillis());
            continue;
        }
        LOG.info(String.format("开始发送事件: [%s]", dateFormat.format(System.currentTimeMillis())));
        // 第 13 秒发送两个事件
        TimeUnit.SECONDS.sleep(13);
        ctx.collect("flink," + System.currentTimeMillis());
        // 产生了一个事件，但是由于网络原因，事件没有发送
        ctx.collect("flink," + System.currentTimeMillis());
        // 第 16 秒发送一个事件
        TimeUnit.SECONDS.sleep(3);
        ctx.collect("flink," + System.currentTimeMillis());
        TimeUnit.SECONDS.sleep(300);
    }

    @Override
    public void cancel() {

    }
}
