package com.eights.upgrade.source;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class MySource implements SourceFunction<Tuple3<String, Integer, Long>> {
    @Override
    public void run(SourceContext<Tuple3<String, Integer, Long>> ctx) throws Exception {
        int index = 1;
        while (true) {
            ctx.collect(new Tuple3<>("key", index++, System.currentTimeMillis()));
            Thread.sleep(100);
        }


    }

    @Override
    public void cancel() {

    }
}
