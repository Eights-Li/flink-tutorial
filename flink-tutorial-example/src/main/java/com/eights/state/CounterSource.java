package com.eights.state;

import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * 实现带cp功能的source
 * @author Eights
 */
public class CounterSource extends RichParallelSourceFunction<Long> implements CheckpointedFunction {

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

    }

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {

    }

    @Override
    public void cancel() {

    }
}
