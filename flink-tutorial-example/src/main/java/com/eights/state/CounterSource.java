package com.eights.state;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * 实现带cp功能的source
 *
 * @author Eights
 */
public class CounterSource extends RichParallelSourceFunction<Long> implements CheckpointedFunction {

    /**
     * 当前的offset - exactly once语义
     */
    private Long offset = 0L;

    private volatile boolean isRunning = true;

    private ListState<Long> state;


    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        state.clear();
        state.add(offset);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

        ListStateDescriptor<Long> desc = new ListStateDescriptor<>("state", Long.class);
        state = context.getOperatorStateStore().getListState(desc);

        for (Long elem : state.get()) {
            offset = elem;
        }
    }

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {

        final Object lock = ctx.getCheckpointLock();

        while (isRunning) {
            //保证输出是原子的
            synchronized (lock) {
                ctx.collect(offset);
                offset += 1;
            }
        }

    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
