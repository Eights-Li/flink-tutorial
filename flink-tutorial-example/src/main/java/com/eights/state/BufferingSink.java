package com.eights.state;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import sensor.bean.SensorReading;

import java.util.Collection;
import java.util.List;

/**
 * 实现传感器数据的 buffering sink
 *
 * @author Eights
 */
public class BufferingSink implements SinkFunction<SensorReading>, CheckpointedFunction {

    private final int threshold;

    private transient ListState<SensorReading> cpState;

    private List<SensorReading> bufferedElements;

    public BufferingSink(int threshold, List<SensorReading> bufferedElements) {
        this.threshold = threshold;
        this.bufferedElements = bufferedElements;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        cpState.clear();
        cpState.addAll(bufferedElements);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<SensorReading> sensorDesc = new ListStateDescriptor<>("buffered-elems",
                SensorReading.class);

        cpState = context.getOperatorStateStore().getListState(sensorDesc);

        if (context.isRestored()) {
            bufferedElements.addAll((Collection<? extends SensorReading>) cpState.get());
        }

    }

    @Override
    public void invoke(SensorReading value, Context context) throws Exception {

        bufferedElements.add(value);

        if (bufferedElements.size() == threshold) {
            for (SensorReading element : bufferedElements) {
                //sink到外部系统
                System.out.println(element);
            }
            bufferedElements.clear();
        }

    }
}
