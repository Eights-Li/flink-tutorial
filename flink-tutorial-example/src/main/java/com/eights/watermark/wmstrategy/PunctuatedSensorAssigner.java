package com.eights.watermark.wmstrategy;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import sensor.bean.SensorReading;

/**
 * 为传感器数据流每个事件打上watermark
 * @author Eights
 */
public class PunctuatedSensorAssigner implements WatermarkGenerator<SensorReading> {

    @Override
    public void onEvent(SensorReading event, long eventTimestamp, WatermarkOutput output) {
        output.emitWatermark(new Watermark(event.timestamp()));
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        //不需要做任何操作
    }
}
