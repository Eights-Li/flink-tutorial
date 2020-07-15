package com.eights.time

import sensor.bean.SensorReading
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time

class SensorOutOfOrderTimeAssigner extends BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(2)){
  override def extractTimestamp(t: SensorReading): Long = {
    t.timestamp
  }
}
