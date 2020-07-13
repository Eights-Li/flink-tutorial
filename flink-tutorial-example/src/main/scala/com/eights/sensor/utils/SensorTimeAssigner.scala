package com.eights.sensor.utils

import com.eights.sensor.bean.SensorReading
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time

class SensorTimeAssigner  extends BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(5)) {

  /** Extracts timestamp from SensorReading. */
  override def extractTimestamp(r: SensorReading): Long = r.timestamp

}
