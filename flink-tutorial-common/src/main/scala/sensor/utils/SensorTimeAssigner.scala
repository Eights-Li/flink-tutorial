package sensor.utils

import sensor.bean.SensorReading
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time

class SensorTimeAssigner  extends BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {

  /** Extracts timestamp from SensorReading. */
  override def extractTimestamp(r: SensorReading): Long = r.timestamp

}
