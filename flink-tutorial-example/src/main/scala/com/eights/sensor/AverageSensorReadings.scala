package com.eights.sensor

import com.eights.sensor.bean.SensorReading
import com.eights.sensor.utils.{SensorSource, SensorTimeAssigner}
import org.apache.flink.api.common.functions.{FilterFunction, MapFunction}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object AverageSensorReadings {

  /**
   * compute the average temperature every 5s for sensor
   *
   * @param args input args
   */
  def main(args: Array[String]): Unit = {

    //set up streaming execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //uer event time
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //create a DataStream from a source
    val sensorData: DataStream[SensorReading] = env.addSource(new SensorSource)
      //assign timestamp and watermarks (event time)
      .assignTimestampsAndWatermarks(new SensorTimeAssigner)

    val avgTemp: DataStream[SensorReading] = sensorData
      .filter(new TemperatureFilterFunction)
      .map(new CelsiusMapFunction)
      .keyBy(_.id)
      .timeWindow(Time.seconds(5))
      .apply(new TemperatureAverager)

    avgTemp.print("sensor avg temp:")

    env.execute("Compute avg sensor temperature")

  }

  /**
   * user define map function
   */
  class CelsiusMapFunction extends MapFunction[SensorReading, SensorReading] {

    override def map(value: SensorReading): SensorReading = {
      val celsius: Double = (value.temperature - 32) * (5.0 / 9.0)
      SensorReading(value.id, value.timestamp, celsius)
    }
  }

  /**
   * user define filter function
   */
  class TemperatureFilterFunction extends FilterFunction[SensorReading] {
    override def filter(value: SensorReading): Boolean = {
      value.temperature >= 25
    }
  }

  /** User-defined WindowFunction to compute the average temperature of SensorReadings */
  class TemperatureAverager extends WindowFunction[SensorReading, SensorReading, String, TimeWindow] {

    /** apply() is invoked once for each window */
    override def apply(
                        sensorId: String,
                        window: TimeWindow,
                        vals: Iterable[SensorReading],
                        out: Collector[SensorReading]): Unit = {

      // compute the average temperature
      val (cnt, sum) = vals.foldLeft((0, 0.0))((c, r) => (c._1 + 1, c._2 + r.temperature))
      val avgTemp: Double = sum / cnt

      // emit a SensorReading with the average temperature
      out.collect(SensorReading(sensorId, window.getEnd, avgTemp))
    }
  }

}
