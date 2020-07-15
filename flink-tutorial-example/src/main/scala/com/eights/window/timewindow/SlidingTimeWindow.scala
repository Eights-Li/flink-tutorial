package com.eights.window.timewindow

import sensor.bean.SensorReading
import sensor.utils.SensorSource
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object SlidingTimeWindow {

  /**
   * sliding time window
   * @param args input args
   */
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val sensorDataStream: DataStream[SensorReading] = env.addSource(new SensorSource)

    val resDataStream: DataStream[(String, Double)] = sensorDataStream.map(elem => (elem.id, elem.temperature))
      .keyBy(0)
      .timeWindow(Time.seconds(30), Time.seconds(10))
      .reduce((x, y) => (x._1, x._2.min(y._2)))

    resDataStream.print("min:")

    env.execute("Calculate the minimum value of the first 30 seconds every 10 seconds")
  }

}
