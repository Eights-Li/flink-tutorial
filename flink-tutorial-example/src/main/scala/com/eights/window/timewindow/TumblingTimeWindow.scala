package com.eights.window.timewindow

import com.eights.sensor.bean.SensorReading
import com.eights.sensor.utils.SensorSource
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object TumblingTimeWindow {

  /**
   * time Tumbling window
   * @param args input args
   */
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val sensorSource: DataStream[SensorReading] = env.addSource(new SensorSource)

    val resDataStream: DataStream[(String, Double)] = sensorSource.map(elem => (elem.id, elem.temperature))
      .keyBy(0)
      .timeWindow(Time.seconds(15))
      .reduce((x, y) => (x._1, x._2.min(y._2)))

    resDataStream.print("min temperature")

    env.execute("sensor id min temperature")

  }

}
