package com.eights.window.countwindow

import sensor.bean.SensorReading
import sensor.utils.SensorSource
import org.apache.flink.streaming.api.scala._

object SlidingCountWindow {

  /**
   * sliding count window
   * @param args input args
   */
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val sensorDataStream: DataStream[SensorReading] = env.addSource(new SensorSource)

    val resDataStream: DataStream[(String, Double)] = sensorDataStream.map(elem => (elem.id, elem.temperature))
      .keyBy(0)
      .countWindow(50, 20)
      .reduce((x, y) => (x._1, x._2.max(y._2)))

      resDataStream.print("max:")

    env.execute("Calculate the maximum value of the first 50 items for every 20 items")


  }

}
