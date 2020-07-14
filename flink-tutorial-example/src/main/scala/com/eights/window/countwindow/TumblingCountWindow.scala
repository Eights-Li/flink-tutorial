package com.eights.window.countwindow

import com.eights.sensor.bean.SensorReading
import com.eights.sensor.utils.SensorSource
import org.apache.flink.streaming.api.scala._

object TumblingCountWindow {

  /**
   * tumbling time window
   * @param args input args
   */
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val sensorDataStream: DataStream[SensorReading] = env.addSource(new SensorSource)

    val resDataStream: DataStream[(String, Double)] = sensorDataStream.map(elem => (elem.id, elem.temperature))
      .keyBy(0)
      .countWindow(10)
      .reduce((x, y) => (x._1, x._2.max(y._2)))

    resDataStream.print("max:")

    env.execute("10 count max")

  }

}
