package com.eights.transform

import com.eights.sensor.bean.SensorReading
import com.eights.sensor.utils.SensorSource
import org.apache.flink.streaming.api.scala._

object SplitConnect {

  /**
   * split & select
   * 1 split divides an input steam into two or more output steams of the same
   * type as the input stream
   *
   * connect
   * 1 connect method receives a DataStream and returns a ConnectedStreams
   * 2 connect operator can connect two data stream of different type
   *
   * @param args input args
   */
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val sensorDataStream: DataStream[SensorReading] = env.addSource(new SensorSource)

    //split
    val splitStream: SplitStream[SensorReading] = sensorDataStream.map(elem => {
      val celsius = (elem.temperature - 32) * (5.0 / 9.0)
      SensorReading(elem.id, elem.timestamp, celsius)
    }).split(elem => {
      if (elem.temperature > 30) Seq("high") else Seq("low")
    })

    val highStream: DataStream[SensorReading] = splitStream.select("high")
    val lowStream: DataStream[SensorReading] = splitStream.select("low")

    //connect, not same type input stream can connect
    val alert: DataStream[(String, Double)] = highStream.map(elem => (elem.id, elem.temperature))
    val connected: ConnectedStreams[(String, Double), SensorReading] = alert.connect(lowStream)

    //coMap -> return a data stream
    val resDataStream: DataStream[(String, Double, String)] = connected.map(
      alertElem => (alertElem._1, alertElem._2, "Alert"),
      lowElem => (lowElem.id, lowElem.temperature, "Normal")
    )

    resDataStream.filter(_._3.equals("Alert"))
        .print("Alert Info:")

    env.execute("Split and Connect")


  }


}