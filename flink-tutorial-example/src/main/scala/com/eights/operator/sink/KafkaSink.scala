package com.eights.operator.sink

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import sensor.bean.SensorReading
import sensor.utils.SensorSource

object KafkaSink {

  /**
   * kafka sink
   *
   * @param args input args
   */
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val sensorDataStream: DataStream[SensorReading] = env.addSource(new SensorSource)

    //celsius
    val celsiusDataStream: DataStream[String] = sensorDataStream.map(elem => {
      val celsius: Double = (elem.temperature - 32) * (5.0 / 9.0)
      elem.id + "|" + elem.timestamp + "|" + celsius
    })

    //sink to apache kafka
    celsiusDataStream.addSink(new FlinkKafkaProducer[String]("dn2.eights.com:9092",
      "sensor_celsius",
      new SimpleStringSchema()))

    env.execute("kafka sink demo")

  }

}
