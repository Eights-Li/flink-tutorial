package com.eights.operator.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaConsumerBase}

object KafkaSource {

  /**
   * kafka source
   *
   * @param args input args
   */
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()

    properties.setProperty("bootstrap.servers", "dn2.eights.com:9092")
    properties.setProperty("group.id", "flink_sensor_celsius")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val kafkaSource: FlinkKafkaConsumerBase[String] = new FlinkKafkaConsumer011[String]("sensor_celsius",
      new SimpleStringSchema(),
      properties)
      .setStartFromEarliest()

    val sensorDataStream: DataStream[String] = env.addSource(kafkaSource)

    sensorDataStream.print()

    env.execute("flink kafka consumer")


  }

}
