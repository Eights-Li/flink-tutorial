package com.eights.operator.sink

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaConsumerBase}

object HDFSSink {

  /**
   * reade data from kafka
   * sink data into Hadoop File System
   *
   * @param args input args
   */
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.setParallelism(1)

    val properties = new Properties()

    properties.setProperty("bootstrap.servers", "dn4.develop.com:9092,dn5.develop.com:9092,dn6.develop.com:9092")
    properties.setProperty("group.id", "flink-test-sink-hdfs")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val kafkaSource: FlinkKafkaConsumerBase[String] = new FlinkKafkaConsumer[String]("can-msg-correct-data",
      new SimpleStringSchema(),
      properties)

    val canDataStream: DataStream[String] = env.addSource(kafkaSource)

    //sink to hdfs
    val hdfsSink: StreamingFileSink[String] = StreamingFileSink.forRowFormat(
      new Path("hdfs://master.develop.com:8020/flink-sink-hdfs/kafka2hdfs"),
      new SimpleStringEncoder[String]("UTF-8"))
      .withRollingPolicy(
        DefaultRollingPolicy.builder()
          .withRolloverInterval(TimeUnit.MINUTES.toMillis(5))
          .withInactivityInterval(TimeUnit.MINUTES.toMillis(1))
          .withMaxPartSize(1024 * 1024)
          .build()
      ).build()

    canDataStream.addSink(hdfsSink)

    env.execute("flink sink to hdfs")
  }

}
