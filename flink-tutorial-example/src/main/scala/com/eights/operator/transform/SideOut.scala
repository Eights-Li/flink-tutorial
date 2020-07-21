package com.eights.operator.transform

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}
import sensor.bean.SensorReading
import sensor.utils.SensorOutOfOrderSource

/**
 * use side out to split data stream
 * 1 define output tag
 * 2 using functions:
 * process function
 * keyed process function
 * coProcess function
 * keyed coProcess function
 * process window function
 * process all window function
 */
object SideOut {

  def main(args: Array[String]): Unit = {

    val LOG: Logger = LoggerFactory.getLogger(SideOut.getClass)

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val sensorStream: DataStream[SensorReading] = env.addSource(new SensorOutOfOrderSource)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(5)) {
        override def extractTimestamp(element: SensorReading): Long = {
          element.timestamp
        }
      }).map(elem => {
      val celsius: Double = (elem.temperature - 32) * (5.0 / 9.0)
      SensorReading(elem.id, elem.timestamp, celsius)
    })
    //define tag
    val lowTag: OutputTag[SensorReading] = new OutputTag[SensorReading]("low")
    val mediaTag: OutputTag[SensorReading] = new OutputTag[SensorReading]("media")
    val highTag: OutputTag[SensorReading] = new OutputTag[SensorReading]("high")

    val sideOutStream: DataStream[SensorReading] = sensorStream.process(
      new ProcessFunction[SensorReading, SensorReading] {
        override def processElement(value: SensorReading,
                                    ctx: ProcessFunction[SensorReading, SensorReading]#Context,
                                    out: Collector[SensorReading]): Unit = {
          if (value.temperature < 10) ctx.output(lowTag, value)
          if (value.temperature >= 10 && value.temperature < 20) ctx.output(mediaTag, value)
          if (value.temperature >= 20) ctx.output(highTag, value)
        }
      })

    val lowStream: DataStream[SensorReading] = sideOutStream.getSideOutput(lowTag)
    val mediaStream: DataStream[SensorReading] = sideOutStream.getSideOutput(mediaTag)
    val highStream: DataStream[SensorReading] = sideOutStream.getSideOutput(highTag)

    lowStream.print("LOW Stream:")
    mediaStream.print("MEDIA Stream")
    highStream.print("HIGH Stream")

    env.execute("side out demo")
  }

}

