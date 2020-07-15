package com.eights.time

import java.util.Date

import sensor.bean.SensorReading
import sensor.utils.SensorOutOfOrderSource
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

import scala.collection.mutable

object SensorTumblingEventTimeWindow {

  /**
   * use event time in tumbling window
   *
   * @param args input args
   */
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val sensorOutOfOrderSource: DataStream[SensorReading] = env.addSource(new SensorOutOfOrderSource)
      .assignTimestampsAndWatermarks(new SensorOutOfOrderTimeAssigner)

    val dateFormat: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss SSS")

    val sensorWindowStream: WindowedStream[(String, Double, Long, String, String), Tuple, TimeWindow] = sensorOutOfOrderSource.map(elem => {
      (elem.id, elem.temperature, elem.timestamp, dateFormat.format(elem.timestamp), dateFormat.format(new Date))
    })
      .filter(_._1 == "sensor_1")
      .keyBy(0)
//      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(2)))


    //collect id and time in sensor for each window
    val eachWindowStream: DataStream[mutable.HashSet[(String, String, String)]] = sensorWindowStream
      .fold(new mutable.HashSet[(String, String, String)]()) { case (set, (id, _, _, eventTime, processTime)) =>
        set.+=((id, eventTime, processTime))
      }

    eachWindowStream.print("window:")


    env.execute("tumbling event time window")

  }

}





