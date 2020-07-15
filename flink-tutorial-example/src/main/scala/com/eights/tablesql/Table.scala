package com.eights.tablesql

import com.eights.sensor.bean.SensorReading
import com.eights.sensor.utils.SensorSource
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._

object Table {

  /**
   * table demo
   *
   * @param args input args
   */
 def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    val sensorDataStream: DataStream[SensorReading] = env.addSource(new SensorSource)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(2)) {
        override def extractTimestamp(element: SensorReading): Long = {
          element.timestamp
        }
      })

    val sensorTable: Table = tableEnv.fromDataStream(sensorDataStream, 'id,'timestamp.rowtime, 'temperature)

    val resTable: Table = sensorTable.window(Tumble over 5.seconds on 'timestamp as 'tw)
      .groupBy('id, 'tw)
      .select('id, 'id.count)

    resTable.toAppendStream[(String, Long)].print()

    env.execute("table demo")


  }

}
