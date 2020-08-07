package com.eights.sql

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import sensor.bean.SensorReading
import sensor.utils.SensorSource

object SQL {

  /**
   * flink sql
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

    val sensorTable: Table = tableEnv.fromDataStream(sensorDataStream, 'id, 'timestamp.rowtime, 'temperature)
      .as("id", "ts", "temperature")

    //registry a temp table
    tableEnv.createTemporaryView("sensor_table", sensorTable)

    val resTable: Table = tableEnv.sqlQuery(
      s"""
         |select id, count(id)
         |from sensor_table
         |group by id, tumble(ts, interval '5' second)
         |""".stripMargin)


    env.execute("sql demo")

  }

}
