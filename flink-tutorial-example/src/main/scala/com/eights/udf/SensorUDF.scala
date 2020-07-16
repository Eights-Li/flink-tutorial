package com.eights.udf

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row
import sensor.bean.SensorReading
import sensor.utils.SensorOutOfOrderSource

object SensorUDF {

  /**
   * add the label according the temperature
   * low medium high
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

    val sensorDs: DataStream[SensorReading] = env.addSource(new SensorOutOfOrderSource)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(2)) {
        override def extractTimestamp(element: SensorReading): Long = {
          element.timestamp
        }
      })
      .map(elem => {
        val celsius: Double = (elem.temperature - 32) * (5.0 / 9.0)
        SensorReading(elem.id, elem.timestamp, celsius)
      })

    val sensorTable: Table = tableEnv.fromDataStream(sensorDs, 'id, 'timestamp as 'event_time,
      'temperature,
      'timestamp.rowtime as 'ts)

    val label = new TemperatureLabel

    //table
    //    val resDataStream: Table = sensorTable.select('id, 'temperature, label('temperature).as('label), 'timestamp)
    //    resDataStream.toAppendStream[(String, Double, String, Long)].print("labelTable")

    //sql
    tableEnv.createTemporaryView("sensor_table", sensorTable)
    tableEnv.registerFunction("label", label)

    val sqlResTable: Table = tableEnv.sqlQuery(
      s"""
         |select id, temperature,
         |label(temperature) as label,
         |event_time
         |from sensor_table
         |""".stripMargin)

    sqlResTable.toAppendStream[(String, Double, String, Long)].print("sqlTable")

    env.execute("flink table udf")
  }

  /**
   * sensor udf
   * input the temperature
   * add the label field
   */
  class TemperatureLabel() extends ScalarFunction {
    def eval(temperature: Double): String = {
      temperature match {
        case temperature if (temperature > 0 && temperature < 10) => "low"
        case temperature if (temperature >= 10 && temperature < 20) => "medium"
        case temperature if temperature >= 20 => "high"
        case _ => "known"
      }
    }
  }

}
