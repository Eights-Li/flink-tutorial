package com.eights.transform

import com.eights.sensor.bean.SensorReading
import com.eights.sensor.utils.SensorSource
import org.apache.flink.streaming.api.scala._


object Union {

  /**
   * union demo
   * 1 union method merges two or more DataStream of the same type
   * and produces a new DataStream of the same type
   * 2 events are merged in a FIFO fashion
   * 3 no distinct for input stream
   *
   * @param args input args
   */
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

     //get three input sensor stream
     val parisStream: DataStream[(String, SensorReading)] = env.addSource(new SensorSource)
       .map(("paris", _))

     val tokyoStream: DataStream[(String, SensorReading)] = env.addSource(new SensorSource)
       .map(("tokyo", _))

     val rioStream: DataStream[(String, SensorReading)] = env.addSource(new SensorSource)
       .map(("rio", _))

     val allCities: DataStream[(String, SensorReading)] = parisStream.union(tokyoStream)
       .union(rioStream)

     allCities.print()

     env.execute("union demo")

  }

}
