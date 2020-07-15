package com.eights.sensor.utils

import java.util.{Calendar, Random}

import com.eights.sensor.bean.SensorReading
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

import scala.collection.immutable

class SensorOutOfOrderSource extends RichParallelSourceFunction[SensorReading] {
  // flag indicating whether source is still running.
  var running: Boolean = true

  /** run() continuously emits SensorReadings by emitting them through the SourceContext. */
  override def run(srcCtx: SourceContext[SensorReading]): Unit = {

    // initialize random number generator
    val rand = new Random()
    // look up index of this parallel task
    val taskIdx: Int = this.getRuntimeContext.getIndexOfThisSubtask

    // initialize sensor ids and temperatures
    var curFTemp: immutable.Seq[(String, Double)] = (1 to 10).map {
      i => ("sensor_" + (taskIdx * 10 + i), 65 + (rand.nextGaussian() * 20))
    }

    // emit data until being canceled
    while (running) {

      // update temperature
      curFTemp = curFTemp.map(t => (t._1, t._2 + (rand.nextGaussian() * 0.5)))

      // add random 1s delay
      val curTime: Long = Calendar.getInstance.getTimeInMillis - rand.nextInt(3000)

      // emit new SensorReading
      curFTemp.foreach(t => srcCtx.collect(SensorReading(t._1, curTime, t._2)))

      // wait for 100 ms
      Thread.sleep(500)
    }

  }

  /** Cancels this SourceFunction. */
  override def cancel(): Unit = {
    running = false
  }
}
