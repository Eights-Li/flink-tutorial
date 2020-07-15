package sensor.utils

import java.util.{Calendar, Random}

import sensor.bean.SensorReading
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

import scala.collection.immutable

class SensorSource extends RichParallelSourceFunction[SensorReading] {
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
      // get current time
      val curTime: Long = Calendar.getInstance.getTimeInMillis

      // emit new SensorReading
      curFTemp.foreach(t => srcCtx.collect(SensorReading(t._1, curTime, t._2)))

      // wait for 100 ms
      Thread.sleep(100)
    }

  }

  /** Cancels this SourceFunction. */
  override def cancel(): Unit = {
    running = false
  }
}
