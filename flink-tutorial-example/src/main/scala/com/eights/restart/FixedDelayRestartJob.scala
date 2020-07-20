package com.eights.restart

import java.util.concurrent.TimeUnit

import org.apache.commons.lang.time.FastDateFormat
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.slf4j.{Logger, LoggerFactory}

/**
 * 来自孙金城 - Flink知其然知其所以然 No.24
 * 采用FixedDelay进行作业重启,Flink作业异常后采用固定的频率进行作业重启
 */
object FixedDelayRestartJob {

  def main(args: Array[String]): Unit = {

    val LOG: Logger = LoggerFactory.getLogger(FixedDelayRestartJob.getClass)

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(2, TimeUnit.SECONDS)))

    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(3, TimeUnit.SECONDS)))

    env.addSource(new SourceFunction[(String, Integer, Long)] {
      override def run(ctx: SourceFunction.SourceContext[(String, Integer, Long)]): Unit = {
        var index = 1

        while (true) {
          ctx.collect(("key", index, System.currentTimeMillis()))
          index += 1
          Thread.sleep(100)
        }

      }

      override def cancel(): Unit = {

      }
    })
      .map(elem => {
        if (elem._2 % 10 == 0) {
          val msg: String = String.format("Bad data [%d]...", elem._2)
          LOG.error(msg)
          throw new RuntimeException(msg)
        }
        (elem._1, elem._2, elem._3, FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss.SSS").format(elem._3))
      }).print()

    env.execute("fixed delay restart")
  }

}
