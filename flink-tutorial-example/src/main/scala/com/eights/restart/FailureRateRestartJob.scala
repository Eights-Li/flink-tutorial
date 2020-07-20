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
 * 采用failureRate重启策略
 * 1 作业5分钟内5次失败，测终止作业，每次启动间隔5秒钟，当在5分钟内第5次异常后，终止作业
 */
object FailureRateRestartJob {

  def main(args: Array[String]): Unit = {

    val LOG: Logger = LoggerFactory.getLogger(FailureRateRestartJob.getClass)

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.setRestartStrategy(RestartStrategies.failureRateRestart(
      //5秒内失败5次，作业会一直重启
      5, Time.of(5, TimeUnit.MINUTES), Time.of(5, TimeUnit.SECONDS)
    ))

    env.addSource(new SourceFunction[(String, Integer, Long)] {
      override def run(ctx: SourceFunction.SourceContext[(String, Integer, Long)]): Unit = {
        var index = 1
        while (true) {
          ctx.collect(("key", index, System.currentTimeMillis()))
          index += 1
        }
        Thread.sleep(200)
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
      })
      .print()

    env.execute("fixed delay restart")

  }

}
