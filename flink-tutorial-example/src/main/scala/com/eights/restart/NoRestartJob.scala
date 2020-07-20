package com.eights.restart

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.slf4j.{Logger, LoggerFactory}

/**
 * 本案例来自 孙金城 - Flink知其然知其所以然课程 No.24
 * Flink作业不设置失败重试，生成异常之后直接退出
 *
 * 1 直接运行程序，当作业打印出99之后，作业退出。
 * 2 增加env.setRestartStrategy(RestartStrategies.noRestart());观察行为和默认一样。
 */
object NoRestartJob {


  def main(args: Array[String]): Unit = {
    val LOG: Logger = LoggerFactory.getLogger(NoRestartJob.getClass)

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //配置失败退出
    env.setRestartStrategy(RestartStrategies.noRestart())

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
        if (elem._2 % 100 == 0) {
          val msg: String = String.format("Bad Data [%d]...", elem._2)
          LOG.error(msg)
          throw new RuntimeException(msg)
        }
        elem
      })
      .print()

    env.execute("no restart strategy")

  }
}
