package com.eights.restart

import org.apache.commons.lang.time.FastDateFormat
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.slf4j.{Logger, LoggerFactory}

/**
 * 来自孙金城 - Flink知其然知其所以然 No.24
 * 开启cp后的，flink作业默认重启策略 - 固定频率重启
 */
object CPRestartJob {

  def main(args: Array[String]): Unit = {

    val LOG: Logger = LoggerFactory.getLogger(CPRestartJob.getClass)

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.enableCheckpointing(20000)

    env.addSource(new SourceFunction[(String, Integer, Long)] {

      override def run(ctx: SourceFunction.SourceContext[(String, Integer, Long)]): Unit = {
        var index = 1
        while (true) {
          index += 1
          ctx.collect(("key", index, System.currentTimeMillis()))
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
      })
        .print()

    env.execute("cp fixed delay restart")
  }

}
