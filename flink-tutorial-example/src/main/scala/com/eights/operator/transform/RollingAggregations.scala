package com.eights.operator.transform

import org.apache.flink.streaming.api.scala._

object RollingAggregations {

  /**
   * demo for rolling aggregations
   *
   * @param args input args
   */
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val inputStream: DataStream[(Int, Int, Int)] = env.fromElements((1, 2, 2),
      (2, 3, 1),
      (2, 2, 4),
      (1, 5, 3))

    val resStream: DataStream[(Int, Int, Int)] = inputStream.keyBy(0).sum(1)

    resStream.print()

    env.execute("rolling aggregations demo")
  }

}
