package com.eights.operator.transform

import org.apache.flink.streaming.api.scala._

object Reduce {

  /**
   * reduce demo
   *
   * @param args input args
   */
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val inputStream: DataStream[(String, List[String])] = env.fromElements(
      ("en", List("tea")),
      ("fr", List("vin")),
      ("en", List("cake")))

    val resDataStream: DataStream[(String, List[String])] = inputStream.keyBy(0)
      .reduce((a, b) => (a._1, a._2 ::: b._2))

    resDataStream.print()

    env.execute("reduce demo")

  }

}

