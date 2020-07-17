package com.eights.ordertimeout

import java.util

import com.eights.bean.{OrderEvent, OrderResult}
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.Map

object OrderTimeoutDetect {

  /**
   * using cep to order detection
   * Orders that are not paid within 15 minutes will be cancelled
   *
   * @param args input args
   */


  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val orderStream: DataStream[OrderEvent] = env.readTextFile("flink-tutorial-cep/src/main/resources/OrderLog.csv")
      .map(elem => {
        val splits: Array[String] = elem.split(",")
        OrderEvent(splits(0).toLong, splits(1), splits(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(5)) {
        override def extractTimestamp(element: OrderEvent): Long = element.eventTime * 1000L
      })

    val orderPattern: Pattern[OrderEvent, OrderEvent] = Pattern.begin[OrderEvent]("begin")
      .where(_.eventType == "create")
      .followedBy("follow")
      .where(_.eventType == "pay")
      .within(Time.minutes(15))

    val orderTimeoutOutput: OutputTag[OrderResult] = OutputTag[OrderResult]("orderTimeout")

    val patternStream: PatternStream[OrderEvent] = CEP.pattern(orderStream.keyBy(_.orderId), orderPattern)

    val patternOrderStream: DataStream[OrderResult] = patternStream.select(orderTimeoutOutput)(
      (timeout: Map[String, Iterable[OrderEvent]], timestamp: Long) => {
        val id: Long = timeout.getOrElse("begin", null).iterator.next().orderId
        OrderResult(id, "timeout")
      }
    )(
      (success: Map[String, Iterable[OrderEvent]]) => {
        val id: Long = success.getOrElse("follow", null).iterator.next().orderId
        OrderResult(id, "success")
      })

    val timeoutStream: DataStream[OrderResult] = patternOrderStream.getSideOutput(orderTimeoutOutput)

    timeoutStream.print()

    env.execute("cep order")
  }


}

