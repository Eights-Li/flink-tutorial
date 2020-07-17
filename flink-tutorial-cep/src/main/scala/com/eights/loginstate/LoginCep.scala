package com.eights.loginstate

import java.util

import com.eights.bean.{LoginEvent, Warning}
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object LoginCep {

  /**
   * using cep for Malicious login user
   *
   * @param args input args
   */
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val loginStream: DataStream[LoginEvent] = env.readTextFile("flink-tutorial-cep/src/main/resources/LoginLog.csv")
      .map(elem => {
        val splits: Array[String] = elem.split(",")
        LoginEvent(splits(0).toLong, splits(1), splits(2), splits(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
        override def extractTimestamp(element: LoginEvent): Long = {
          element.eventTime * 1000L
        }
      })

    val fastDateFormat: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

    //set cep pattern
    val failPattern: Pattern[LoginEvent, LoginEvent] = Pattern.begin[LoginEvent]("begin")
      .where(_.eventType == "fail")
      .next("next")
      .where(_.eventType == "fail")
      .within(Time.seconds(2))

    val failPatternStream: PatternStream[LoginEvent] = CEP.pattern(loginStream.keyBy(_.userId), failPattern)

    val failResDataStream: DataStream[Warning] = failPatternStream.select(new LoginFailTwiceFunction)

    failResDataStream.map(elem => (elem.userId,
      fastDateFormat.format(elem.firstFailTime * 1000L),
      fastDateFormat.format(elem.secondFailTime * 1000L),
      elem.warnInfo))
      .print()

    env.execute("cep Malicious login user")
  }

}

class LoginFailTwiceFunction extends PatternSelectFunction[LoginEvent, Warning] {

  override def select(map: util.Map[String, util.List[LoginEvent]]): Warning = {
    val firstFail: LoginEvent = map.getOrDefault("begin", null).iterator().next()
    val secondFail: LoginEvent = map.getOrDefault("next", null).iterator().next()
    Warning(firstFail.userId,
      Math.min(firstFail.eventTime, secondFail.eventTime),
      Math.max(firstFail.eventTime, secondFail.eventTime),
      firstFail.userId + ": have fail twice")
  }
}
