package com.eights.loginstate

import java.util

import com.eights.bean.{LoginEvent, Warning}
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object LoginFailStateDemo {

  /**
   * according to login fail info to get the Malicious login user
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

    loginStream.keyBy(_.userId)
      .process(new MaliciousUserStateFunction)
      .map(elem => (elem.userId, fastDateFormat.format(elem.firstFailTime * 1000L),
        fastDateFormat.format(elem.secondFailTime * 1000L), elem.warnInfo))
      .print()

    env.execute("Malicious login user")
  }

}

class MaliciousUserStateFunction extends KeyedProcessFunction[Long, LoginEvent, Warning] {

  var loginFailState: ListState[LoginEvent] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    val loginUserStateDesc = new ListStateDescriptor[LoginEvent]("login-user-fail", classOf[LoginEvent])
    loginFailState = getRuntimeContext.getListState(loginUserStateDesc)
  }

  override def processElement(value: LoginEvent,
                              ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context,
                              out: Collector[Warning]): Unit = {

    if (value.eventType == "fail") {

      val loginInter: util.Iterator[LoginEvent] = loginFailState.get().iterator()

      if (loginInter.hasNext) {
        val firstFail: LoginEvent = loginInter.next()

        if (Math.abs(value.eventTime - firstFail.eventTime) <= 2) {
          out.collect(Warning(value.userId,
            Math.min(value.eventTime, firstFail.eventTime),
            Math.max(value.eventTime, firstFail.eventTime),
            value.userId + ":login fail twice"))
        }

        loginFailState.clear()
        loginFailState.add(value)
      } else {
        loginFailState.add(value)
      }
    }

  }
}
