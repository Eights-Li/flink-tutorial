package com.eights.loginstate

import com.eights.bean.LoginEvent
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

object LoginFailState {

  /**
   * according to state filter Malicious login user
   * Continuous login failure within 5 seconds is Malicious user
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
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(10)) {
        override def extractTimestamp(element: LoginEvent): Long = {
          element.eventTime * 1000L
        }
      })

    val timeFormat: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss SSS")

    loginStream.keyBy(_.userId)
      .process(new MatchMaliciousFunction)
      .map(elem => (elem.eventTime, timeFormat.format(elem.eventTime * 1000L), elem.userId, elem.ip))
      .print()

    env.execute("Malicious login user")
  }

}

class MatchMaliciousFunction extends KeyedProcessFunction[Long, LoginEvent, LoginEvent] {

  //def state for each user store login event
  var loginState: ListState[LoginEvent] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    val loginStateDescriptor: ListStateDescriptor[LoginEvent] = new ListStateDescriptor[LoginEvent]("user-login-fail", classOf[LoginEvent])
    loginState = getRuntimeContext.getListState(loginStateDescriptor)
  }

  override def processElement(value: LoginEvent,
                              ctx: KeyedProcessFunction[Long, LoginEvent, LoginEvent]#Context,
                              out: Collector[LoginEvent]): Unit = {

    //set login fail state
    if (value.eventType == "fail") loginState.add(value)

    //set a timer
    ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L + 5 * 1000L)
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Long, LoginEvent, LoginEvent]#OnTimerContext,
                       out: Collector[LoginEvent]): Unit = {
    super.onTimer(timestamp, ctx, out)

    //get the element from loginListState
    val failLoginEvent: ListBuffer[LoginEvent] = ListBuffer()

    import scala.collection.JavaConversions._
    for (item <- loginState.get()) {
      failLoginEvent += item
    }

    //clear state
    loginState.clear()

    if (failLoginEvent.length > 1) out.collect(failLoginEvent.sortBy(_.eventTime).reverse.head)
  }
}
