package com.eights.ordertimeout

import com.eights.bean.{OrderEvent, OrderResult}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object OrderTimeoutDetectProcess {

  /**
   * use process function to deal with order detect
   * use value status to mark whether the order has timed out
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
        override def extractTimestamp(element: OrderEvent): Long = {
          element.eventTime * 1000L
        }
      })

    orderStream.keyBy(_.orderId)
      .process(new OrderTimeoutFunction)
      .print()

    env.execute("order detect using process function")


  }

}

class OrderTimeoutFunction extends KeyedProcessFunction[Long, OrderEvent, OrderResult] {

  var payFlag: ValueState[Boolean] = _
  var createFlag: ValueState[Boolean] = _
  var timerTsState: ValueState[Long] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val payFlagDesc = new ValueStateDescriptor[Boolean]("pay-flag", classOf[Boolean])
    val createFlagDesc = new ValueStateDescriptor[Boolean]("create-flag", classOf[Boolean])
    val timerTsStateDesc = new ValueStateDescriptor[Long]("time-flag", classOf[Long])
    payFlag = getRuntimeContext.getState(payFlagDesc)
    createFlag = getRuntimeContext.getState(createFlagDesc)
    timerTsState = getRuntimeContext.getState(timerTsStateDesc)
  }

  override def processElement(value: OrderEvent,
                              ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context,
                              out: Collector[OrderResult]): Unit = {

    val isPayed = payFlag.value()
    val isCreated = createFlag.value()
    val timerTs = timerTsState.value()

    if (value.eventType == "create") {
      if (isPayed) {
        out.collect(OrderResult(value.orderId, "success"))
        payFlag.clear()
        createFlag.clear()
        ctx.timerService().deleteEventTimeTimer(timerTs)
      } else {
        val ts = value.eventTime * 1000L + Time.minutes(15).toMilliseconds
        ctx.timerService().registerEventTimeTimer(ts)
        timerTsState.update(ts)
        createFlag.update(true)
      }
    } else if (value.eventType == "pay") {
      if (isCreated) {
        if (value.eventTime * 1000L < timerTs) {
          out.collect(OrderResult(value.orderId, "success"))
        } else {
          out.collect(OrderResult(value.orderId, "timeout"))
        }

        createFlag.clear()
        timerTsState.clear()
        ctx.timerService().deleteEventTimeTimer(timerTs)
      } else {
        val ts = value.eventTime * 1000L
        ctx.timerService().registerEventTimeTimer(ts)
        timerTsState.update(ts)
        payFlag.update(true)
      }
    }


  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext,
                       out: Collector[OrderResult]): Unit = {
    super.onTimer(timestamp, ctx, out)

    out.collect(OrderResult(ctx.getCurrentKey, "timeout"))

    payFlag.clear()
    createFlag.clear()
    timerTsState.clear()
  }
}
