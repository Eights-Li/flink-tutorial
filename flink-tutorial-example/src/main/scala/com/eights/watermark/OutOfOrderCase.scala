package com.eights.watermark

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object OutOfOrderCase {

  /**
   * 本案例来自 孙金城 - Flink知其然知其所以然 No.22
   * 利用watermark的机制解决流计算的乱序问题
   * 理解watermark到底是个啥
   *
   * @param args input args
   */
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.addSource(new SourceFunction[(String, Long)]() {
      override def run(ctx: SourceFunction.SourceContext[(String, Long)]): Unit = {
        //数据序列 - 5秒一个窗口
        //0 1 2 3 3 4 | 5 4 6 6 7 8 | 10 8 9
        ctx.collect("key", 0L)
        ctx.collect("key", 1000L)
        ctx.collect("key", 2000L)
        ctx.collect("key", 3000L)
        ctx.collect("key", 3000L)
        ctx.collect("key", 4000L)
        ctx.collect("key", 5000L)
        // out of order
        ctx.collect("key", 4000L)
        ctx.collect("key", 6000L)
        ctx.collect("key", 6000L)
        ctx.collect("key", 7000L)
        ctx.collect("key", 8000L)
        ctx.collect("key", 10000L)
        // out of order
        ctx.collect("key", 8000L)
        ctx.collect("key", 9000L)
      }

      override def cancel(): Unit = {}
    })
      .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks[(String, Long)] {
        //0 1 2 3 3 4 | 5 6 6 7 8 | 10 = 13 | 32 | 10
        //        val outOfOrder = 0

        // 0 1 2 3 3 4 4 | 5 6 6 7 8 8 9 | 10 = 17 | 49 | 10
        val outOfOrder = 3000

        override def checkAndGetNextWatermark(lastElement: (String, Long), extractedTimestamp: Long): Watermark = {

          val ts: Long = lastElement._2 - outOfOrder

          //watermark就是一个时间戳
          //ourOfOrder = 0表示，严格按照数据的事件时间生成时间戳，迟到的数据会被抛弃
          //outOfOrder = 3000表示，允许按照事件时间延迟3秒触发计算，根据时间序列获取乱序数据正确的结果
          new Watermark(ts)
        }

        override def extractTimestamp(element: (String, Long), previousElementTimestamp: Long): Long = {
          element._2
        }
      }).keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .sum(1)
      .map(elem => (elem._1, elem._2 / 1000))
      .print()

    env.execute("using watermark to deal with out of order data")

  }
}
