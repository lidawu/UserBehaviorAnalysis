package com.henan.market_analysis

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * 页面广告分析
 *
 * @author Lida
 * @create 2020-08-17 17:46
 */
//定义输入输出的样例类
case class AdClickLog(userId: Long, adId: Long, province: String, city: String, timestamp: Long)

case class AdClickCountByProvince(windowEnd: String, province: String, count: Long)

//侧输出流 黑名单报警信息样例类
case class BlackListUserWarning(userId: Long, adId: Long, msg: String)

object AdClickAnalysis {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //从文件中读取数据
    val resource = getClass.getResource("/AdClickLog.csv")
    val inputStream = env.readTextFile(resource.getPath)

    //转换成样例类，并读取时间戳和waterMark
    val adLogStream = inputStream
      .map(data => {
        val arr = data.split(",")
        AdClickLog(arr(0).toLong, arr(1).toLong, arr(2), arr(3), arr(4).toLong)
      }
      )
      .assignAscendingTimestamps(_.timestamp * 1000L) //用于时间戳是升序的

    //插入一步过滤操作，并将有刷单的行为用户输出的测输出流中(黑名单报警)
    val filterBlackListUserStream: DataStream[AdClickLog] = adLogStream
      .keyBy(data => (data.userId, data.adId))
      .process(new FliterBlackListUserResult(100))

    //开窗聚合统计
    val adCountResultStream = filterBlackListUserStream
      .keyBy(_.province)
      .timeWindow(Time.hours(1), Time.seconds(5))
      .aggregate(new AdCountAgg(), new AdCountWindowResult())

    adCountResultStream.print("count result")
    filterBlackListUserStream.getSideOutput(OutputTag[BlackListUserWarning]("warning")).print("warning")

    env.execute("ad count statistics job")
  }

}

class AdCountAgg() extends AggregateFunction[AdClickLog, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: AdClickLog, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class AdCountWindowResult() extends WindowFunction[Long, AdClickCountByProvince, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[AdClickCountByProvince]): Unit = {
    val end = new Timestamp(window.getEnd).toString
    out.collect(AdClickCountByProvince(end, key, input.head))
  }
}

//自定义KeyedProcessFunction
class FliterBlackListUserResult(maxCount: Int) extends KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog] {
  //定义状态，保存用户对广告的点击量，每天0点定时清空状态的时间戳，标记当前用户是否已经进入黑名单
  lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count", classOf[Long]))
  lazy val resetTimesState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("reset-ts", classOf[Long]))
  lazy val isBlackState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-black", classOf[Boolean]))


  override def processElement(value: AdClickLog, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#Context, out: Collector[AdClickLog]): Unit = {
    //获取当前的countState状态
    val curCount = countState.value()
    //判断只有是第一个数据来了，直接注册0点的清空状态定时器  ---》用于当日统计的个数清空
    if (curCount == 0) {
      val ts = (ctx.timerService().currentProcessingTime() / (1000 * 60 * 60 * 24) + 1) * (24 * 60 * 60 * 1000)
      resetTimesState.update(ts)
      ctx.timerService().registerProcessingTimeTimer(ts)
    }

    //判断count 值是否已经达到定义的预值，如果超过就输出到黑名单中
    if (curCount >= maxCount) {
      //判断是否已在黑名单中，没有的话才输出侧输出流中
      if (!isBlackState.value()) {
        isBlackState.update(true)
        //写入到测输出流中
        ctx.output(new OutputTag[BlackListUserWarning]("warning"), BlackListUserWarning(value.userId, value.adId, "Click ad over " + maxCount + " times today."))
      }
      return
    }
    //正常情况，count +1 ，然后将数据原样输出
    countState.update(curCount + 1)
    out.collect(value)

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#OnTimerContext, out: Collector[AdClickLog]): Unit = {
    if (timestamp == resetTimesState.value())
      isBlackState.clear()
    countState.clear()
  }

}