package com.henan.market_analysis

import java.sql.Timestamp
import java.util.UUID

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/**
 * APP市场推广统计
 *
 * @author Lida
 * @create 2020-08-17 13:40
 */

//定义输入数据的样例类
case class MarketUserBehavior(userId: String, behavior: String, channel: String, timestamp: Long)

//定义输出数据样例类
case class MarketViewCount(windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long)

//自定义测试数据源
class SimulatedEventSource extends RichParallelSourceFunction[MarketUserBehavior] {
  //是否运行的标志位
  var running = true
  //定义用户行为和渠道的集合
  val behaviorSet: Seq[String] = Seq("view", "download", "install", "uninstall")
  val channelSet: Seq[String] = Seq("appstore", "weibo", "wechat", "tieba")

  val rand: Random = Random //随机数生成器

  override def run(ctx: SourceFunction.SourceContext[MarketUserBehavior]): Unit = {
    //定义一个生成数据最大的数量
    val maxCounts = Long.MaxValue
    var count = 0L
    //while 循环，不停地随机产生数据
    while (running && count < maxCounts) {
      val id = UUID.randomUUID().toString
      val behavior = behaviorSet(rand.nextInt(behaviorSet.size))
      val channel = channelSet(rand.nextInt(channelSet.size))
      val ts = System.currentTimeMillis() //系统时间

      ctx.collect(MarketUserBehavior(id, behavior, channel, ts))
      count += 1
      Thread.sleep(50L)
    }
  }

  override def cancel(): Unit = running = false

}

object AppMarketByChannel {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //数据源
    val dataStream = env.addSource(new SimulatedEventSource)
      .assignAscendingTimestamps(_.timestamp)

    //开窗统计输出
    val resultStream = dataStream
      .filter(_.behavior != "uninstall")
      .keyBy(data => (data.channel, data.behavior)) // 分渠道统计
      .timeWindow(Time.days(1), Time.seconds(5)) //
      .process(new MarketCountByChannel())

    resultStream.print()
    env.execute("app market  by  channel job")
  }
}

class MarketCountByChannel() extends ProcessWindowFunction[MarketUserBehavior, MarketViewCount, (String, String), TimeWindow] {
  override def process(key: (String, String), context: Context, elements: Iterable[MarketUserBehavior], out: Collector[MarketViewCount]): Unit = {
    val start = new Timestamp(context.window.getStart).toString
    val end = new Timestamp(context.window.getEnd).toString
    val channel = key._1
    val behavior = key._2
    val count = elements.size

    out.collect(MarketViewCount(start,end,channel,behavior,count))
  }
}