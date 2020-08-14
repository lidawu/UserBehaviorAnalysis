package com.henan.hotItems_analysis

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * @author Lida
 * @create 2020-08-13 15:46
 */
//定义输入数据样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

//定义窗口聚合结果样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)


object HotItems {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) //定义事件时间语义


    //从文件中读取数据，并转换成样例类 ，提取时间戳生成watermark

    val inputStream: DataStream[String] = env.readTextFile("D:\\maven\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")

    val dataStream: DataStream[UserBehavior] = inputStream
      .map(
        data => {
          val arr = data.split(",")
          UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
        })
      .assignAscendingTimestamps(_.timestamp * 1000) // 生成watermark //*1000 是因为单位是毫秒而数据中是秒

    //得到窗口聚合结果
    val aggStream: DataStream[ItemViewCount] = dataStream
      .filter(_.behavior == "pv") // 过滤pv行为
      .keyBy("itemId") //按照商品Id分组
      .timeWindow(Time.hours(1), Time.minutes(5)) //开窗 ： 设置滑动窗口进行统计
      .aggregate(new CountAgg(), new ItemViewWindowResult())

    val resultStream = aggStream
      .keyBy("windowEnd") // 按照窗口分组，收集当前窗口内的商品count数据
      .process(new TopNHostItems(5)) //自定义处理流程

    resultStream.print()

    env.execute("hot items")
  }

}

/**
 * 自定义预聚合函数AggregateFunction
 * 聚合状态就是当前商品的count值
 */
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] { //传的三个参数： 输入类型，聚合状态类型，输出类型

  override def createAccumulator(): Long = 0L //聚合状态

  //每来一条数据调用一次add，count值加1
  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator //得到状态直接返回给WindowFunction进行操作

  override def merge(a: Long, b: Long): Long = a + b //窗口合并的时候用，在此处没有用
}

//自定义窗口函数windowFunction
class ItemViewWindowResult extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
  /**
   * WindowFunction[IN, OUT, KEY, W <: Window] Key的类型是.keyBy("itemId")按照商品Id分组 中的类型 不是Long 是java的元组类型
   */
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val itemId = key.asInstanceOf[Tuple1[Long]].f0
    val windowEnd = window.getEnd
    val count = input.iterator.next()
    out.collect(ItemViewCount(itemId, windowEnd, count))
  }
}

//自定义keyedProcessFunction
class TopNHostItems(topSize: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] {

  //先定义状态：ListSate --》每一个窗口都应该有一个ListSate保存当前窗口内所有的商品对应的count值
  private var itemViewCountListState: ListState[ItemViewCount] = _


  override def open(parameters: Configuration): Unit = {
    itemViewCountListState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("itemViewCount-list", classOf[ItemViewCount]))
  }

  //每来条数据都要执行
  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, out: Collector[String]): Unit = {

    //每来一条数据，直接加入ListState
    itemViewCountListState.add(value)

    /**
     * 注册一个windowEnd +1 之后触发的定时器
     * 因为每来条数据都执行，那么需要判断注册的定时器是否存在 。在此处不是不判断是因为我们是按照windowEnd进行分组的
     * 所以同一个时间会进入一个组中
     */
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)

  }

  //当定时器触发， 可以认为所有窗口统计结果都一到齐，可以排序输出了
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

    //为了方便排序，另外定义一个ListBuffer ,保存ListState里面的所有数据

    val allItemViewCounts: ListBuffer[ItemViewCount] = ListBuffer()
    val iter = itemViewCountListState.get().iterator()
    while (iter.hasNext) {
      allItemViewCounts += iter.next()
    }
    //清空状态
    itemViewCountListState.clear()

    //按照count 大小进行排序 ，取前n个
    val sortedItemViewCounts = allItemViewCounts.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

    //将排名信息格式化成String ,便于打印输出可视化展示

    val result: StringBuffer = new StringBuffer()
    result.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n")

    //遍历结果列表中每一个ItemViewCount 输出到一行

    for (i <- sortedItemViewCounts.indices) {
      val currentItemViewCount = sortedItemViewCounts(i)
      result.append("NO").append(i + 1).append(": \t")
        .append("商品id =").append(currentItemViewCount.itemId).append("\t")
        .append("热门度 = ").append(currentItemViewCount.count).append("\n")
    }

    result.append("===================================\n")
    Thread.sleep(1000)
    out.collect(result.toString)
  }
}

