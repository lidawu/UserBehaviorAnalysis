package com.henan.login_fail_detect

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 *
 * 判断任意紧邻的事件，是否连续失败
 * 这个需求其实可以不用定时器触发，直接在状态中存取上一次登录失败的事件，每次都做判断和比对，就可以实现最初的需求。
 *
 *
 * 次方法还是有缺陷，虽然能够在连续两次失败并且在2s内立刻报警，但是如果连续多次会很麻烦
 * 扩展性不好
 * @author Lida
 * @create 2020-08-17 19:55
 */

object LoginFailAdvance {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取数据
    val resource = getClass.getResource("/LoginLog.csv")
    val inputStream = env.readTextFile(resource.getPath)

    //转换成样例类型，并提取数据戳和 watermark
    val loginEventStream = inputStream
      .map(data => {
        val arr = data.split(",")
        LoginEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
        override def extractTimestamp(element: LoginEvent): Long = element.timestamp * 1000L
      }) //是乱序的

    //进行判断和检查， 如果2s之内连续登录失败，输出报警信息
    val loginFailWarningStream = loginEventStream
      .keyBy(_.userId)
      .process(new LoginFailWaringAdvanceResult())

    loginFailWarningStream.print()
    env.execute("login  fail detect job ")
  }
}

class LoginFailWaringAdvanceResult() extends KeyedProcessFunction[Long, LoginEvent, LoginFailWarning] {
  //定义状态，保存当前所有失败事件，保存定时器的时间戳
  lazy val loginFailListState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("loginfail-list", classOf[LoginEvent]))

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#Context, out: Collector[LoginFailWarning]): Unit = {
    //首先判断事件类型
    if (value.Event == "fail") {
      //1. 如果是失败，进一步做判断
      val iter = loginFailListState.get().iterator() //得到一个迭代器
      //判断之前是否有登录失败事件
      if (iter.hasNext){
        //1.1 如果有， 那么判断两次失败的时间差
        val firstFailEvent= iter.next()
        if (value.timestamp<firstFailEvent.timestamp+2){
          //如果在 2s 之内，输出报警
          out.collect(LoginFailWarning(value.userId,firstFailEvent.timestamp,value.timestamp,"login fail 2 times in 2s"))
        }
        //不管报不报警，当前都已处理完毕，将状态更新为最近依次登录失败的事件
        loginFailListState.clear()
        loginFailListState.add(value)
      }else{
        //1.2 如果没有，直接把当前事件添加到 ListState 中
        loginFailListState.add(value)
      }
    }else{
      //2 如果是成功的，直接清空状态
      loginFailListState.clear()
    }

  }
}


/**
 * 为什么这样的数据
 * 1035,83.149.9.216,fail,1558430842
 * 1035,83.149.9.216,success,1558430845
 * 1035,83.149.9.216,fail,1558430843
 * 1035,83.149.24.26,fail,1558430844
 * 不能这样输出 是因为数据一条一条的来的，success就会清空loginFailListState状态
 * // LoginFailWarning(1035,1558430842,1558430843,login fail 2 times in 2s)
 * // LoginFailWarning(1035,1558430843,1558430844,login fail 2 times in 2s)
 *
 */
