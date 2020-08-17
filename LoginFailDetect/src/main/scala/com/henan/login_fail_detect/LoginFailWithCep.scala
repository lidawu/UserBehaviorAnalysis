package com.henan.login_fail_detect

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


/**
 * @author Lida
 * @create 2020-08-17 15:35
 */
object LoginFailWithCep {
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
      }) //是乱序的 使用assignTimestampsAndWatermarks
    // 1. 定义匹配的模式，要求是一个登录失败事件后，紧跟另一个登录失败事件
    val loginFailPattern = Pattern
      .begin[LoginEvent]("firstFail").where(_.Event == "fail")
      .next("secondFail").where(_.Event == "fail")
//      .next("thirdFail").where(_.Event == "fail")
      .within(Time.seconds(2))

    //2. 将模式应用到数据流上，得到一个PatternStream
    val patternStream: PatternStream[LoginEvent] = CEP.pattern(loginEventStream.keyBy(_.userId), loginFailPattern)
    //3. 检出符合模式的数据流，需要调用select

    val loginFailWarningStream: DataStream[LoginFailWarning] = patternStream.select(new LoginFailEventMatch)

    loginFailWarningStream.print()
    env.execute("login fail with cep job")
  }

}

// 实现自定义PatternSelectFunction
class LoginFailEventMatch extends PatternSelectFunction[LoginEvent, LoginFailWarning] {
  override def select(pattern: util.Map[String, util.List[LoginEvent]]): LoginFailWarning = {
    // 当前匹配到的事件序列，就保存在Map里
    val firstFailEvent = pattern.get("firstFail").get(0)
    val secondFailEvent = pattern.get("secondFail").iterator().next()
    LoginFailWarning(firstFailEvent.userId, firstFailEvent.timestamp, secondFailEvent.timestamp, "login fail")
  }
}