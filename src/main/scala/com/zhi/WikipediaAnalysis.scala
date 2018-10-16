package com.zhi

import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.wikiedits.{WikipediaEditEvent, WikipediaEditsSource}


object WikipediaAnalysis {
    def main(args: Array[String]): Unit = {
        val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        val edits:DataStream[WikipediaEditEvent] = env.addSource(new WikipediaEditsSource())
        val keyedEdits:KeyedStream[WikipediaEditEvent, String] = edits.keyBy(event=>event.getUser)
        val res=keyedEdits.timeWindow(Time.seconds(5)).sum("name")
        res.print()
        env.execute()
    }
}
