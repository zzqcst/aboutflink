package com.zhi

import org.apache.flink.api.scala.ExecutionEnvironment

object DataSetAPIDemo {
    def main(args: Array[String]): Unit = {
        val env = ExecutionEnvironment.getExecutionEnvironment
        val text = env.fromElements("hello","there")
    }
}
