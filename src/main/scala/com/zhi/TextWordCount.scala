package com.zhi

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import scala.util.control.NonFatal

object TextWordCount {
    def main(args: Array[String]): Unit = {
        /**
          * val params: ParameterTool = ParameterTool.fromArgs(args)
          *
          * // set up execution environment
          * val env = ExecutionEnvironment.getExecutionEnvironment
          *
          * // make parameters available in the web interface
          *     env.getConfig.setGlobalJobParameters(params)
          * val text =
          * if (params.has("input")) {
          *         env.readTextFile(params.get("input"))
          * } else {
          * println("Executing WordCount example with default input data set.")
          * println("Use --input to specify file input.")
          *         env.fromCollection(WordCountData.WORDS)
          * }
          */
        val inpath = try {
            ParameterTool.fromArgs(args).get("inpath")
        } catch {
            case NonFatal(e) =>
                System.err.println("No file specified. Please run 'TextWordCount --inpath <path>'")
                return
        }

        val env = ExecutionEnvironment.getExecutionEnvironment
        val text = env.readTextFile(inpath)
        val res = text.flatMap {
            _.split(" ") filter {
                _.nonEmpty
            }
        }.map {
            (_, 1)
        }.groupBy(0).sum(1)
        res.writeAsCsv("./res.txt", "\n", " ")
        env.execute("textwordcount")
    }
}
