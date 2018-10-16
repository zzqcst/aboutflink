package com.zhi

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import scala.util.control.NonFatal

object TextWordCount {
    def main(args: Array[String]): Unit = {
        val inpath = try {
            ParameterTool.fromArgs(args).get("inpath")
        }catch {
            case NonFatal(e)=>
                System.err.println("No file specified. Please run 'TextWordCount --inpath <path>'")
                return
        }

        val env=ExecutionEnvironment.getExecutionEnvironment
        val text=env.readTextFile(inpath)
        val res=text.flatMap{_.split(" ") filter{_.nonEmpty}}.map{(_,1)}.groupBy(0).sum(1)
        res.writeAsCsv("./res.txt","\n"," ")
        env.execute("textwordcount")
    }
}
