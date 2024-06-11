package com.nvidia.spark.rapids.tool.benchmarker

import java.util.concurrent.{TimeoutException, TimeUnit}

object BenchmarkUtils {
  def measureTimeAndMemory(mainClass: String, args: Array[String],
                           jvmArgs: Array[String], timeout: Long): (Long, Long) = {
    val processBuilder = new ProcessBuilder(
      (Seq("java","-cp",System.getProperty("java.class.path"))
        ++ jvmArgs ++ Seq(mainClass) ++ args):_*
    )

    processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT)
    processBuilder.redirectError(ProcessBuilder.Redirect.INHERIT)

    val startTime = System.nanoTime()
    val startMemory = Runtime.getRuntime.totalMemory() - Runtime.getRuntime.freeMemory()

    val process = processBuilder.start()

    try {
      if (!process.waitFor(timeout, TimeUnit.MILLISECONDS)) {
        process.destroy()
        throw new TimeoutException(s"Process $process timed out after $timeout milliseconds")
      }
    } catch {
        case e: InterruptedException =>
          process.destroy()
          throw e
    }

    val endTime = System.nanoTime()
    val endMemory = Runtime.getRuntime.totalMemory() - Runtime.getRuntime.freeMemory()

    val timeElapsed = endTime - startTime
    val memoryUsed = endMemory - startMemory

    (timeElapsed, memoryUsed)
  }
}
