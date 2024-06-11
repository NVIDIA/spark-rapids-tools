package com.nvidia.spark.rapids.tool.benchmarker

import java.util.concurrent.TimeoutException

class Benchmarker {
  def benchmark(mainClass: String, args: Array[String], jvmArgs: Array[String],
                runs: Int = 2, timeout: Long = 60000): Unit = {

    var totalTime: Long = 0
    var totalMemory: Long = 0
    var currentJvmArgs = jvmArgs

    for (_ <- 1 to runs) {
      try{
        val (time, memory) = BenchmarkUtils.measureTimeAndMemory(
          mainClass, args, currentJvmArgs, timeout)
        totalTime += time
        totalMemory += memory
      } catch {
        case _: TimeoutException =>
          println(s"Process $mainClass timed out after $timeout milliseconds")
          currentJvmArgs = increaseHeapSize(currentJvmArgs)
        case _: OutOfMemoryError =>
          println(s"Process $mainClass ran out of memory")
          currentJvmArgs = increaseHeapSize(currentJvmArgs)
        case e: Exception =>
          println(s"Process $mainClass failed with exception: $e")
          throw e
      }
    }

    val avgTime = totalTime / runs
    val avgMemory = totalMemory / runs

    generateReport(mainClass, avgTime, avgMemory, runs)
  }

  def generateReport(mainClass: String, avgTime: Long, avgMemory: Long, runs: Int): Unit = {
    println(s"Benchmark Report for $mainClass")
    println(s"Runs: $runs")
    println(f"Average Time: ${avgTime / 1e9}%.2f s")
    println(f"Average Memory Usage: ${avgMemory / 1e6}%.2f MB")
  }

  private def increaseHeapSize(jvmArgs: Array[String]): Array[String] = {
    val heapSizeRegex = "-Xmx([0-9]+)([a-zA-Z]+)".r
    val newJvmArgs = jvmArgs.map {
      case heapSizeRegex(size, unit) =>
        val newSize = size.toInt * 1.5
        print(s"Increasing heap size to -Xmx$newSize$unit")
        s"-Xmx$newSize$unit"
      case arg => arg
    }
    newJvmArgs
  }
}

object Benchmarker {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Usage: Benchmarker <main class> <args>")
      System.exit(1)
    }
    val jvmArgs = Array("-Xmx14G","-XX:+UseG1GC")
    val mainClass = args(0)
    val benchmarker = new Benchmarker
    benchmarker.benchmark(mainClass, args.drop(1), jvmArgs, 10, 100000)
  }
}

