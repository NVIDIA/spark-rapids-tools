/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.rapids.tool.benchmarks

import java.io.{OutputStream, PrintStream}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.NANOSECONDS

import org.apache.commons.io.output.TeeOutputStream

import org.apache.spark.sql.rapids.tool.util.{MemoryMetricsTracker, RuntimeUtil, ToolsTimer}

/**
 * This code is mostly copied from org.apache.spark.benchmark.BenchmarkBase
 *
 * Utility class to benchmark components. An example of how to use this is:
 *  val benchmark = new Benchmark("My Benchmark", valuesPerIteration)
 *   benchmark.addCase("V1")(<function>)
 *   benchmark.addCase("V2")(<function>)
 *   benchmark.run
 * This will output the average time to run each function and the rate of each function.
 */
class Benchmark(
    name: String = "Benchmarker",
    valuesPerIteration: Long,
    minNumIters: Int,
    warmUpIterations: Int,
    outputPerIteration: Boolean = false,
    output: Option[OutputStream] = None) {
  import Benchmark._

  val benchmarks: mutable.ArrayBuffer[Case] = mutable.ArrayBuffer.empty[Benchmark.Case]
  val out: PrintStream = if (output.isDefined) {
    new PrintStream(new TeeOutputStream(System.out, output.get))
  } else {
    System.out
  }

  /**
   * Adds a case to run when run() is called. The given function will be run for several
   * iterations to collect timing statistics.
   *
   * @param name of the benchmark case
   * @param numIters if non-zero, forces exactly this many iterations to be run
   */
  def addCase(name: String, numIters: Int = 0)(f: Int => Unit): Unit = {
    addTimerCase(name, numIters) { timer =>
      timer.startTiming()
      f(timer.iteration)
      timer.stopTiming()
    }
  }

  /**
   * Adds a case with manual timing control. When the function is run, timing does not start
   * until timer.startTiming() is called within the given function. The corresponding
   * timer.stopTiming() method must be called before the function returns.
   *
   * @param name of the benchmark case
   * @param numIters if non-zero, forces exactly this many iterations to be run
   */
  def addTimerCase(name: String, numIters: Int = 0)(f: ToolsTimer => Unit): Unit = {
    benchmarks += Benchmark.Case(name, f, numIters)
  }

  /**
   * Runs the benchmark and outputs the results to stdout. This should be copied and added as
   * a comment with the benchmark. Although the results vary from machine to machine, it should
   * provide some baseline.
   */
  def run(): Unit = {
    require(benchmarks.nonEmpty)
    println("-" * 80)
    println("Running benchmark: " + name)
    println("-" * 80)
    val results = benchmarks.map { c =>
      println("  RUNNING CASE : " + c.name)
      println("-" * 80)
      measure(valuesPerIteration, c.numIters)(c.fn)
    }
    println

    val firstBest = results.head.bestMs
    val jvmInfo = RuntimeUtil.getJVMOSInfo
    out.printf(s"%-26s :   %s \n","JVM Name", jvmInfo("jvm.name"))
    out.printf(s"%-26s :   %s \n","Java Version", jvmInfo("jvm.version"))
    out.printf(s"%-26s :   %s \n","OS Name", jvmInfo("os.name"))
    out.printf(s"%-26s :   %s \n","OS Version", jvmInfo("os.version"))
    out.printf(s"%-26s :   %s MB \n","MaxHeapMemory", (Runtime.getRuntime.maxMemory()/1024/1024).toString)
    out.printf(s"%-26s :   %s \n","Total Warm Up Iterations", warmUpIterations.toString)
    out.printf(s"%-26s :   %s \n \n","Total Runtime Iterations", minNumIters.toString)
    val nameLen = Math.max(40, Math.max(name.length, benchmarks.map(_.name.length).max))
    out.printf(s"%-${nameLen}s %14s %14s %11s %20s %18s %18s %18s %18s %10s\n",
      name + ":", "Best Time(ms)", "Avg Time(ms)", "Stdev(ms)","Avg GC Time(ms)",
      "Avg GC Count", "Stdev GC Count","Max GC Time(ms)","Max GC Count", "Relative")
    out.println("-" * (nameLen + 160))
    results.zip(benchmarks).foreach { case (result, benchmark) =>
      out.printf(s"%-${nameLen}s %14s %14s %11s %20s %18s %18s %18s %18s %10s\n",
        benchmark.name,
        "%5.0f" format result.bestMs,
        "%4.0f" format result.avgMs,
        "%5.0f" format result.stdevMs,
        "%5.1f" format result.memoryParams.avgGCTime,
        "%5.1f" format result.memoryParams.avgGCCount,
        "%5.0f" format result.memoryParams.stdDevGCCount,
        "%5d" format result.memoryParams.maxGcTime,
        "%5d" format result.memoryParams.maxGCCount,
        "%3.2fX" format (firstBest / result.bestMs))
    }
    out.println()
  }

  /**
   * Runs a single function `f` for iters, returning the average time the function took and
   * the rate of the function.
   */
  def measure(num: Long, overrideNumIters: Int)(f: ToolsTimer => Unit): Result = {
    System.gc()  // ensures garbage from previous cases don't impact this one
    for (wi <- 0 until warmUpIterations) {
      f(new ToolsTimer(-1))
    }
    val minIters = if (overrideNumIters != 0) overrideNumIters else minNumIters
    val runTimes = ArrayBuffer[Long]()
    val gcCounts = ArrayBuffer[Long]()
    val gcTimes = ArrayBuffer[Long]()
    //For tracking maximum GC over iterations
    for (i <- 0 until minIters) {
      val timer = new ToolsTimer(i)
      val memoryTracker = new MemoryMetricsTracker
      f(timer)
      val runTime = timer.totalTime()
      runTimes += runTime
      gcCounts += memoryTracker.getTotalGCCount
      gcTimes += memoryTracker.getTotalGCTime
      if (outputPerIteration) {
        println("*"*80)
        println(s"Iteration $i took ${NANOSECONDS.toMicros(runTime)} microseconds")
        println("*"*80)
      }
    }
    println("*"*80)
    println(s"  Stopped after $minIters iterations, ${NANOSECONDS.toMillis(runTimes.sum)} ms")
    println("*"*80)
    assert(runTimes.nonEmpty)
    val bestRuntime = runTimes.min
    val avgRuntime = runTimes.sum / runTimes.size
    val stdevRunTime = if (runTimes.size > 1) {
      math.sqrt(runTimes.map(time => (time - avgRuntime) *
        (time - avgRuntime)).sum / (runTimes.size - 1))
    } else {
      0
    }
    val maxGcCount = gcCounts.max
    val stdevGcCount = if (gcCounts.size > 1) {
      math.sqrt(gcCounts.map(gc => (gc - maxGcCount) *
        (gc - maxGcCount)).sum / (gcCounts.size - 1))
    } else {
      0
    }
    val avgGcCount = gcCounts.sum / minIters
    val avgGcTime = gcTimes.sum / minIters
    val maxGcTime = gcTimes.max
    Benchmark.Result(avgRuntime / 1000000.0,  bestRuntime / 1000000.0, stdevRunTime / 1000000.0,
      JVMMemoryParams(avgGcTime, avgGcCount, stdevGcCount, maxGcCount, maxGcTime))
  }
}


object Benchmark {
  case class Case(name: String, fn: ToolsTimer => Unit, numIters: Int)
  case class JVMMemoryParams( avgGCTime:Double, avgGCCount:Double,
      stdDevGCCount: Double, maxGCCount: Long, maxGcTime:Long)
  case class Result(avgMs: Double, bestMs: Double, stdevMs: Double,
      memoryParams: JVMMemoryParams)
}
