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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.NANOSECONDS

import org.apache.spark.sql.rapids.tool.util.{MemoryMetricsTracker, ToolsTimer}

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
    minNumIters: Int,
    warmUpIterations: Int,
    outputPerIteration: Boolean = false) {
  import Benchmark._

  val benchmarks: mutable.ArrayBuffer[Case] = mutable.ArrayBuffer.empty[Benchmark.Case]

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
  def run(): Seq[Result] = {
    require(benchmarks.nonEmpty)
    val separator = "-" * 80
    println(separator)
    println("Running benchmark: " + name)
    println(separator)
    val results = benchmarks.map { c =>
      println("  RUNNING CASE : " + c.name)
      println(separator)
      measure(c.name, c.numIters)(c.fn)
    }
    println
    results
  }

  /**
   * Runs a single function `f` for iters, returning the average time the function took and
   * the rate of the function.
   */
  def measure(name: String, overrideNumIters: Int)(f: ToolsTimer => Unit): Result = {
    System.gc()  // ensures garbage from previous cases don't impact this one
    val separator = "-" * 80
    for (wi <- 0 until warmUpIterations) {
      f(new ToolsTimer(-1))
    }
    val minIters = if (overrideNumIters != 0) overrideNumIters else minNumIters
    val runTimes = ArrayBuffer[Long]()
    val gcCounts = ArrayBuffer[Long]()
    val gcTimes = ArrayBuffer[Long]()
    //For tracking maximum GC over iterations
    for (i <- 0 until minIters) {
      System.gc()  // ensures GC for a consistent state across different iterations
      val timer = new ToolsTimer(i)
      val memoryTracker = new MemoryMetricsTracker
      f(timer)
      val runTime = timer.totalTime()
      runTimes += runTime
      gcCounts += memoryTracker.getTotalGCCount
      gcTimes += memoryTracker.getTotalGCTime
      if (outputPerIteration) {
        println(separator)
        println(s"Iteration $i took ${NANOSECONDS.toMicros(runTime)} microseconds")
        println(separator)
      }
    }
    println(separator)
    println(s"  Stopped after $minIters iterations, ${NANOSECONDS.toMillis(runTimes.sum)} ms")
    println(separator)
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
    Benchmark.Result(name, avgRuntime / 1000000.0,
      bestRuntime / 1000000.0,
      stdevRunTime / 1000000.0,
      JVMMemoryParams(avgGcTime, avgGcCount, stdevGcCount, maxGcCount, maxGcTime))
  }
}


object Benchmark {
  case class Case(name: String, fn: ToolsTimer => Unit, numIters: Int)
  case class JVMMemoryParams( avgGCTime:Double, avgGCCount:Double,
      stdDevGCCount: Double, maxGCCount: Long, maxGcTime:Long)
  case class Result(caseName: String, avgMs: Double, bestMs: Double, stdevMs: Double,
      memoryParams: JVMMemoryParams)
}
