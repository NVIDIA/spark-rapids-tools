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

import org.apache.spark.sql.rapids.tool.util.{RuntimeUtil, ToolsTimer}

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
    name: String,
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
    // scalastyle:off
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
    // The results are going to be processor specific so it is useful to include that.
    out.println(RuntimeUtil.getJVMOSInfo.mkString("\n"))
    val nameLen = Math.max(40, Math.max(name.length, benchmarks.map(_.name.length).max))
    out.printf(s"%-${nameLen}s %14s %14s %11s %10s\n",
      name + ":", "Best Time(ms)", "Avg Time(ms)", "Stdev(ms)", "Relative")
    out.println("-" * (nameLen + 80))
    results.zip(benchmarks).foreach { case (result, benchmark) =>
      out.printf(s"%-${nameLen}s %14s %14s %11s %10s\n",
        benchmark.name,
        "%5.0f" format result.bestMs,
        "%4.0f" format result.avgMs,
        "%5.0f" format result.stdevMs,
        "%3.1fX" format (firstBest / result.bestMs))
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
    var totalTime = 0L
    for (i <- 0 until minIters) {
      val timer = new ToolsTimer(i)
      f(timer)
      val runTime = timer.totalTime()
      runTimes += runTime
      totalTime += runTime

      if (outputPerIteration) {
        // scalastyle:off
        println("*"*80)
        println(s"Iteration $i took ${NANOSECONDS.toMicros(runTime)} microseconds")
        println("*"*80)
        // scalastyle:on
      }
    }
    // scalastyle:off
    println("*"*80)
    println(s"  Stopped after $minIters iterations, ${NANOSECONDS.toMillis(runTimes.sum)} ms")
    println("*"*80)
    // scalastyle:on
    assert(runTimes.nonEmpty)
    val best = runTimes.min
    val avg = runTimes.sum / runTimes.size
    val stdev = if (runTimes.size > 1) {
      math.sqrt(runTimes.map(time => (time - avg) * (time - avg)).sum / (runTimes.size - 1))
    } else 0
    Benchmark.Result(avg / 1000000.0,  best / 1000000.0, stdev / 1000000.0)
  }
}


object Benchmark {
  case class Case(name: String, fn: ToolsTimer => Unit, numIters: Int)
  case class Result(avgMs: Double, bestMs: Double, stdevMs: Double)
}
