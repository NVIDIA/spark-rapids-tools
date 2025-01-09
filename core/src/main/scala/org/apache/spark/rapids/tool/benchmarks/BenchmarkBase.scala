/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION.
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

import java.io.{File, FileOutputStream, PrintStream}

import org.apache.commons.io.output.TeeOutputStream

import org.apache.spark.sql.rapids.tool.util.RuntimeUtil

/**
 * This code is mostly copied from org.apache.spark.benchmark.BenchmarkBase
 *
 * A base class for generating benchmark results to a file.
 * For JDK9+, JDK major version number is added to the result file to distinguish
 * the generated results.
 */
abstract class BenchmarkBase {
  private var output: Option[PrintStream] = None
  private var benchmark: Option[Benchmark] = None
  /**
   * Main process of the whole benchmark.
   * Implementations of this method are supposed to use the wrapper method `runBenchmark`
   * for each benchmark scenario.
   */
  def runBenchmarkSuite(inputArgs: Array[String]): Unit

  final def runBenchmark(benchmarkName: String)(func: => Any): Unit = {
    val separator = "=" * 96
    val testHeader = (separator + '\n' + benchmarkName + '\n' + separator + '\n' + '\n').getBytes
    output.foreach(_.write(testHeader))
    func
    output.foreach(_.write('\n'))
  }
  def prefix: String = "rapids-tools-benchmark"
  def suffix: String = ""

  /**
   * Add a benchmark case to the suite
   * @param name Name of the benchmark case
   * @param numIters Number of iterations to run
   * @param f Function to run
   */
  def addCase(name: String, numIters: Int = 0)(f: Int => Unit): Unit = {
    benchmark.get.addCase(name, numIters)(f)
  }

  /**
   * Method to trigger the benchmarker cases run method
   */
  def run(): Unit = {
    // Getting correct output stream
    val printStream = output.getOrElse(System.out)
    // Running the underlying benchmark
    val results: Seq[Benchmark.Result] = benchmark.get.run()
    // Generating the output report
    val firstBest = results.head.bestMs
    val nameLen = Math.max(40, results.map(_.caseName.length).max)
    printStream.printf(s"%-${nameLen}s %14s %14s %11s %20s %18s %18s %18s %18s %10s\n",
      "Benchmark :", "Best Time(ms)", "Avg Time(ms)", "Stdev(ms)", "Avg GC Time(ms)",
      "Avg GC Count", "Stdev GC Count", "Max GC Time(ms)", "Max GC Count", "Relative")
    // scalastyle:off println
    printStream.println("-" * (nameLen + 160))
    results.foreach { result =>
      printStream.printf(s"%-${nameLen}s %14s %14s %11s %20s %18s %18s %18s %18s %10s\n",
        result.caseName,
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
    printStream.println()
    // scalastyle:on println
  }

  /**
   * Method to print the system run specific information
   * @param warmUpIterations Total warm up iterations
   * @param iterations Total runtime iterations
   * @param inputArgs Input arguments
   */
  private def printSystemInformation(warmUpIterations: Int, iterations: Int,
      inputArgs: String ): Unit = {
    val jvmInfo = RuntimeUtil.getJVMOSInfo
    output.get.printf(s"%-26s :   %s \n", "JVM Name", jvmInfo("jvm.name"))
    output.get.printf(s"%-26s :   %s \n", "Java Version", jvmInfo("jvm.version"))
    output.get.printf(s"%-26s :   %s \n", "OS Name", jvmInfo("os.name"))
    output.get.printf(s"%-26s :   %s \n", "OS Version", jvmInfo("os.version"))
    output.get.printf(s"%-26s :   %s MB \n", "MaxHeapMemory",
      (Runtime.getRuntime.maxMemory()/1024/1024).toString)
    output.get.printf(s"%-26s :   %s \n", "Total Warm Up Iterations",
      warmUpIterations.toString)
    output.get.printf(s"%-26s :   %s \n", "Total Runtime Iterations",
      iterations.toString)
    output.get.printf(s"%-26s :   %s \n \n", "Input Arguments", inputArgs)
  }

  /**
   * Any shutdown code to ensure a clean shutdown
   */
  def afterAll(): Unit = {}

  def main(args: Array[String]): Unit = {

    val benchArgs = new BenchmarkArgs(args)
    val dirRoot = benchArgs.outputDirectory().stripSuffix("/")
    val resultFileName = "results.txt"
    val dir = new File(s"$dirRoot/$prefix/")
    if (!dir.exists()) {
      dir.mkdirs()
    }
    val file = new File(dir, resultFileName)
    if (!file.exists()) {
      file.createNewFile()
    }
    // Creating a new output stream
    // Using TeeOutputStream to multiplex output to both file and stdout
    val outputStream = new FileOutputStream(file)
    output = Some(new PrintStream(new TeeOutputStream(System.out, outputStream)))
    benchmark = Some(new Benchmark(minNumIters = benchArgs.iterations(),
      warmUpIterations = benchArgs.warmupIterations(),
      outputPerIteration = true))
    // Printing the system information
    printSystemInformation(benchArgs.warmupIterations(),
      benchArgs.iterations(), benchArgs.inputArgs())
    // Passing the input arguments to the suite function
    runBenchmarkSuite(benchArgs.inputArgs().split("\\s+").filter(_.nonEmpty))
    // Closing the output stream
    outputStream.close()
    afterAll()
  }
}
