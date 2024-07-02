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

import java.io.{File, FileOutputStream, OutputStream}

/**
 * This code is mostly copied from org.apache.spark.benchmark.BenchmarkBase
 *
 * A base class for generate benchmark results to a file.
 * For JDK9+, JDK major version number is added to the file names to distinguish the results.
 */
abstract class BenchmarkBase {
  var output: Option[OutputStream] = None
  /**
   * Main process of the whole benchmark.
   * Implementations of this method are supposed to use the wrapper method `runBenchmark`
   * for each benchmark scenario.
   */
  def runBenchmarkSuite(iterations: Int,
      warmUpIterations: Int,
      outputFormat: String,
      mainArgs: Array[String]): Unit

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
   * Any shutdown code to ensure a clean shutdown
   */
  def afterAll(): Unit = {}

  def main(args: Array[String]): Unit = {

    val conf = new BenchmarkArgs(args)
    // TODO: get the dirRoot from the arguments instead
    val dirRoot = conf.outputDirectory().stripSuffix("/")
    val resultFileName = "results.txt"
    val dir = new File(s"$dirRoot/$prefix/")
    if (!dir.exists()) {
      dir.mkdirs()
    }
    val file = new File(dir, resultFileName)
    if (!file.exists()) {
      file.createNewFile()
    }
    output = Some(new FileOutputStream(file))
    runBenchmarkSuite(conf.iterations(),
      conf.warmupIterations(),
      conf.outputFormat(),
      conf.extraArgs().split("\\s+").filter(_.nonEmpty))
    output.foreach { o =>
      if (o != null) {
        o.close()
      }
    }
    afterAll()
  }
}
