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

import org.rogach.scallop.{ScallopConf, ScallopOption}

class BenchmarkArgs(arguments: Seq[String]) extends ScallopConf(arguments) {

    banner("""
Benchmarker class for running various benchmarks.
    """)

  val iterations: ScallopOption[Int] = opt[Int](short = 'i', default = Some(5),
    descr = "Total iterations to run excluding warmup (for avg time calculation)." +
      " Default is 5 iterations", validate = _ > 0)
  val warmupIterations: ScallopOption[Int] = opt[Int](short = 'w',
    default = Some(3), descr = "Total number of warmup iterations to run. Can take " +
      "any input >=0. Warm up is important for benchmarking to ensure initial " +
      "JVM operations do not skew the result ( classloading etc. )", validate = _ >= 0)
  val outputDirectory: ScallopOption[String] = opt[String](short = 'o',
    default = Some("."), descr = "Base output directory for benchmark results. " +
      "Default is current directory. The final output will go into a subdirectory called" +
      " rapids-tools-benchmark. It will override any directory with the same name")
  val outputFormat: ScallopOption[String] = opt[String](short = 'f',
    default = Some("text"), descr = "Output format for the benchmark results. For text" +
      " the result output will be tabular. In case of json , the results" +
      "will be JSON formatted. Currently supported formats are text, json")
  // Conflict with `iterations` for using short flag - `i`.
  // Going with - `a` for now
  val inputArgs: ScallopOption[String] = opt[String](short = 'a',
    required = false,
    descr = "Input arguments to pass to the benchmark suite. Used as common arguments across " +
      "benchmarks. The format is space separated arguments. For example " +
      "--output-directory /tmp --per-sql /tmp/eventlogs")
  verify()
}
