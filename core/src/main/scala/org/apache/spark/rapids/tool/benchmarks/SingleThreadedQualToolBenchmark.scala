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

import com.nvidia.spark.rapids.tool.qualification.QualificationArgs
import com.nvidia.spark.rapids.tool.qualification.QualificationMain.mainInternal

/**
 * This class is used to run the QualificationMain class as a benchmark.
 * This can be used as a reference to write any benchmark class
 * Usage -
 * 1. Override the runBenchmarkSuite method
 * 2. Write the benchmark code in the runBenchmark method passing relevant arguments
 * 3. Write benchmarked code inside
 */
object SingleThreadedQualToolBenchmark extends BenchmarkBase {
  override def runBenchmarkSuite(iterations: Int,
    warmUpIterations: Int,
    outputFormat: String,
    extraArgs: Array[String]): Unit = {
    runBenchmark("Benchmark_Per_SQL_Arg_Qualification") {
      val benchmarker =
        new Benchmark(
          valuesPerIteration = 2,
          output = output,
          outputPerIteration = true,
          warmUpIterations = warmUpIterations,
          minNumIters = iterations)
      val (prefix, suffix) = extraArgs.splitAt(extraArgs.length - 1)
      benchmarker.addCase("Enable_Per_SQL_Arg_Qualification") { _ =>
        mainInternal(new QualificationArgs(prefix :+ "--per-sql" :+ "--num-threads"
          :+ "1" :+ suffix.head),
          enablePB = true)
      }
      benchmarker.addCase("Disable_Per_SQL_Arg_Qualification") { _ =>
        mainInternal(new QualificationArgs(prefix :+ "--num-threads" :+ "1" :+ suffix.head),
          enablePB = true)
      }
      benchmarker.run()
    }
  }
}
