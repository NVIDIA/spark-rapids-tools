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
object QualificationBenchmark extends BenchmarkBase {
  override def runBenchmarkSuite(iterations: Int,
    warmUpIterations: Int,
    outputFormat: String,
    mainArgs: Array[String]): Unit = {
    runBenchmark("QualificationBenchmark") {
      val benchmarker =
        new Benchmark(
          "QualificationBenchmark",
          2,
          output = output,
          outputPerIteration = true,
          warmUpIterations = warmUpIterations,
          minNumIters = iterations)
      benchmarker.addCase("QualificationBenchmark") { _ =>
        mainInternal(new QualificationArgs(mainArgs),
          printStdout = true, enablePB = true)
      }
      benchmarker.run()
    }
  }
}
