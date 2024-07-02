package org.apache.spark.rapids.tool.benchmarks

import com.nvidia.spark.rapids.tool.qualification.QualificationArgs
import com.nvidia.spark.rapids.tool.qualification.QualificationMain.mainInternal

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
