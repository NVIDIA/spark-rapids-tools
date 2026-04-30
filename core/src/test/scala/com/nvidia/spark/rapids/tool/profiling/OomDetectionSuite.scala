/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.tool.profiling

import org.scalatest.funsuite.AnyFunSuite

class OomDetectionSuite extends AnyFunSuite {

  // (description, failureReason, expectedIsGpuOom)
  private val gpuOomTestCases = Seq(
    ("GpuSplitAndRetryOOM",
      "com.nvidia.spark.rapids.jni.GpuSplitAndRetryOOM: " +
        "GPU OutOfMemory: a batch of 1 cannot be split!",
      true),
    ("GpuRetryOOM",
      "com.nvidia.spark.rapids.jni.GpuRetryOOM: GPU OutOfMemory",
      true),
    ("GpuOOM base class",
      "com.nvidia.spark.rapids.jni.GpuOOM: GPU OutOfMemory",
      true),
    ("pre-24.02 jni.SplitAndRetryOOM (no Gpu prefix)",
      "com.nvidia.spark.rapids.jni.SplitAndRetryOOM: " +
        "GPU OutOfMemory: a batch of 1 cannot be split!",
      true),
    ("pre-24.02 jni.RetryOOM (no Gpu prefix)",
      "com.nvidia.spark.rapids.jni.RetryOOM: GPU OutOfMemory",
      true),
    ("CpuSplitAndRetryOOM should not match",
      "com.nvidia.spark.rapids.jni.CpuSplitAndRetryOOM: " +
        "CPU OutOfMemory",
      false),
    ("CpuRetryOOM should not match",
      "com.nvidia.spark.rapids.jni.CpuRetryOOM: CPU OutOfMemory",
      false),
    ("NullPointerException should not match",
      "java.lang.NullPointerException: some error",
      false),
    ("ExecutorLostFailure should not match",
      "ExecutorLostFailure (executor 5 exited) Exit status: 137",
      false)
  )

  gpuOomTestCases.foreach { case (desc, reason, expected) =>
    test(s"isGpuOom: $desc") {
      assert(SparkRapidsOomExceptions.isGpuOom(reason) === expected)
    }
  }
}
