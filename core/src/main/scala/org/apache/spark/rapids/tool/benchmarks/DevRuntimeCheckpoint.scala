/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

import org.apache.spark.sql.rapids.tool.util.RuntimeUtil

/**
 * A simple implementation to insert checkpoints during runtime to pull some performance metrics
 * related to Tools. This is disabled by default and can be enabled by setting the build
 * property `benchmarks.checkpoints`.
 */
class DevRuntimeCheckpoint extends RuntimeCheckpointTrait {
  /**
   * Insert a memory marker with the given label. This will print the memory information.
   * @param label the label for the memory marker
   */
  override def insertMemoryMarker(label: String): Unit = {
    val memoryInfo = RuntimeUtil.getJVMHeapInfo(runGC = true)
    // scalastyle:off println
    println(s"Memory Marker: $label, ${memoryInfo.mkString("\n")}")
    // scalastyle:on println
  }
}
