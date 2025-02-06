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

import org.apache.spark.sql.rapids.tool.util.RapidsToolsConfUtil

/**
 * The global runtime injector that will be used to insert checkpoints during runtime.
 * This is used to pull some performance metrics related to Tools.
 */
object RuntimeInjector extends RuntimeCheckpointTrait {
  /**
   * Initializes the runtime injector based on the build properties "benchmarks.checkpoints".
   * @return the runtime injector
   */
  private def loadRuntimeCheckPoint(): RuntimeCheckpointTrait = {
    val buildProps = RapidsToolsConfUtil.loadBuildProperties
    if (buildProps.getProperty("build.benchmarks.checkpoints").contains("dev")) {
      // The benchmark injection is enabled.
      new DevRuntimeCheckpoint
    } else { // loads the noOp implementation by default
      new NoOpRuntimeCheckpoint
    }
  }
  private lazy val runtimeCheckpoint: RuntimeCheckpointTrait = loadRuntimeCheckPoint()

  override def insertMemoryMarker(label: String): Unit = {
    runtimeCheckpoint.insertMemoryMarker(label)
  }
}
