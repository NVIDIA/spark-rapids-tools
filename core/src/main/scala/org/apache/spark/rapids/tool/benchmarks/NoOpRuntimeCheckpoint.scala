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

import scala.annotation.nowarn

/**
 * An empty implementation of the Checkpoint interface that inserts NoOps.
 * This is the default implementation that will be used in production and normal builds.
 */
class NoOpRuntimeCheckpoint extends RuntimeCheckpointTrait {
  override def insertMemoryMarker(@nowarn label: String): Unit = {
    // Do nothing. This is a noOp
  }
}
