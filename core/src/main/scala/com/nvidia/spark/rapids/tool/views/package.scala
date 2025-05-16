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

package com.nvidia.spark.rapids.tool

/**
 * Package that contains implementation specific to generate the tools views. This is part of the
 * refactor code to separate the internal object representation from the final output format.
 */
package object views {
  val STAGE_AGG_LABEL = "Stage level aggregated task metrics"
  val JOB_AGG_LABEL = "Job level aggregated task metrics"
  val TASK_SHUFFLE_SKEW = "Shuffle Skew Check"
  val SQL_AGG_LABEL = "SQL level aggregated task metrics"
  val IO_LABEL = "IO Metrics"
  val SQL_DUR_LABEL = "SQL Duration and Executor CPU Time Percent"
  val SQL_MAX_INPUT_SIZE = "SQL Max Task Input Size"
  val STAGE_DIAGNOSTICS_LABEL = "Stage Level Diagnostic Metrics"
  val CLUSTER_INFORMATION_LABEL = "Cluster Information"

  val AGG_DESCRIPTION = Map(
    STAGE_AGG_LABEL -> "Stage metrics",
    JOB_AGG_LABEL -> "Job metrics",
    SQL_AGG_LABEL -> "SQL metrics",
    IO_LABEL -> "IO Metrics per SQL",
    SQL_DUR_LABEL -> "Total duration and CPUTime per SQL",
    TASK_SHUFFLE_SKEW ->
      "(When task's Shuffle Read Size > 3 * Avg Stage-level size)"
  )
}
