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

package com.nvidia.spark.rapids.tool.analysis.util

import org.apache.spark.sql.rapids.tool.store.TaskModel

/**
 * Accumulator for Job Aggregates.
 * This is an optimization to avoid using the Scala collections API on each field for the entire
 * number of tasks/stages in a Job.
 */
case class JobAggAccum() extends TaskMetricsAccumRec {
  override def addRecord(rec: TaskModel): Unit = {
    throw new UnsupportedOperationException(
      "Not implemented: JobAggAccum accepts only cached records")
  }
}
