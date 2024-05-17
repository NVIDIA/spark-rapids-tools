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

package com.nvidia.spark.rapids.tool.analysis

import com.nvidia.spark.rapids.tool.profiling.{IOAnalysisProfileResult, JobStageAggTaskMetricsProfileResult, ShuffleSkewProfileResult, SQLDurationExecutorTimeProfileResult, SQLMaxTaskInputSizes, SQLTaskAggMetricsProfileResult}

/**
 * The result of the aggregation of the raw metrics. It contains the aggregated metrics for an
 * application. This case class is used to allow to separate the aggregation of the metrics from
 * how the view are generated.
 * For example, the profiler tool currently merges both job/stage-level in a single list.
 * As a step toward separating the logic from the views, the analyzer returns
 * AggRawMetricsResult that contains the aggregated metrics for jobs, stages, SQLs, and IOs.
 * In later refactors, we can revisit *TaskMetricsProfileResult to have integer IDs instead of
 * the current format "stage_ID" or "job_ID". We still use the old format to keep the compatibility
 * with other modules.
 *
 * @param jobAggs           the aggregated Spark metrics for jobs
 * @param stageAggs         the aggregated Spark metrics for stages
 * @param taskShuffleSkew   list of tasks that exhibit shuffle skewness
 * @param sqlAggs           the aggregated Spark metrics for SQLs
 * @param ioAggs            lists the SQLs along their IO metrics
 * @param sqlDurAggs        the aggregated duration and CPU time for SQLs
 * @param maxTaskInputSizes a sequence of SQLMaxTaskInputSizes that contains the maximum input size
 */
case class AggRawMetricsResult(
    jobAggs: Seq[JobStageAggTaskMetricsProfileResult],
    stageAggs: Seq[JobStageAggTaskMetricsProfileResult],
    taskShuffleSkew: Seq[ShuffleSkewProfileResult],
    sqlAggs: Seq[SQLTaskAggMetricsProfileResult],
    ioAggs: Seq[IOAnalysisProfileResult],
    sqlDurAggs: Seq[SQLDurationExecutorTimeProfileResult],
    maxTaskInputSizes: Seq[SQLMaxTaskInputSizes])
