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

import org.apache.spark.sql.rapids.tool.{AppBase, ToolUtils}
import org.apache.spark.sql.rapids.tool.store.TaskModel

// A trait that provides the common methods used for Spark metrics aggregator
// This is extended by the Qual/Prof aggregators
trait AppAggregatorTrait extends AppIndexMapperTrait {
  def getDurations(tcs: Iterable[TaskModel]): (Long, Long, Long, Double) = {
    val durations = tcs.map(_.duration)
    if (durations.nonEmpty) {
      (durations.sum, durations.max, durations.min,
        ToolUtils.calculateAverage(durations.sum, durations.size, 1))
    } else {
      (0L, 0L, 0L, 0.toDouble)
    }
  }

  def maxWithEmptyHandling(arr: Iterable[Long]): Long = {
    if (arr.isEmpty) {
      0L
    } else {
      arr.max
    }
  }

  def minWithEmptyHandling(arr: Iterable[Long]): Long = {
    if (arr.isEmpty) {
      0L
    } else {
      arr.min
    }
  }

  /**
   * Given an application and its index, this methods creates a new appAnalysis
   * object to aggregate the Raw metrics and returns the result
   * @param app the AppBase to be analyzed
   * @param index the application index
   * @return a single record of AggregateRawMetricsResult containing all the raw aggregated Spark
   *         metrics
   */
  def getAggregateRawMetrics(app: AppBase, index: Int): AggregateRawMetricsResult = {
    val analysisObj = new AppAnalysis(app)
    AggregateRawMetricsResult(
      analysisObj.aggregateSparkMetricsByJob(index),
      analysisObj.aggregateSparkMetricsByStage(index),
      analysisObj.shuffleSkewCheck(index),
      analysisObj.aggregateSparkMetricsBySql(index),
      analysisObj.aggregateIOMetricsBySql(analysisObj.aggregateSparkMetricsBySql(index)),
      analysisObj.aggregateDurationAndCPUTimeBySql(index),
      Seq(analysisObj.maxTaskInputSizeBytesPerSQL(index)))
  }

  /**
   * Given a list of applications, this method aggregates the raw metrics for all the applications
   * and returns the results as a single record
   * @param apps the sequence of the apps to be analyzed
   * @return a single record of all the aggregated metrics
   */
  def getAggregateRawMetrics(
      apps: Seq[AppBase]): AggregateRawMetricsResult = {
    zipAppsWithIndex(apps).map { case (app, index) =>
      getAggregateRawMetrics(app, index)
    }.reduce { (agg1, agg2) =>
      AggregateRawMetricsResult(
        agg1.jobAggs ++ agg2.jobAggs,
        agg1.stageAggs ++ agg2.stageAggs,
        agg1.taskShuffleSkew ++ agg2.taskShuffleSkew,
        agg1.sqlAggs ++ agg2.sqlAggs,
        agg1.ioAggs ++ agg2.ioAggs,
        agg1.sqlDurAggs ++ agg2.sqlDurAggs,
        agg1.maxTaskInputSizes ++ agg2.maxTaskInputSizes)
    }
  }
}
