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
package com.nvidia.spark.rapids.tool.analysis

import org.apache.spark.sql.rapids.tool.AppBase

/**
 * Aggregates application data for the qualification tool, handling standard Spark metrics and performance analysis.
 * 
 * This object extends [[AppSparkMetricsAggTrait]] for basic Spark metrics aggregation and
 * [[QualAppIndexMapperTrait]] for application index mapping. It provides comprehensive qualification
 * capabilities through:
 * 
 * - Standard Spark metrics aggregation
 * - Performance analysis and qualification
 * - SQL-level metrics processing
 * - Stage-level metrics analysis
 * - Application qualification metrics collection
 * 
 * @see [[AppSparkMetricsAggTrait]]
 * @see [[QualAppIndexMapperTrait]]
 */
object QualSparkMetricsAggregator extends AppSparkMetricsAggTrait with QualAppIndexMapperTrait {

  override def getAggRawMetrics(
      app: AppBase,
      index: Int = 1,
      sqlAnalyzer: Option[AppSQLPlanAnalyzer]): AggRawMetricsResult = {
    val analysisObj = new AppSparkMetricsAnalyzer(app)
    val sqlMetricsAgg = analysisObj.aggregateSparkMetricsBySql(index)
    AggRawMetricsResult(
      analysisObj.aggregateSparkMetricsByJob(index),
      analysisObj.aggregateSparkMetricsByStage(index),
      analysisObj.shuffleSkewCheck(index),
      sqlMetricsAgg,
      analysisObj.aggregateIOMetricsBySql(sqlMetricsAgg),
      analysisObj.aggregateDurationAndCPUTimeBySql(index),
      Seq(analysisObj.maxTaskInputSizeBytesPerSQL(index)),
      Seq.empty) // Qualification doesn't use diagnostic views
  }

  override def getAggregateRawMetrics(
      apps: Seq[AppBase]): AggRawMetricsResult = {
    zipAppsWithIndex(apps).map { case (app, index) =>
      getAggRawMetrics(app, index, sqlAnalyzer = None)
    }.reduce { (agg1, agg2) =>
      AggRawMetricsResult(
        agg1.jobAggs ++ agg2.jobAggs,
        agg1.stageAggs ++ agg2.stageAggs,
        agg1.taskShuffleSkew ++ agg2.taskShuffleSkew,
        agg1.sqlAggs ++ agg2.sqlAggs,
        agg1.ioAggs ++ agg2.ioAggs,
        agg1.sqlDurAggs ++ agg2.sqlDurAggs,
        agg1.maxTaskInputSizes ++ agg2.maxTaskInputSizes,
        agg1.stageDiagnostics ++ agg2.stageDiagnostics)
    }
  }
}
