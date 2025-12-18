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

package com.nvidia.spark.rapids.tool.views

import com.nvidia.spark.rapids.tool.analysis.{AppSQLPlanAnalyzer, ProfAppIndexMapperTrait, QualAppIndexMapperTrait}
import com.nvidia.spark.rapids.tool.profiling.{DataSourceProfileResult, SQLAccumProfileResults}

import org.apache.spark.sql.rapids.tool.AppBase
import org.apache.spark.sql.rapids.tool.profiling.ApplicationInfo
import org.apache.spark.sql.rapids.tool.qualification.QualificationAppInfo


trait AppDataSourceViewTrait extends ViewableTrait[DataSourceProfileResult] {
  override def getLabel: String = "Data Source Information"

  private def getIoMetrics(app: AppBase, sqlAccums: Seq[SQLAccumProfileResults]): IoMetrics = {
    val ioMetricHelper = IoMetrics.getIoMetricsHelper(app)
    val finalRes = IoMetrics(0, 0, 0, 0)
    try {
      sqlAccums.foreach { accum =>
        ioMetricHelper.updateIoRecord(finalRes, accum)
      }
    } catch {
      case e: Exception =>
        logError(s"Error while processing DataSource metrics: ${e.getMessage}")
    }
    finalRes
  }

  def getSQLAccums(app: AppBase, appIndex: Integer = 1): Seq[SQLAccumProfileResults] = {
    app match {
      case qApp: QualificationAppInfo =>
        // TODO: We are currently processing SQL plan metrics twice, once in AppSQLPlanAnalyzer and
        //       once here. We should refactor this to avoid the duplicate calculation.
        val sqlAnalyzer = new AppSQLPlanAnalyzer(qApp)
        sqlAnalyzer.processSQLPlanMetrics()
        QualSQLPlanMetricsView.getRawViewFromSqlProcessor(sqlAnalyzer)
      case pApp: ApplicationInfo =>
        ProfSQLPlanMetricsView.getRawView(pApp, appIndex)
    }
  }

  def getRawView(
      apps: Seq[AppBase],
      appSqlAccums: Seq[SQLAccumProfileResults]): Seq[DataSourceProfileResult] = {
    val allRows = zipAppsWithIndex(apps).flatMap { case (app, _) =>
      getRawView(app, appSqlAccums)
    }.toSeq
    if (allRows.isEmpty) {
      allRows
    } else {
      sortView(allRows)
    }
  }

  def getRawView(app: AppBase, index: Int): Seq[DataSourceProfileResult] = {
    val appSqlAccums = getSQLAccums(app, index)
    getRawView(app, appSqlAccums)
  }

  def getRawView(
      app: AppBase,
      appSqlAccums: Seq[SQLAccumProfileResults]): Seq[DataSourceProfileResult] = {
    // Filter appSqlAccums to get only required metrics
    val ioMetricHelper = IoMetrics.getIoMetricsHelper(app)
    val dataSourceMetrics = appSqlAccums.filter(sqlAccum => ioMetricHelper.isIoMetric(sqlAccum))

    val dsFromLastPlan = app.dataSourceInfo.map { ds =>
      val sqlIdtoDs = dataSourceMetrics.filter(
        sqlAccum => sqlAccum.sqlID == ds.sqlID && sqlAccum.nodeID == ds.nodeId)
      val ioMetrics = if (sqlIdtoDs.nonEmpty) {
        getIoMetrics(app, sqlIdtoDs)
      } else {
        IoMetrics.EMPTY_IO_METRICS
      }
      DataSourceProfileResult(ds.sqlID, ds.version, ds.nodeId,
        ds.format, ioMetrics.bufferTime, ioMetrics.scanTime, ioMetrics.dataSize,
        ioMetrics.decodeTime, ds.location, ds.pushedFilters, ds.schema, ds.dataFilters,
        ds.partitionFilters, ds.isFromFinalPlan)
    }
    val dsFromOrigPlans = app.sqlManager.getDataSourcesFromOrigPlans.map { ds =>
      DataSourceProfileResult(ds.sqlID, ds.version, ds.nodeId, ds.format,
        IoMetrics.EMPTY_IO_METRICS.bufferTime, IoMetrics.EMPTY_IO_METRICS.scanTime,
        IoMetrics.EMPTY_IO_METRICS.dataSize, IoMetrics.EMPTY_IO_METRICS.decodeTime,
        ds.location, ds.pushedFilters, ds.schema, ds.dataFilters, ds.partitionFilters,
        ds.isFromFinalPlan)
    }
    (dsFromLastPlan ++ dsFromOrigPlans).toSeq
  }

  override def sortView(rows: Seq[DataSourceProfileResult]): Seq[DataSourceProfileResult] = {
    rows.sortBy(cols => (cols.sqlID, cols.version, cols.location, cols.schema))
  }
}


object QualDataSourceView extends AppDataSourceViewTrait with QualAppIndexMapperTrait {
  // Keep for the following refactor stages to customize the view based on the app type (Qual/Prof)
}

object ProfDataSourceView extends AppDataSourceViewTrait with ProfAppIndexMapperTrait {
  // Keep for the following refactor stages to customize the view based on the app type (Qual/Prof)
}
