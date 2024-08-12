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

package com.nvidia.spark.rapids.tool.views

import scala.collection.Seq

import com.nvidia.spark.rapids.tool.analysis.{ProfAppIndexMapperTrait, QualAppIndexMapperTrait}
import com.nvidia.spark.rapids.tool.profiling.{DataSourceProfileResult, SQLAccumProfileResults}
import com.nvidia.spark.rapids.tool.qualification.QualSQLPlanAnalyzer

import org.apache.spark.sql.rapids.tool.{AppBase, UnsupportedMetricNameException}
import org.apache.spark.sql.rapids.tool.profiling.ApplicationInfo
import org.apache.spark.sql.rapids.tool.qualification.QualificationAppInfo

case class IoMetrics(
    var bufferTime: Long,
    var scanTime: Long,
    var dataSize: Long,
    var decodeTime: Long
)

object IoMetrics {
  val BUFFER_TIME_LABEL = "buffer time"
  val SCAN_TIME_LABEL = "scan time"
  val DATA_SIZE_LABEL = "size of files read"
  val DECODE_TIME_LABEL = "GPU decode time"
}

trait AppDataSourceViewTrait extends ViewableTrait[DataSourceProfileResult] {
  override def getLabel: String = "Data Source Information"

  private def getIoMetrics(sqlAccums: Seq[SQLAccumProfileResults]): IoMetrics = {
    val finalRes = IoMetrics(0, 0, 0, 0)
    try {
      sqlAccums.map(accum => accum.name match {
        case IoMetrics.BUFFER_TIME_LABEL => finalRes.bufferTime = accum.total
        case IoMetrics.SCAN_TIME_LABEL => finalRes.scanTime = accum.total
        case IoMetrics.DATA_SIZE_LABEL => finalRes.dataSize = accum.total
        case IoMetrics.DECODE_TIME_LABEL => finalRes.decodeTime = accum.total
        case _ => throw UnsupportedMetricNameException(accum.name)
      })
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
        val sqlAnalyzer = new QualSQLPlanAnalyzer(qApp, appIndex)
        sqlAnalyzer.processSQLPlanMetrics()
        QualSQLPlanMetricsView.getRawViewFromSqlProcessor(sqlAnalyzer)
      case pApp: ApplicationInfo =>
        ProfSQLPlanMetricsView.getRawView(pApp, appIndex)
    }
  }

  def getRawView(
      apps: Seq[AppBase],
      appSqlAccums: Seq[SQLAccumProfileResults]): Seq[DataSourceProfileResult] = {
    val allRows = zipAppsWithIndex(apps).flatMap { case (app, index) =>
      getRawView(app, index, appSqlAccums)
    }
    if (allRows.isEmpty) {
      allRows
    } else {
      sortView(allRows)
    }
  }

  def getRawView(app: AppBase, index: Int): Seq[DataSourceProfileResult] = {
    val appSqlAccums = getSQLAccums(app, index)
    getRawView(app, index, appSqlAccums)
  }

  def getRawView(
      app: AppBase,
      index: Int,
      appSqlAccums: Seq[SQLAccumProfileResults]): Seq[DataSourceProfileResult] = {
    // Filter appSqlAccums to get only required metrics
    val dataSourceMetrics = appSqlAccums.filter(
      sqlAccum => sqlAccum.name.contains(IoMetrics.BUFFER_TIME_LABEL)
        || sqlAccum.name.contains(IoMetrics.SCAN_TIME_LABEL)
        || sqlAccum.name.contains(IoMetrics.DECODE_TIME_LABEL)
        || sqlAccum.name.equals(IoMetrics.DATA_SIZE_LABEL))

    app.dataSourceInfo.map { ds =>
      val sqlIdtoDs = dataSourceMetrics.filter(
        sqlAccum => sqlAccum.sqlID == ds.sqlID && sqlAccum.nodeID == ds.nodeId)
      if (sqlIdtoDs.nonEmpty) {
        val ioMetrics = getIoMetrics(sqlIdtoDs)
        DataSourceProfileResult(index, ds.sqlID, ds.nodeId,
          ds.format, ioMetrics.bufferTime, ioMetrics.scanTime, ioMetrics.dataSize,
          ioMetrics.decodeTime, ds.location, ds.pushedFilters, ds.schema)
      } else {
        DataSourceProfileResult(index, ds.sqlID, ds.nodeId,
          ds.format, 0, 0, 0, 0, ds.location, ds.pushedFilters, ds.schema)
      }
    }
  }

  override def sortView(rows: Seq[DataSourceProfileResult]): Seq[DataSourceProfileResult] = {
    rows.sortBy(cols => (cols.appIndex, cols.sqlID, cols.location, cols.schema))
  }
}


object QualDataSourceView extends AppDataSourceViewTrait with QualAppIndexMapperTrait {
  // Keep for the following refactor stages to customize the view based on the app type (Qual/Prof)
}

object ProfDataSourceView extends AppDataSourceViewTrait with ProfAppIndexMapperTrait {
  // Keep for the following refactor stages to customize the view based on the app type (Qual/Prof)
}
