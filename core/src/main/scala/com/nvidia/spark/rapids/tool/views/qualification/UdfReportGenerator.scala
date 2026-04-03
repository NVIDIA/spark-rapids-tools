/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.tool.views.qualification

import org.apache.spark.sql.rapids.tool.qualification.QualificationSummaryInfo


/**
 * Generates UDF detection reports from qualification summary data.
 *
 * Detects UDFs via the ExecInfo.udf flag set during plan parsing and records metadata,
 * such as its name and SQL ID/Stage ID, as described in UdfEntry.
 * Metrics are derived from stage-level unsupported task duration which the UDFs
 * are part of, to give a general idea of impact, and are included in the report.
 */
object UdfReportGenerator {

  case class UdfEntry(
    name: String,
    exec: String,
    sql_id: Long,
    stage_id: Int)

  case class UdfMetrics(
    unsupported_task_duration_ms: Long,
    app_task_duration_ms: Long,
    unsupported_task_duration_pct: Double)

  case class UdfReport(
    has_udfs: Boolean,
    udfs: Seq[UdfEntry],
    metrics: Option[UdfMetrics])

  def generateReport(rec: QualificationSummaryInfo): UdfReport = {
    val udfs = collectUdfs(rec)
    val metrics = if (udfs.nonEmpty) computeMetrics(rec, udfs) else None
    UdfReport(
      has_udfs = udfs.nonEmpty,
      udfs = udfs,
      metrics = metrics)
  }

  private def collectUdfs(rec: QualificationSummaryInfo): Seq[UdfEntry] = {
    rec.planInfo.flatMap { pInfo =>
      pInfo.execInfo.flatMap { execInfo =>
        // Flatten: look at children of cluster nodes (WSCG), and the exec itself otherwise
        val execs = if (execInfo.isClusterNode) {
          execInfo.children.getOrElse(Seq.empty)
        } else {
          Seq(execInfo)
        }

        execs.filter(_.udf).flatMap { e =>
          val stageId = e.stages.headOption.getOrElse(-1)
          val sqlId = e.sqlID

          if (e.unsupportedExprs.nonEmpty) {
            // Container exec (e.g., Project) with named UDF expressions.
            // Report each named expression as a separate UDF entry.
            e.unsupportedExprs.map { expr =>
              UdfEntry(
                name = expr.getOpName,
                exec = e.exec,
                sql_id = sqlId,
                stage_id = stageId)
            }
          } else if (e.exec != "Project") {
            // Actual UDF executor (e.g., ArrowEvalPython, BatchEvalPython).
            // Skip Project nodes with no unsupported expressions since they
            // are just containers for child UDF execs running in Python.
            Seq(UdfEntry(
              name = e.exec,
              exec = e.exec,
              sql_id = sqlId,
              stage_id = stageId))
          } else {
            Seq.empty
          }
        }
      }
    }
  }

  /**
   * Compute timing metrics for a rough estimate of UDF cost.
   *
   * Compute the cumulative unsupportedTaskDuration for stages containing UDFs.
   * Note that this includes all unsupported ops in the stage, not just UDFs,
   * so the metric may overcount. It is just to provide a general idea of impact.
   */
  private def computeMetrics(
      rec: QualificationSummaryInfo,
      udfs: Seq[UdfEntry]): Option[UdfMetrics] = {
    val stageInfo = rec.stageInfo
    if (stageInfo.isEmpty) return None

    val appTaskDuration = stageInfo.map(_.stageTaskTime).sum
    if (appTaskDuration == 0) return None

    val udfStageIds = udfs.map(_.stage_id).filter(_ >= 0).toSet
    val unsupportedTaskDuration = stageInfo
      .filter(s => udfStageIds.contains(s.stageId))
      .map(_.unsupportedTaskDur).sum

    val pct = math.round(1000.0 * unsupportedTaskDuration / appTaskDuration) / 10.0

    Some(UdfMetrics(
      unsupported_task_duration_ms = unsupportedTaskDuration,
      app_task_duration_ms = appTaskDuration,
      unsupported_task_duration_pct = pct))
  }
}
