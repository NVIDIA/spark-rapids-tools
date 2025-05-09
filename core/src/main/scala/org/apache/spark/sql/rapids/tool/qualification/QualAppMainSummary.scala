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

package org.apache.spark.sql.rapids.tool.qualification

/**
 * The main summary of a qualification output that is used in the global report.
 * The purpose of this subset of information is to provide a high level overview of the analysis
 * without adding significant overhead to the memory footprint.
 * All other detailed information is available in the detailed report QualificationSummaryInfo.
 *
 * The fields in that report should always be generated. In other words, this is the default report
 * that should be read without conditional formats.
 *
 * There is a couple of rules to decide whether a field should be added to that report:
 * 1. the field is going to be used by downstreams.
 * 2. the field is human friendly readable.
 * 3. the field is not a collection of objects and is represented as a simple string/number.
 * 4. the field is not platform specific.
 */
case class QualAppMainSummary(
  appName: String,
  appId: String,
  potentialProblems: Seq[String],
  executorCpuTimePercent: Double,
  endDurationEstimated: Boolean,
  failedSQLIds: Seq[String],
  readFileFormatAndTypesNotSupported: Seq[String],
  writeDataFormat: Seq[String],
  complexTypes: Seq[String],
  nestedComplexTypes: Seq[String],
  longestSqlDuration: Long,
  sqlDataframeTaskDuration: Long,
  nonSqlTaskDurationAndOverhead: Long,
  unsupportedSQLTaskDuration: Long,
  supportedSQLTaskDuration: Long,
  user: String,
  startTime: Long,
  sparkSqlDFWallClockDuration: Long,
  estimatedInfo: EstimatedAppInfo,
  totalCoreSec: Long)

object QualAppMainSummary {
  // Constructs a QualAppMainSummary from a QualificationSummaryInfo by copying the
  // necessary fields.
  def apply(origRec: QualificationSummaryInfo): QualAppMainSummary = {
    QualAppMainSummary(
      appName = origRec.appName,
      appId = origRec.appId,
      potentialProblems = origRec.potentialProblems,
      executorCpuTimePercent = origRec.executorCpuTimePercent,
      endDurationEstimated = origRec.endDurationEstimated,
      failedSQLIds = origRec.failedSQLIds,
      readFileFormatAndTypesNotSupported = origRec.readFileFormatAndTypesNotSupported,
      writeDataFormat = origRec.writeDataFormat,
      complexTypes = origRec.complexTypes,
      nestedComplexTypes = origRec.nestedComplexTypes,
      longestSqlDuration = origRec.longestSqlDuration,
      sqlDataframeTaskDuration = origRec.sqlDataframeTaskDuration,
      nonSqlTaskDurationAndOverhead = origRec.nonSqlTaskDurationAndOverhead,
      unsupportedSQLTaskDuration = origRec.unsupportedSQLTaskDuration,
      supportedSQLTaskDuration = origRec.supportedSQLTaskDuration,
      user = origRec.user,
      startTime = origRec.startTime,
      sparkSqlDFWallClockDuration = origRec.sparkSqlDFWallClockDuration,
      estimatedInfo = origRec.estimatedInfo,
      totalCoreSec = origRec.totalCoreSec
    )
  }
}
