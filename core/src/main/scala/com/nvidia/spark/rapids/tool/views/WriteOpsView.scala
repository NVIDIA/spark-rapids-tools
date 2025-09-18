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

package com.nvidia.spark.rapids.tool.views


import com.nvidia.spark.rapids.tool.analysis.{ProfAppIndexMapperTrait, QualAppIndexMapperTrait}
import com.nvidia.spark.rapids.tool.profiling.ProfileResult

import org.apache.spark.sql.rapids.tool.AppBase
import org.apache.spark.sql.rapids.tool.store.WriteOperationRecord
import org.apache.spark.sql.rapids.tool.util.StringUtils

/**
 * Represents a single write operation profiling result.
 * This case class implements the `ProfileResult` trait and provides methods
 * to convert the result into sequences of strings for display or CSV export.
 *
 * @param record The write operation record containing metadata and details.
 */
case class WriteOpProfileResult(record: WriteOperationRecord) extends ProfileResult {

  /**
   * Defines the headers for the output display.
   */
  override def outputHeaders: Array[String] = {
    OutHeaderRegistry.outputHeaders("WriteOpProfileResult")
  }

  /**
   * Converts the profiling result into a sequence of strings for display.
   * Escapes special characters in the description and truncates long strings.
   */
  override def convertToSeq(): Array[String] = {
    Array(record.sqlID.toString,
      record.version.toString,
      record.nodeId.toString,
      record.fromFinalPlan.toString,
      // Extract metadata information
      record.operationMeta.execName(),
      record.operationMeta.dataFormat(),
      record.operationMeta.outputPath(),
      record.operationMeta.table(),
      record.operationMeta.dataBase(),
      record.operationMeta.outputColumns(),
      record.operationMeta.writeMode(),
      record.operationMeta.partitions(),
      record.operationMeta.compressOption(),
      // Escape special characters in the description
      StringUtils.renderStr(record.operationMeta.fullDescr(), doEscapeMetaCharacters = true,
        maxLength = 500, showEllipses = true))
  }

  /**
   * Converts the profiling result into a sequence of strings formatted for CSV output.
   * Escapes special characters and truncates long descriptions to a maximum length.
   */
  override def convertToCSVSeq(): Array[String] = {
    Array(record.sqlID.toString,
      record.version.toString,
      record.nodeId.toString,
      record.fromFinalPlan.toString,
      // Extract metadata information for CSV
      record.operationMeta.execNameCSV,
      record.operationMeta.formatCSV,
      StringUtils.reformatCSVString(record.operationMeta.outputPath()),
      StringUtils.reformatCSVString(record.operationMeta.table()),
      StringUtils.reformatCSVString(record.operationMeta.dataBase()),
      StringUtils.reformatCSVString(record.operationMeta.outputColumns()),
      record.operationMeta.writeMode(),
      StringUtils.reformatCSVString(record.operationMeta.partitions()),
      record.operationMeta.compressOptionCSV(),
      StringUtils.reformatCSVString(
        // Escape special characters in the description and trim at 500 characters.
        StringUtils.renderStr(record.operationMeta.fullDescr(), doEscapeMetaCharacters = true,
          maxLength = 500, showEllipses = true)))
  }
}

/**
 * A trait for creating views of write operation profiling results.
 * This trait provides methods to extract raw results, sort them, and label the view.
 */
trait WriteOpsViewTrait extends ViewableTrait[WriteOpProfileResult] {

  /**
   * Returns the label for the view.
   */
  override def getLabel: String = "Write Operations"

  /**
   * Extracts raw write operation records from the given application and maps them
   * to `WriteOpProfileResult` instances.
   *
   * @param app The application containing write operation records.
   * @param index The index of the application.
   * @return A sequence of `WriteOpProfileResult` instances.
   */
  def getRawView(app: AppBase, index: Int): Seq[WriteOpProfileResult] = {
    app.getWriteOperationRecords().iterator.map { w =>
      WriteOpProfileResult(w)
    }.toSeq
  }

  /**
   * Sorts the write operation profiling results by application index, SQL ID,
   * plan version, and node ID.
   *
   * @param rows The sequence of profiling results to sort.
   * @return A sorted sequence of profiling results.
   */
  override def sortView(rows: Seq[WriteOpProfileResult]): Seq[WriteOpProfileResult] = {
    rows.sortBy(cols => (cols.record.sqlID, cols.record.version,
      cols.record.nodeId))
  }
}

/**
 * A view for write operation profiling results specific to Qualification workflows.
 * Extends `WriteOpsViewTrait` and implements `QualAppIndexMapperTrait` for customization.
 */
object QualWriteOpsView extends WriteOpsViewTrait with QualAppIndexMapperTrait {
  // Placeholder for future customization specific to Qualification workflows.
}

/**
 * A view for write operation profiling results specific to Profiling workflows.
 * Extends `WriteOpsViewTrait` and implements `ProfAppIndexMapperTrait` for customization.
 */
object ProfWriteOpsView extends WriteOpsViewTrait with ProfAppIndexMapperTrait {
  // Placeholder for future customization specific to Profiling workflows.
}
