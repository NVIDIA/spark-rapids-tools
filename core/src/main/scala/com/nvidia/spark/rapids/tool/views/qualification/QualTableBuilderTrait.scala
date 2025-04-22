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

package com.nvidia.spark.rapids.tool.views.qualification

import com.nvidia.spark.rapids.tool.ToolTextFileWriter

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.util.StringUtils


/**
 * This trait is used by the different report generators to write out the reports.
 */
trait QualTableBuilderTrait[T] extends Logging {
  /**
   * The delimiter to use when writing out the CSV files.
   */
  val delim: String = QualReportGenConfProvider.CSV_DELIMITER

  /**
   * The root directory where the reports should be written.
   */
  val rootDirectory: String

  /**
   * The report configurations. This is a map where the key is the configuration
   * name and the value is the configuration value.
   */
  var reportConfigs: Map[String, String] = Map.empty

  /**
   * This method appends the necessary data to the writer for the given record.
   *
   * @param fWriter the writer to append the data to.
   * @param rec the record to write.
   */
  def appendDataToWriter(fWriter: ToolTextFileWriter, rec: T): Unit

  /**
   * This method generates the output report for the given record.
   *
   * @param rec the record to generate the report for.
   */
  def generateOutputReport(rec: T): Unit

  /**
   * This method allows the report generator to be configured with the
   * given report configurations.
   *
   * @param reportConf the report configurations to add.
   * @param confPrefix the prefix to strip from the configuration name.
   * @return the same instance with the new configurations.
   */
  def withReportConfigs(
      reportConf: Map[String, String], confPrefix: String): QualTableBuilderTrait[T] = {
    val filteredConf = reportConf.filterKeys(k => k.startsWith(confPrefix))
    val processedMap = filteredConf.map { case (cK, cV) =>
      cK.stripPrefix(s"$confPrefix.") -> cV
    }
    reportConfigs ++= processedMap
    this
  }

  /**
   * This method checks if the report should be enabled or not.
   *
   * @return true if the report should be enabled, false otherwise.
   */
  def isReportEnabled: Boolean = {
    reportConfigs.getOrElse("report.enabled", "true").toBoolean
  }

  /**
   * This method checks if the file should be generated or not.
   *
   * @return true if the file should be generated, false otherwise.
   */
  def generateFileEnabled(rec: T): Boolean = {
    isReportEnabled
  }

  /**
   * This method gets the directory where the report should be written.
   *
   * @param subFolderId a function that returns the subfolder id. For example, it can be the app id.
   * @return the directory where the report should be written.
   */
  def getDirectory(subFolderId: () => String): String = {
    s"$rootDirectory/${subFolderId()}"
  }

  /**
   * This method formats the given string for writing out to a CSV file.
   *
   * @param value the string to format.
   * @return the formatted string.
   */
  def formatStr(value: String): String = {
    StringUtils.reformatCSVString(value)
  }
}
