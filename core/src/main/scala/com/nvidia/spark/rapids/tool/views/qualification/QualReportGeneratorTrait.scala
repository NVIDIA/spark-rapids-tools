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

import org.apache.hadoop.conf.Configuration

/**
 * The trait that defines how to write out the output to a report.
 * There are two types of reports: global and per-app.
 *
 * The global report is written out once the qualification tools finishes the entire analysis
 * and the per app tables are written out once per application.
 *
 * [T] the type of the record to write out to the report.
 */
trait QualReportGeneratorTrait[T] {
  /**
   * The predicate to select the table definitions. For example, it can be by label, or by-scope.
   */
  val tableSelector: QualOutputTableDefinition => Boolean
  /**
   * The table definitions that are selected by the `tableSelector`.
   */
  lazy val loadedTableDef: Seq[QualOutputTableDefinition] = {
    QualReportGenConfProvider.loadTableReports(tableSelector)
  }

  /**
   * The function to get the relative directory path with the given parent directory.
   * For example, it can return the parent directory as the subdirectory.
   * @param parent the parent directory.
   * @return the relative directory path.
   */
  def relativeDirectory(parent: String): String

  /**
   * The function to generate the report with the given table definition, root directory,
   * hadoopConf, reportConfigs, and the result of the tool.
   * @param tableMeta the table definition.
   * @param rootDirectory the root directory of the report.
   * @param hadoopConf the hadoop configuration.
   * @param reportConfs the report configurations. For example, it can contain the configurations
   *                    for the report enabled, report path, and so on.
   * @param rec the result of the tool.
   */
  def generateReport(
      tableMeta: QualOutputTableDefinition,
      rootDirectory: String,
      hadoopConf: Configuration,
      reportConfs: Map[String, String],
      rec: T): Unit

  /**
   * The function to build the report with the given record, output directory, hadoopConf,
   * and report configurations.
   * @param rec the record to write out to the report.
   * @param outputDir the output directory.
   * @param hadoopConf the hadoop configuration.
   * @param reportOptions the report configurations. For example, it can contain the configurations
   *                      for the report enabled, report path, and so on.
   */
  def build(
      rec: T,
      outputDir: String,
      hadoopConf: Option[Configuration],
      reportOptions: Map[String, String]): Unit = {
    val subDirectory = relativeDirectory(outputDir)
    loadedTableDef.foreach { tDef =>
      generateReport(tDef, subDirectory,
        hadoopConf.getOrElse(QualReportGenConfProvider.hConf), reportOptions, rec)
    }
  }
}
