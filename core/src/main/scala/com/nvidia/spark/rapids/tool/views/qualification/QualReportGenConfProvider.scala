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

import java.nio.file.Paths

import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.rapids.tool.util.RapidsToolsConfUtil

/**
 * This file contains the configuration provider for the qualification report
 * generation.
 *
 * It provides the following configuration:
 * — `TABLES_CONFIG_PATH`: The path to the configuration file that contains
 *   the definitions of the tables and the columns that should be included in
 *   the report.
 * — `PER_APP_SUBDIRECTORY`: The subdirectory where the per-application report
 *   should be generated.
 * — `GLOBAL_SUBDIRECTORY`: The subdirectory where the global report should be
 *   generated.
 * — `TUNING_SUBDIRECTORY`: The subdirectory where the tuning report should be
 *   generated.
 * — `CSV_DELIMITER`: The delimiter that should be used when writing the CSV
 *   tables.
 *
 * It also provides methods to load the configuration from the yaml file and
 * to generate the paths for the different types of reports.
 */
object QualReportGenConfProvider {
  // TODO: Move those configurations to yaml file.
  val TABLES_CONFIG_PATH = "configs/qualOutputTable.yaml"
  val PER_APP_SUBDIRECTORY = "qual_metrics"
  val GLOBAL_SUBDIRECTORY = "qual_core_output"
  val TUNING_SUBDIRECTORY = "tuning"
  val CSV_DELIMITER = ","

  // Initializes hadoop-conf object if it was not provided by the context.
  lazy val hConf: Configuration = RapidsToolsConfUtil.newHadoopConf()

  /**
   * Load the table definitions from the yaml file and filter them based on the provided predicate.
   * @param p the predicate to select the table definition. For example, it can be by label, or
   *          by-scope.
   * @return the filtered table definitions
   */
  def loadTableReports(
    p: QualOutputTableDefinition => Boolean): Seq[QualOutputTableDefinition] = {
    val tableConfigs = QualYamlConfigLoader.loadConfig(TABLES_CONFIG_PATH)
    tableConfigs.qualTableDefinitions.filter(p)
  }

  /**
   * Get the path to the global report directory.
   * @param outputPath the parent directory where the output should be generated.
   * @return the path to the global report directory as the subdirectory concatenated to the parent.
   */
  def getGlobalReportPath(outputPath: String): String = {
    Paths.get(outputPath, GLOBAL_SUBDIRECTORY).toString
  }

  /**
   * Get the path to the per-application report directory.
   * @param outputPath the parent directory where the output should be generated.
   * @return the path to the per-application report directory as the subdirectory concatenated to
   *         the parent.
   */
  def getPerAppReportPath(outputPath: String): String = {
    Paths.get(getGlobalReportPath(outputPath), PER_APP_SUBDIRECTORY).toString
  }

  /**
   * Get the path to the tuning report directory.
   * @param outputPath the parent directory where the output should be generated.
   * @return the path to the tuning report directory as the subdirectory concatenated to the parent.
   */
  def getTuningReportPath(outputPath: String): String = {
    Paths.get(getGlobalReportPath(outputPath), TUNING_SUBDIRECTORY).toString
  }
}
