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

package com.nvidia.spark.rapids.tool.tuning

import com.nvidia.spark.rapids.tool.ToolTextFileWriter
import com.nvidia.spark.rapids.tool.profiling.Profiler
import org.apache.hadoop.conf.Configuration

/**
 * Abstract base class for generating AutoTuner reports.
 *
 * @param tuningResult Result of the tuning process
 * @param outputDir Directory where the report will be written.
 * @param hadoopConf Hadoop configuration
 */
abstract class BaseAutoTunerReportGenerator(
    protected val tuningResult: TuningResult,
    protected val outputDir: String,
    protected val hadoopConf: Configuration) {

  /**
   * Suffix to be used for the configuration file.
   */
  protected def configFileSuffix: String

  /**
   * Generates the recommended properties report using the filteredRecommendations logic.
   */
  def generateRecommendedPropertiesReport(): Unit = {
    val textFileWriter = new ToolTextFileWriter(outputDir,
      s"${tuningResult.appID}-$configFileSuffix.conf",
      s"Required and Tuned configurations to run - ${tuningResult.appID}", Option(hadoopConf))
    try {
      textFileWriter.write(tuningResult.recommendations.map(_.toConfString).reduce(_ + "\n" + _))
    } finally {
      textFileWriter.close()
    }
  }

  /**
   * Generates the recommended properties report with comments.
   */
  def generateRecommendedPropertiesWithCommentsReport(): Unit = {
    val textFileWriter = new ToolTextFileWriter(outputDir,
      s"${tuningResult.appID}.log", s"Tuning App - ${tuningResult.appID}", Option(hadoopConf))
    try {
      textFileWriter.write(
        s"### Recommended SPARK Configuration on GPU Cluster for App: ${tuningResult.appID} ###\n")
      textFileWriter.write(Profiler.getAutoTunerResultsAsString(
        tuningResult.recommendations, tuningResult.comments))
    } finally {
      textFileWriter.close()
    }
  }

  /**
   * Generates the combined configuration report if combinedProps are available.
   */
  def generateCombinedReport(): Unit = {
    tuningResult.combinedProps.collect {
      case combinedProps =>
        val textFileWriter = new ToolTextFileWriter(outputDir,
          s"${tuningResult.appID}.conf",
          s"Combined configurations for App - ${tuningResult.appID}", Option(hadoopConf))
        try {
          textFileWriter.write(combinedProps.map(_.toString).reduce(_ + "\n" + _))
        } finally {
          textFileWriter.close()
        }
    }
  }
}

/**
 * Implementation of BaseAutoTunerReportGenerator for bootstrap (to be used by
 * Qualification tool).
 * Uses bootstrap filtering to include only enabled, bootstrap, non-removed entries.
 * This generates files with app-id prefix for the legacy flat structure.
 */
class BootstrapReportGenerator(
    tuningResult: TuningResult,
    outputDir: String,
    hadoopConf: Configuration)
  extends BaseAutoTunerReportGenerator(tuningResult, outputDir, hadoopConf) {

  override protected def configFileSuffix: String = "bootstrap"
}

/**
 * Implementation for per-app tuning output (NEW structure).
 * Uses simplified file names since files are in per-app directories.
 * Generates:
 *   - bootstrap.conf (instead of <app-id>-bootstrap.conf)
 *   - recommendations.log (instead of <app-id>.log)
 *   - combined.conf (instead of <app-id>.conf)
 *
 * TODO: Once the legacy flat tuning structure (tuning/) is deprecated and removed,
 *   this simplified file naming should be integrated into the base class. Currently,
 *   we override all methods to support the new per-app structure while maintaining
 *   backward compatibility with the old app-id-prefixed naming convention.
 */
class BootstrapReportGeneratorPerApp(
    tuningResult: TuningResult,
    outputDir: String,
    hadoopConf: Configuration)
  extends BaseAutoTunerReportGenerator(tuningResult, outputDir, hadoopConf) {

  override protected def configFileSuffix: String = "bootstrap"

  /**
   * Generates the recommended properties report with simplified file name.
   * Output: bootstrap.conf
   */
  override def generateRecommendedPropertiesReport(): Unit = {
    val textFileWriter = new ToolTextFileWriter(outputDir,
      "bootstrap.conf",  // Simplified name - no app-id prefix
      s"Required and Tuned configurations to run - ${tuningResult.appID}", Option(hadoopConf))
    try {
      textFileWriter.write(tuningResult.recommendations.map(_.toConfString).reduce(_ + "\n" + _))
    } finally {
      textFileWriter.close()
    }
  }

  /**
   * Generates the recommended properties report with comments and simplified file name.
   * Output: recommendations.log
   */
  override def generateRecommendedPropertiesWithCommentsReport(): Unit = {
    val textFileWriter = new ToolTextFileWriter(outputDir,
      "recommendations.log",  // Simplified name - no app-id prefix
      s"Tuning App - ${tuningResult.appID}", Option(hadoopConf))
    try {
      textFileWriter.write(
        s"### Recommended SPARK Configuration on GPU Cluster for App: ${tuningResult.appID} ###\n")
      textFileWriter.write(Profiler.getAutoTunerResultsAsString(
        tuningResult.recommendations, tuningResult.comments))
    } finally {
      textFileWriter.close()
    }
  }

  /**
   * Generates the combined configuration report with simplified file name.
   * Output: combined.conf
   */
  override def generateCombinedReport(): Unit = {
    tuningResult.combinedProps.collect {
      case combinedProps =>
        val textFileWriter = new ToolTextFileWriter(outputDir,
          "combined.conf",  // Simplified name - no app-id prefix
          s"Combined configurations for App - ${tuningResult.appID}", Option(hadoopConf))
        try {
          textFileWriter.write(combinedProps.map(_.toString).reduce(_ + "\n" + _))
        } finally {
          textFileWriter.close()
        }
    }
  }
}
