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
import com.nvidia.spark.rapids.tool.qualification.QualToolResult
import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.rapids.tool.ToolUtils

/**
 * This file contains the code for writing out the tool's output to CSV files.
 * Each type of output is written out to a different CSV file.
 *
 * The tables are defined in `QualOutputTableDefinition` and the global ones are
 * defined in this file. The ones per app are defined in `QualPerAppReportGenerator`.
 *
 * The output is written out to a directory specified by the user. The
 * directory structure is as follows:
 * rootDirectory/[globalSubDirectory]/tableName.csv
 *
 * The root directory is specified by the user and the appId is the name of
 * the application. The tableName is the name of the `QualOutputTableDefinition`
 * which is used to look up the table name to write the output to.
 *
 * The output is written out in the following order:
 *   1. The global tables
 *   2. The per app tables
 *
 * The global tables are written out once the qualification tools finishes the entire analysis
 * and the per app tables are written out once per application.
 */

/**
 * A trait that defines how to write out the tool's output to a CSV file.
 * The tables are defined in `QualOutputTableDefinition`.
 *
 * The output is written out to a directory specified by the user. The
 * directory structure is as follows:
 * rootDirectory/[globalSubDirectory]/tableName.csv
 *
 * The root directory is specified by the user and the appId is the name of
 * the application. The tableName is the name of the `QualOutputTableDefinition`
 * which is used to look up the table name to write the output to.
 */
trait QualToolTableTrait extends QualTableBuilderTrait[QualToolResult] {

}


/**
 * Abstract class for generating the Qualification report tables.
 *
 * @param tableMeta table definition.
 * @param rootDirectory root directory of the report.
 * @param hadoopConf Hadoop configuration.
 */
abstract class QualToolTable(
    tableMeta: QualOutputTableDefinition,
    override val rootDirectory: String,
    hadoopConf: Configuration) extends QualToolTableTrait {
  /**
   * The label for the table.
   */
  def getLabel: String = tableMeta.label

  /**
   * The description for the table.
   */
  def getDescription: String = tableMeta.description.getOrElse("")

  /**
   * The file name for the table.
   */
  def getFileName: String = tableMeta.fileName

  /**
   * Writes out the header for the table.
   */
  def writeCSVHeader(fWriter: ToolTextFileWriter): Unit = {
    fWriter.writeLn(tableMeta.columns.map(_.name).mkString(delim))
  }

  /**
   * Generates the output report for the table.
   */
  override def generateOutputReport(qualSumInfo: QualToolResult): Unit = {
    val csvFileWriter = {
      // the table is written out to the global subdirectory
      new ToolTextFileWriter(getDirectory(() => QualReportGenConfProvider.GLOBAL_SUBDIRECTORY),
        getFileName,
        getDescription,
        Option(hadoopConf))
    }
    try {
      // write the header
      writeCSVHeader(csvFileWriter)
      appendDataToWriter(csvFileWriter, qualSumInfo)
    } finally {
      csvFileWriter.close()
    }
  }
}

// Generate the Qualification status table
class QualToolStatusTable(
    tableMeta: QualOutputTableDefinition,
    override val rootDirectory: String,
    hadoopConf: Configuration) extends QualToolTable(tableMeta, rootDirectory, hadoopConf) {
  override def appendDataToWriter(fWriter: ToolTextFileWriter, rec: QualToolResult): Unit = {
    rec.appStatus.foreach { r =>
      fWriter.writeLn(
        Seq(formatStr(r.path),
          formatStr(r.status),
          formatStr(r.appId),
          formatStr(r.message)
        ).mkString(delim))
    }
  }
}

// Generate the Qualification summary table
class QualToolSummaryTable(
    tableMeta: QualOutputTableDefinition,
    override val rootDirectory: String,
    hadoopConf: Configuration) extends QualToolTable(tableMeta, rootDirectory, hadoopConf) {
  override def appendDataToWriter(fWriter: ToolTextFileWriter, rec: QualToolResult): Unit = {
    rec.appSummaries.foreach { r =>
      fWriter.writeLn(Seq(formatStr(r.appName),
        r.appId,
        r.estimatedInfo.attemptId,
        r.estimatedInfo.appDur,
        ToolUtils.truncateDoubleToTwoDecimal(r.executorCpuTimePercent),
        formatStr(ToolUtils.renderTextField(r.failedSQLIds, ",", delim)),
        formatStr(
          ToolUtils.renderTextField(r.readFileFormatAndTypesNotSupported, ";", delim)),
        formatStr(
          ToolUtils.renderTextField(r.writeDataFormat, ";", delim)).toUpperCase,
        formatStr(ToolUtils.formatComplexTypes(r.complexTypes)),
        formatStr(ToolUtils.formatComplexTypes(r.nestedComplexTypes)),
        formatStr(ToolUtils.formatPotentialProblems(r.potentialProblems)),
        r.longestSqlDuration,
        r.sqlStageDurationsSum,
        r.endDurationEstimated,
        r.totalCoreSec).mkString(delim))
    }
  }
}

/**
 * The object that generates the global report for the tool.
 *
 * tableSelector the predicate to select the table definitions. For example, it can be by label, or
 *               by-scope.
 * generateReport the function to generate the report with the given table definition,
 *               root directory, hadoopConf, reportConfigs, and the result of the tool.
 * relativeDirectory the function to get the relative directory path with the given parent
 *               directory. For example, it can return the parent directory as the subdirectory.
 */
object QualToolReportGenerator extends QualReportGeneratorTrait[QualToolResult] {
  override val tableSelector: QualOutputTableDefinition => Boolean = { tDef =>
    tDef.isGlobal
  }

  /**
   * Generate the report with the given table definition, root directory, hadoopConf,
   * reportConfigs, and the result of the tool.
   *
   * @param tableMeta the table definition.
   * @param rootDirectory the root directory of the report.
   * @param hadoopConf the hadoop configuration.
   * @param reportConfigs the report configurations. For example, it can contain the configurations
   *                      for the report enabled, report path, and so on.
   * @param qualToolResult the result of the tool.
   */
  override def generateReport(
      tableMeta: QualOutputTableDefinition,
      rootDirectory: String,
      hadoopConf: Configuration,
      reportConfigs: Map[String, String],
      qualToolResult: QualToolResult): Unit = {
    val table = tableMeta.label match {
      case "qualCoreCSVStatus" => new QualToolStatusTable(tableMeta, rootDirectory, hadoopConf)
      case "qualCoreCSVSummary" => new QualToolSummaryTable(tableMeta, rootDirectory, hadoopConf)
      case _ =>
        throw new IllegalArgumentException(
          s"Unknown table label ${tableMeta.label} for table ${tableMeta.description}")
    }
    table.withReportConfigs(reportConfigs, tableMeta.label).generateOutputReport(qualToolResult)
  }

  /**
   * Get the relative directory path with the given parent directory.
   *
   * @param parent the parent directory.
   * @return the relative directory path.
   */
  override def relativeDirectory(parent: String): String = {
    parent
  }
}
