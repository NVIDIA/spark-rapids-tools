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

package com.nvidia.spark.rapids.tool.planparser

import scala.util.matching.Regex

import com.nvidia.spark.rapids.tool.planparser.ops.{OpTypes, UnsupportedExprOpRef}
import com.nvidia.spark.rapids.tool.qualification.PluginTypeChecker

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.AppBase
import org.apache.spark.sql.rapids.tool.plangraph.SparkPlanGraphNode

/**
 * Base parser for data source scan execution nodes (e.g., FileSourceScanExec, BatchScan).
 *
 * This class provides common functionality for parsing and analyzing scan operations that read
 * data from external sources such as Parquet, ORC, JSON, CSV, Delta Lake, Iceberg, etc.
 * It handles both Hive table scans and regular file-based scans.
 *
 * The parser performs several key functions:
 * 1. Extracts metadata about the data source (format, location, schema, filters)
 * 2. Calculates GPU speedup potential based on the file format and schema data types
 * 3. Determines if the scan operation is supported on GPU
 * 4. Reports scan operations in a normalized format for analysis
 *
 * Subclasses should override specific methods to handle different scan node types
 * (e.g., FileSourceScanExec, BatchScan, DataSourceV2ScanExec).
 *
 * @param node the SparkPlanGraphNode representing the scan operation
 * @param checker plugin type checker for determining GPU support and speedup factors
 * @param sqlID the SQL query ID this scan belongs to
 * @param app optional AppBase containing application context
 * @param execName optional override for the execution node name
 */
class BaseSourceScanExecParser(
  override val node: SparkPlanGraphNode,
  override val checker: PluginTypeChecker,
  override val sqlID: Long,
  override val app: Option[AppBase],
  override val execName: Option[String] = None
) extends GenericExecParser(
  node = node,
  checker = checker,
  sqlID = sqlID,
  execName = execName,
  app = app
) with Logging {
  /**
   * Cached speedup factor calculated for this scan operation.
   * This value is computed based on the file format and schema compatibility with GPU execution.
   * It starts at 1.0 (no speedup) and is updated when pullSupportedFlag() is called.
   * A value greater than 1.0 indicates potential GPU acceleration.
   */
  protected var calculatedSpeedup: Double = 1.0

  /**
   * Flag indicating whether this is a Hive table scan.
   * Hive table scans are also FileSourceScanExec nodes, but we want to
   * report them differently.
   * We detect them using the HiveParseHelper.
   */
  protected val isHiveScan: Boolean = HiveParseHelper.isHiveTableScanNode(node)

  /**
   * Regular expression to extract the first word from a node name.
   * Matches the first alphanumeric characters of a string after trimming leading/trailing
   * white spaces. Used to extract the scan type (e.g., "Scan", "BatchScan", "FileSourceScan").
   * Pattern: `^\s*(\w+).*`
   */
  protected val nodeNameRegeX: Regex = """^\s*(\w+).*""".r

  /**
   * The token representing the scan node type extracted from the trimmed node name.
   * This is used as part of the reported exec name to identify the type of scan operation.
   * Examples: "Scan", "BatchScan", "FileSourceScan"
   */
  protected lazy val scanNodeToken: String = trimmedNodeName

  /**
   * Record containing the parsed metadata information from the read node.
   * This includes:
   * - schema: the read schema
   * - location: the data source location (file path, table name, etc.)
   * - format: the data format (parquet, orc, json, csv, etc.)
   * - tags: additional metadata tags (filters, pushed filters, partition filters)
   *
   * The metadata is extracted differently for Hive scans vs. regular scans.
   */
  protected lazy val readInfo: ReadMetaData = if (isHiveScan) {
    HiveParseHelper.parseReadNode(node)
  } else {
    ReadParser.parseReadNode(node)
  }

  /**
   * The normalized format string extracted from readInfo in lowercase.
   * This ensures consistent format comparison regardless of case variations in the event log.
   * Examples: "parquet", "orc", "json", "csv", "delta", "iceberg"
   */
  protected lazy val readFormat: String = readInfo.getReadFormatLC

  /**
   * Check if the scan operation is supported.
   * Delta Lake metadata table scans are not supported.
   * @return true if supported, false otherwise
   */
  protected def isScanOpSupported: Boolean = true

  /**
   * Determine the operation type for the scan.
   * If it is a delta lake metadata table scan, then return ReadDeltaLog.
   * Otherwise, return ReadExec.
   * @return the operation type
   */
  protected def pullOpType: OpTypes.Value = OpTypes.ReadExec

  /**
   * Parse a non-RDD scan operation and generate ExecInfo.
   * This method performs the complete analysis of a scan operation:
   * 1. Computes the execution duration from metrics
   * 2. Parses expressions used in the scan (filters, projections)
   * 3. Identifies unsupported expressions that cannot run on GPU
   * 4. Determines if the entire scan is GPU-supported
   * 5. Creates and returns an ExecInfo object with all the collected information
   *
   * The scan is considered supported if both:
   * - The pullSupportedFlag() returns true (format and schema are GPU-compatible)
   * - There are no unsupported expressions
   *
   * @return ExecInfo containing analysis results and GPU compatibility information
   */
  protected def parseNonRDDScan: ExecInfo = {
    val duration = computeDuration
    val expressions = parseExpressions()
    val notSupportedExprs = getNotSupportedExprs(expressions)
    val isExecSupported = pullSupportedFlag() && notSupportedExprs.isEmpty
    createExecInfo(
      calculatedSpeedup,
      isExecSupported,
      duration,
      notSupportedExprs = notSupportedExprs,
      expressions = expressions)
  }

  /**
   * Override the reported execution name to include the data format.
   * The reported exec name follows the pattern: "<ScanType> <format>"
   * This makes it easier to identify scan operations by their data format in execution reports.
   *
   * Examples:
   * - "Scan parquet"
   * - "BatchScan orc"
   * - "FileSourceScan json"
   *
   * @return the formatted execution name for reporting
   */
  override def reportedExecName: String = s"$scanNodeToken $readFormat"

  /**
   * Calculate the speedup factor for the scan operation.
   *
   * The speedup factor is calculated as:
   * speedupFactor = checker.getSpeedupFactor(fullExecName) * readScoreRatio
   *
   * Where:
   * - checker.getSpeedupFactor(fullExecName): base speedup for the scan operation type
   * - readScoreRatio: calculated from the file format and schema data type compatibility
   *
   * The read score ratio is computed by ReadParser.calculateReadScoreRatio which:
   * 1. Checks if the file format is GPU-supported
   * 2. Analyzes the schema to determine the ratio of GPU-supported data types
   * 3. Returns a score between 0.0 and 1.0
   *
   * The final speedup factor is clamped to a minimum of 1.0 (no slowdown).
   *
   * @param registeredName optional registered name to check
   * @return the calculated speedup factor (minimum 1.0)
   */
  override def pullSpeedupFactor(registeredName: Option[String] = None): Double = {
    val speedupFactor = checker.getSpeedupFactor(fullExecName)
    // don't use the isExecSupported because we have finer grain.
    val score = ReadParser.calculateReadScoreRatio(readInfo, checker)
    Math.max(speedupFactor * score, 1.0)
  }

  /**
   * Determine if the scan operation is supported on GPU.
   *
   * This method performs a comprehensive check of GPU support by:
   * 1. Verifying the scan operation type is supported (e.g., not a Delta Lake metadata scan)
   * 2. Checking if the operation is registered as GPU-supported in the checker
   * 3. Calculating the speedup factor based on format and schema compatibility
   * 4. Determining support based on whether speedup > 1.0
   *
   * The scan is considered GPU-supported if ALL of the following are true:
   * - isScanOpSupported returns true (operation-specific checks pass)
   * - checker.isExecSupported() returns true (operation type is GPU-supported)
   * - calculatedSpeedup > 1.0 (file format and schema are GPU-compatible)
   *
   * If any check fails, the method returns false and calculatedSpeedup remains 1.0.
   * As a side effect, this method updates the calculatedSpeedup field for later use.
   *
   * @param registeredName optional registered name to check for GPU support
   * @return true if the scan operation can benefit from GPU acceleration, false otherwise
   */
  override def pullSupportedFlag(registeredName: Option[String] = None): Boolean = {
    if (checker.isExecSupported(registeredName.getOrElse(fullExecName)) && isScanOpSupported) {
      calculatedSpeedup = pullSpeedupFactor()
      // If calculated speedup factor is GT 1.0, then the format and the schema run on GPU.
      // Otherwise, it returns false.
      return calculatedSpeedup > 1.0
    }
    false
  }

  override def reportedExpr: String = {
    if (readInfo.hasUnknownFormat) {
      node.desc
    } else {
      s"Format: $readFormat"
    }
  }
  /**
   * Create an ExecInfo object with scan-specific information.
   *
   * This method constructs the execution information object that will be reported in the
   * qualification/profiling output. It customizes the expression field based on the scan type:
   * - If the format is unknown, uses the full node description
   * - Otherwise, uses a formatted string "Format: <format>" for clarity
   *
   * The created ExecInfo includes:
   * - sqlID: the SQL query ID this scan belongs to
   * - exec: the reported execution name (e.g., "Scan parquet")
   * - expr: the expression/format description
   * - speedupFactor: calculated GPU speedup potential
   * - duration: execution time from metrics
   * - nodeId: graph node identifier
   * - opType: operation type (usually ReadExec or ReadDeltaLog)
   * - isSupported: whether GPU acceleration is supported
   * - children: child execution nodes
   * - unsupportedExecReason: reason for lack of GPU support (if any)
   * - expressions: array of parsed expression strings
   *
   * @param speedupFactor the calculated GPU speedup factor
   * @param isSupported whether the scan is GPU-supported
   * @param duration optional execution duration in milliseconds
   * @param notSupportedExprs sequence of unsupported expressions found
   * @param expressions array of all parsed expression strings
   * @return ExecInfo object containing all scan analysis results
   */
  override protected def createExecInfo(
    speedupFactor: Double,
    isSupported: Boolean,
    duration: Option[Long],
    notSupportedExprs: Seq[UnsupportedExprOpRef],
    expressions: Array[String]
  ): ExecInfo = {
    ExecInfo.createExecNoNode(
      sqlID = sqlID,
      exec = reportedExecName,
      expr = reportedExpr,
      speedupFactor = speedupFactor,
      duration = duration,
      nodeId = node.id,
      opType = pullOpType,
      isSupported = isSupported,
      children = getChildren,
      unsupportedExecReason = unsupportedReason,
      expressions = expressions)
  }
}
