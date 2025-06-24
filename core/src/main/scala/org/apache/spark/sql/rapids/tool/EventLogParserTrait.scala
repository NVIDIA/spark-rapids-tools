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

package org.apache.spark.sql.rapids.tool

import org.apache.spark.sql.rapids.tool.util.{EventUtils, ToolsMetric}

/**
 * Trait defining common methods, fields and metrics to process eventlog lines.
 * Metrics counts number help us understand the overhead and the work done by the parser.
 */
trait EventLogParserTrait {
  lazy private val linePrefixes =
    EventUtils.getAcceptedLinePrefix(this.getClass.getSimpleName)
  // Map between name and toolsMetrics.
  // Track the number of skipped lines and number of parsed lines in the eventlog.
  private val parserMetrics: Map[String, ToolsMetric] =
    Map(
      "skippedLines" ->
        ToolsMetric("eventLog skippedLines",
          "Number of lines skipped in an eventlog"),
      "parsedLines" ->
        ToolsMetric("eventLog parsedLines",
          "Number of lines parsed in an eventlog"))

  /**
   * Check if the line starts with one of the accepted prefixes
   * @param line the line extracted from the evntlog.
   * @return true if the line starts with one of the accepted prefixes.
   */
  def acceptLine(line: String): Boolean = {
    val result = linePrefixes.isEmpty || linePrefixes.exists(line.startsWith)
    if (!result) {
      parserMetrics("skippedLines").inc()
    } else {
      parserMetrics("parsedLines").inc()
    }
    result
  }

  // Define getters for metrics used to gather statistics
  def getTotalParsedLines: Long = parserMetrics.values.map(_.getValue).sum
  def getProcessedLinesCount: Long = parserMetrics("parsedLines").getValue
  def getSkippedLinesCount: Long = parserMetrics("skippedLines").getValue
}
