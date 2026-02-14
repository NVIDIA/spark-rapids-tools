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
package com.nvidia.spark.rapids.tool.planparser

import java.util.regex.Pattern

/**
 * Utility for extracting information from Spark's physicalPlanDescription string.
 *
 * == Why name-based matching ==
 *
 * physicalPlanDescription uses Spark's internal operator IDs (assigned bottom-up during query
 * execution), which differ from ToolsPlanGraph sequential IDs (assigned top-down starting at 0).
 * These two numbering systems do not correspond to each other. This utility matches nodes by
 * name rather than ID, avoiding mismatches between the two systems.
 *
 * == Applicability ==
 *
 * This utility is designed for operators that appear '''at most once''' per SQL plan, such as:
 *  - Iceberg: ReplaceData, WriteDelta, MergeRows (one per MERGE INTO statement)
 *  - Delta Lake: SaveIntoDataSourceCommand, DeleteCommand, UpdateCommand,
 *    MergeIntoCommand, OptimizeTableCommand (one per SQL statement)
 *
 * It is '''not safe''' for operators that appear multiple times in a single plan (e.g., Project,
 * Filter, HashAggregate). For those, name-based matching with `occurrence=0` would silently
 * return the wrong node's data. Disambiguating those would require correlating Spark's internal
 * IDs with ToolsPlanGraph IDs (e.g., via SparkPlanInfo)
 *
 * == physicalPlanDescription format ==
 * {{{
 *   (12) ReplaceData
 *   Input [16]: [_c0#523, _c1#524, ...]
 *   Arguments: IcebergWrite(table=spark_catalog.default.my_table, format=PARQUET)
 *
 *   (11) Project
 *   Output [16]: [_c0#494, ...]
 *   Input [36]: [_c0#478, ...]
 * }}}
 */
object PhysicalPlanDescHelper {

  /**
   * Extracts the Arguments value for a node matched by name.
   *
   * Searches for `(\d+) <nodeName>` followed by an `Arguments:` line in the same node section,
   * and returns all content from `Arguments:` to the end of the node section.
   *
   * @param physPlanDesc the full physicalPlanDescription string
   * @param nodeName     the node name to match (e.g., "ReplaceData", "WriteDelta", "MergeRows").
   *                     Must be unique per plan, or use `occurrence` to disambiguate.
   * @param occurrence   0-based occurrence index when multiple same-named nodes exist (default: 0)
   * @return the Arguments string if found, None otherwise
   */
  def extractArgumentsForNode(
      physPlanDesc: String,
      nodeName: String,
      occurrence: Int = 0): Option[String] = {
    if (occurrence < 0) {
      return None
    }
    // First extract the node's section (everything between this node header and the next),
    // then look for Arguments: within that bounded section.
    val sections = extractNodeSections(physPlanDesc, nodeName)
    sections.lift(occurrence).flatMap { section =>
      argumentsPattern.findFirstMatchIn(section).map(_.group(1).trim)
    }
  }

  // Regex to extract Arguments content within a node section
  private val argumentsPattern = """(?s)Arguments:\s*(.+)""".r

  /**
   * Extracts all sections for nodes matching the given name.
   * Each section is the text from the node header up to (but not including) the next node header.
   */
  private def extractNodeSections(physPlanDesc: String, nodeName: String): Seq[String] = {
    val quotedName = Pattern.quote(nodeName)
    // Match the node header: (<id>) <nodeName>
    val headerPattern = s"""(?m)^\\(\\d+\\)\\s+$quotedName\\s*$$""".r
    // Matches any node header line: "(<id>) <anything>".
    // Operator names can contain spaces (e.g., "SortMergeJoin FullOuter"), so we match the
    // entire line rather than a single token.
    val nodeHeaderPattern = """(?m)^\(\d+\)\s+.*$""".r

    headerPattern.findAllMatchIn(physPlanDesc).map { m =>
      val startOfContent = m.end
      // Find the next node header after this one
      val remaining = physPlanDesc.substring(startOfContent)
      nodeHeaderPattern.findFirstMatchIn(remaining) match {
        case Some(nextHeader) => remaining.substring(0, nextHeader.start)
        case None => remaining
      }
    }.toSeq
  }
}
