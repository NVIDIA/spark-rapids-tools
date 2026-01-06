/*
 * Copyright (c) 2022-2025, NVIDIA CORPORATION.
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

import scala.collection.mutable.HashMap

import com.nvidia.spark.rapids.tool.planparser.hive.HiveParseHelper
import com.nvidia.spark.rapids.tool.qualification.PluginTypeChecker

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.plangraph.SparkPlanGraphNode
import org.apache.spark.sql.rapids.tool.util.StringUtils

case class ReadMetaData(schema: String, location: String, format: String,
    tags: Map[String, String] = ReadParser.DEFAULT_METAFIELD_MAP) {
  // Define properties to access the tags

  /**
   * Returns the combined pushed filters and runtime filters for this scan operation.
   *
   * This property provides a comprehensive view of all filters applied to the data scan:
   * - **Pushed Filters**: Static filters that are pushed down to the data source at plan time.
   *   These filters are applied before data is read to reduce I/O.
   *   Examples: IsNotNull(column), EqualTo(column, value), GreaterThan(column, value)
   *
   * - **Runtime Filters**: Dynamic filters generated during query execution based on runtime
   *   information (e.g., from broadcast joins, dynamic partition pruning). These are not
   *   known at plan time but can significantly reduce data scanned.
   *   Examples: dynamicpruningexpression(column IN subquery)
   *
   * The format of the returned string depends on whether runtime filters are present:
   * - If no runtime filters: Returns only pushed filters
   *   Example: "IsNotNull(id), GreaterThan(id, 100)"
   *
   * - If runtime filters exist: Returns combined format
   *   Example: "filters=[IsNotNull(id)],runtimeFilters=[id#1 IN dynamicpruning#2]"
   *
   * This information is valuable for understanding:
   * - What data filtering is occurring at the scan level
   * - Whether dynamic optimizations like partition pruning are being applied
   * - The effectiveness of filter pushdown for GPU acceleration analysis
   *
   * @return Combined string of pushed filters and runtime filters, or UNKNOWN if not available
   * @see ReadParser.combinePushedFilters for the combination logic
   */
  def pushedFilters: String = {
    ReadParser.combinePushedFilters(tags)
  }

  /**
   * Returns the data filters applied to this scan operation.
   *
   * @return Data filters string, or UNKNOWN if not available
   */
  def dataFilters: String = tags(ReadParser.METAFIELD_TAG_DATA_FILTERS)

  /**
   * Returns the partition filters applied to this scan operation.
   *
   * @return Partition filters string, or UNKNOWN if not available
   */
  def partitionFilters: String = tags(ReadParser.METAFIELD_TAG_PARTITION_FILTERS)

  def hasUnknownFormat: Boolean = format.equals(ReadParser.UNKNOWN_METAFIELD)

  def hasUnknowLocation: Boolean = location.equals(ReadParser.UNKNOWN_METAFIELD)

  /**
   * Returns the read format in lowercase. This is used to be consistent.
   * @return the lower case of the read format
   */
  def getReadFormatLC: String = format.toLowerCase
}

object ReadParser extends Logging {
  // It was found that some eventlogs could have "NativeScan" instead of "Scan"
  val SCAN_NODE_PREFIXES = Seq("Scan", "NativeScan")
  // Do not include OneRowRelation in the scan nodes, consider it as regular Exec
  val SCAN_ONE_ROW_RELATION = "Scan OneRowRelation"
  // DatasourceV2 node names that match partially on the following labels
  val DATASOURCE_V2_NODE_PREF = Set(
    "GpuScan",
    // The following entry covers both GpuBatchScan and BatchScan
    "BatchScan",
    "JDBCRelation")

  val METAFIELD_TAG_DATA_FILTERS = "DataFilters"
  val METAFIELD_TAG_PUSHED_FILTERS = "PushedFilters"
  val METAFIELD_TAG_PARTITION_FILTERS = "PartitionFilters"
  val METAFIELD_TAG_RUNTIME_FILTERS = "RuntimeFilters"
  val METAFIELD_TAG_READ_SCHEMA = "ReadSchema"
  val METAFIELD_TAG_FORMAT = "Format"
  val METAFIELD_TAG_LOCATION = "Location"

  val UNKNOWN_METAFIELD: String = StringUtils.UNKNOWN_EXTRACT
  val DEFAULT_METAFIELD_MAP: Map[String, String] = collection.immutable.Map(
    METAFIELD_TAG_DATA_FILTERS -> UNKNOWN_METAFIELD,
    METAFIELD_TAG_PUSHED_FILTERS -> UNKNOWN_METAFIELD,
    METAFIELD_TAG_PARTITION_FILTERS -> UNKNOWN_METAFIELD,
    METAFIELD_TAG_RUNTIME_FILTERS -> UNKNOWN_METAFIELD
  )

  def isScanNode(nodeName: String): Boolean = {
    SCAN_NODE_PREFIXES.exists(nodeName.startsWith(_)) && !nodeName.startsWith(SCAN_ONE_ROW_RELATION)
  }

  def isScanNode(node: SparkPlanGraphNode): Boolean = {
    isScanNode(node.name)
  }

  def isDataSourceV2Node(node: SparkPlanGraphNode): Boolean = {
    DATASOURCE_V2_NODE_PREF.exists(node.name.contains(_))
  }

  // strip off the struct<> part that Spark adds to the ReadSchema
  def formatSchemaStr(schema: String): String = {
    schema.stripPrefix("struct<").stripSuffix(">")
  }

  // This tries to get just the field specified by tag in a string that
  // may contain multiple fields. It looks for a comma to delimit fields.
  private def getFieldWithoutTag(str: String, tag: String): String = {
    val index = str.indexOf(tag)
    // remove the tag from the final string returned
    val subStr = str.substring(index + tag.size)
    val commaIndex = subStr.indexOf(", ")
    // In some operators like BatchScan, the schema is followed by " RuntimeFilters: [" instead of
    // ", ". In that case, we need to stop at that index.
    val runtimeFilterIndex = subStr.indexOf(" RuntimeFilters: [")

    val endIndex = if (commaIndex == -1) {
      runtimeFilterIndex
    } else if (runtimeFilterIndex == -1) {
      commaIndex
    } else {
      Math.min(commaIndex, runtimeFilterIndex)
    }

    // InMemoryFileIndex[hdfs://bdbl-rpm-1160
    if (endIndex != -1) {
      subStr.substring(0, endIndex)
    } else {
      subStr
    }
  }

  // Used to extract metadata fields from Spark GraphNode’s description.
  // It is made public for testing purposes. It returns DEFAULT_METAFIELD_MAP if no tags exist.
  // Note that this method returns an empty string when the tag has an empty list:
  //      (i.e., TagName: []). This is intentional to distinguish when the tag is missing Vs. when
  //      it is empty.
  def extractReadTags(value: String): Map[String, String] = {
    // initialize the results to the default values
    var result = Map[String, String]() ++ DEFAULT_METAFIELD_MAP
    // For filter tags in metadata they are in the form of:
    // - empty form: "TagName: []"
    // - complete form:  "TagName: [foo(arg1, arg2, argn), bar(), field00, fieldn]".
    // - truncated form ends by ellipsis that can start at any position:
    //   "TagName: [foo(arg1, arg2, argn), bar(), field00, fieldn...".
    // To parse the filter tags: for each meta tag, create a regx that matches the value of the tag
    // until the first closing bracket or the first ellipsis.
    // The regex uses (.*?) for non-greedy match which handles empty brackets correctly.
    val metaFieldRegexMap = result.map { case (k, _) =>
      (k, s"($k): \\[(.*?)(?:(\\.\\.\\.)|(]))".r)
    }
    metaFieldRegexMap.foreach { case (k, v) =>
      v.findFirstMatchIn(value).foreach { m =>
        // group(2) contains the content between brackets
        // group(3) contains "..." if truncated, group(4) contains "]" if complete
        val ellipse = if (m.group(3) != null) "..." else ""
        result += (k -> s"${m.group(2) + ellipse}")
      }
    }
    result
  }

  /**
   * Retrieves the tag from SparkPlanInfo.metadata and removes the opening and closing brackets to
   * ensure consistency with the V2 ReadParser.
   * @param tag key (example: PushedFilters, DataFilters, and PartitionFilters)
   * @param metadata SparkPlanInfo.metadata
   * @return the value extracted from the metadata map or "unknown: if not found.
   */
  def extractTagFromV1ReadMeta(tag: String, metadata: Map[String, String]): String = {
    tag match {
      case METAFIELD_TAG_DATA_FILTERS | METAFIELD_TAG_PUSHED_FILTERS |
           METAFIELD_TAG_PARTITION_FILTERS =>
        metadata.getOrElse(tag, UNKNOWN_METAFIELD).stripPrefix("[").stripSuffix("]")
      case _ => metadata.getOrElse(tag, UNKNOWN_METAFIELD)
    }
  }

  /**
   * Finalizes the data format string by adding "gpu" prefix for GPU execution nodes.
   *
   * This method ensures that data formats are properly labeled when they are processed
   * by GPU-accelerated scan operations. The format normalization is important for:
   * - Distinguishing between CPU and GPU execution in reports
   * - Accurate speedup calculations and GPU support analysis
   * - Consistent format naming across different scan node types
   *
   * The method applies the following logic:
   * 1. If the node name starts with "Gpu" (e.g., "GpuScan", "GpuBatchScan")
   *    AND the format doesn't already start with "gpu":
   *    - Prepends "gpu" to the format
   *    - Example: "parquet" becomes "gpuparquet"
   *
   * 2. Otherwise:
   *    - Returns the format unchanged
   *    - Handles cases where format is already prefixed (e.g., "gpuparquet")
   *    - Handles CPU scan nodes (e.g., "Scan", "BatchScan")
   *
   * Example transformations:
   * - GPU node with "parquet" → "gpuparquet"
   * - GPU node with "gpuparquet" → "gpuparquet" (no change)
   * - CPU node with "parquet" → "parquet" (no change)
   * - GPU node with "orc" → "gpuorc"
   *
   * @param format the original data format string (e.g., "parquet", "orc", "json")
   * @param node the SparkPlanGraphNode being processed
   * @return the finalized format string with "gpu" prefix if appropriate
   */
  def finalizeFormat(format: String, node: SparkPlanGraphNode): String = {
    if (node.name.startsWith("Gpu") && !format.startsWith("gpu")) {
      s"gpu$format"
    } else {
      format
    }
  }

  def parseReadNode(node: SparkPlanGraphNode): ReadMetaData = {
    if (HiveParseHelper.isHiveTableScanNode(node)) {
      HiveParseHelper.parseReadNode(node)
    } else {
      val schemaTag = "ReadSchema: "
      val schema = if (node.desc.contains(schemaTag)) {
        formatSchemaStr(getFieldWithoutTag(node.desc, schemaTag))
      } else {
        ""
      }
      val locationTag = "Location: "
      val location = if (node.desc.contains(locationTag)) {
        val stringWithBrackets = getFieldWithoutTag(node.desc, locationTag)
        // Remove prepended InMemoryFileIndex[  or  PreparedDeltaFileIndex[
        // and return only location
        // Ex: InMemoryFileIndex[hdfs://bdbl-rpm-1106-57451/numbers.parquet,
        // PreparedDeltaFileIndex[file:/tmp/deltatable/delta-table1]
        if (stringWithBrackets.contains("[")) {
          stringWithBrackets.split("\\[", 2).last.replace("]", "")
        } else {
          stringWithBrackets
        }
      } else if (node.name.contains("JDBCRelation")) {
        // see if we can report table or query
        val JDBCPattern = raw".*JDBCRelation\((.*)\).*".r
        node.name match {
          case JDBCPattern(tableName) => tableName
          case _ => UNKNOWN_METAFIELD
        }
      } else {
        UNKNOWN_METAFIELD
      }
      val formatTag = "Format: "
      val fileFormat = if (node.desc.contains(formatTag)) {
        val format = getFieldWithoutTag(node.desc, formatTag)
        finalizeFormat(format, node)
      } else if (node.name.contains("JDBCRelation")) {
        "JDBC"
      } else {
        UNKNOWN_METAFIELD
      }
      ReadMetaData(schema, location, fileFormat, tags = extractReadTags(node.desc))
    }
  }

  // For the read score we look at the read format and datatypes for each
  // format and for each read give it a value 0.0 - 1.0 depending on whether
  // the format is supported and if the data types are supported. So if none
  // of the data types are supported, the score would be 0.0 and if the format
  // and datatypes are supported the score would be 1.0.
  def calculateReadScoreRatio(meta: ReadMetaData,
      pluginTypeChecker: PluginTypeChecker): Double = {
    val notSupportFormatAndTypes: HashMap[String, Set[String]] = HashMap[String, Set[String]]()
    val (readScore, nsTypes) = pluginTypeChecker.scoreReadDataTypes(meta.format, meta.schema)
    if (nsTypes.nonEmpty) {
      val currentFormat = notSupportFormatAndTypes.get(meta.format).getOrElse(Set.empty[String])
      notSupportFormatAndTypes(meta.format) = (currentFormat ++ nsTypes)
    }
    // TODO - not doing anything with note supported types right now
    readScore
  }

  /**
   * Combines pushed filters and runtime filters into a single formatted string.
   *
   * This method merges static pushed filters with dynamic runtime filters to provide
   * a comprehensive view of all filters applied to a scan operation. The combination
   * follows this logic:
   *
   * 1. If no RuntimeFilters tag exists in the tags map:
   *    - Returns only the PushedFilters value
   *
   * 2. If RuntimeFilters tag exists but has UNKNOWN value:
   *    - Returns only the PushedFilters value (runtime filters are not available)
   *
   * 3. If RuntimeFilters tag exists with a known value:
   *    - Returns combined format: "filters=[pushedFilters],runtimeFilters=[runtimeFilters]"
   *
   * Runtime filters are typically generated during query execution for operations like
   * dynamic partition pruning, broadcast hash joins, etc. They are not known at plan time
   * but can significantly reduce the data scanned.
   *
   * Example outputs:
   * - No runtime filters: "IsNotNull(id), GreaterThan(id, 100)"
   * - With runtime filters: "filters=[IsNotNull(id)],runtimeFilters=[id#1 IN subquery#2]"
   *
   * @param tags Map containing metadata tags including METAFIELD_TAG_PUSHED_FILTERS and
   *             optionally METAFIELD_TAG_RUNTIME_FILTERS
   * @return Combined filter string, or just pushed filters if runtime filters are not available
   */
  def combinePushedFilters(tags: Map[String, String]): String = {
    if (tags.contains(METAFIELD_TAG_RUNTIME_FILTERS)) {
      val runtimeFilters = tags(METAFIELD_TAG_RUNTIME_FILTERS)
      if (runtimeFilters != UNKNOWN_METAFIELD) {
        s"filters=[${tags(METAFIELD_TAG_PUSHED_FILTERS)}],runtimeFilters=[$runtimeFilters]"
      } else {
        tags(METAFIELD_TAG_PUSHED_FILTERS)
      }
    } else {
      tags(METAFIELD_TAG_PUSHED_FILTERS)
    }
  }
}
