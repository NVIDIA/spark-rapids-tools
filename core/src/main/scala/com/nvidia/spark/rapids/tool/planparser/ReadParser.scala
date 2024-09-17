/*
 * Copyright (c) 2022-2024, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.tool.qualification.PluginTypeChecker

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.ui.SparkPlanGraphNode

case class ReadMetaData(schema: String, location: String, format: String,
    tags: Map[String, String] = ReadParser.DEFAULT_METAFIELD_MAP) {
  // Define properties to access the tags
  def pushedFilters: String = tags(ReadParser.METAFIELD_TAG_PUSHED_FILTERS)
  def dataFilters: String = tags(ReadParser.METAFIELD_TAG_DATA_FILTERS)
  def partitionFilters: String = tags(ReadParser.METAFIELD_TAG_PARTITION_FILTERS)
}

object ReadParser extends Logging {
  // It was found that some eventlogs could have "NativeScan" instead of "Scan"
  val SCAN_NODE_PREFIXES = Seq("Scan", "NativeScan")
  // DatasourceV2 node names that exactly match the following labels
  val DATASOURCE_V2_NODE_EXACT_PREF = Set(
    "BatchScan")
  // DatasourceV2 node names that match partially on the following labels
  val DATASOURCE_V2_NODE_PREF = Set(
    "GpuScan",
    "GpuBatchScan",
    "JDBCRelation")

  val METAFIELD_TAG_DATA_FILTERS = "DataFilters"
  val METAFIELD_TAG_PUSHED_FILTERS = "PushedFilters"
  val METAFIELD_TAG_PARTITION_FILTERS = "PartitionFilters"

  val UNKNOWN_METAFIELD: String = "unknown"
  val DEFAULT_METAFIELD_MAP: Map[String, String] = collection.immutable.Map(
    METAFIELD_TAG_DATA_FILTERS -> UNKNOWN_METAFIELD,
    METAFIELD_TAG_PUSHED_FILTERS -> UNKNOWN_METAFIELD,
    METAFIELD_TAG_PARTITION_FILTERS -> UNKNOWN_METAFIELD
  )

  def isScanNode(nodeName: String): Boolean = {
    SCAN_NODE_PREFIXES.exists(nodeName.startsWith(_))
  }

  def isScanNode(node: SparkPlanGraphNode): Boolean = {
    isScanNode(node.name)
  }

  def isDataSourceV2Node(node: SparkPlanGraphNode): Boolean = {
    DATASOURCE_V2_NODE_EXACT_PREF.exists(node.name.equals(_)) ||
      DATASOURCE_V2_NODE_PREF.exists(node.name.contains(_))
  }

  // strip off the struct<> part that Spark adds to the ReadSchema
  def formatSchemaStr(schema: String): String = {
    schema.stripPrefix("struct<").stripSuffix(">")
  }

  // This tries to get just the field specified by tag in a string that
  // may contain multiple fields.  It looks for a comma to delimit fields.
  private def getFieldWithoutTag(str: String, tag: String): String = {
    val index = str.indexOf(tag)
    // remove the tag from the final string returned
    val subStr = str.substring(index + tag.size)
    val endIndex = subStr.indexOf(", ")
    // InMemoryFileIndex[hdfs://bdbl-rpm-1160
    if (endIndex != -1) {
      subStr.substring(0, endIndex)
    } else {
      subStr
    }
  }

  // Used to extract metadata fields from Spark GraphNode’s description.
  // It is made public for testing purposes. It returns DEFAULT_METAFIELD_MAP if no tags exist.
  def extractReadTags(value: String): Map[String, String] = {
    // initialize the results to the default values
    var result = Map[String, String]() ++ DEFAULT_METAFIELD_MAP
    // For each meta tag, create a regx that matches the value of the tag until the first
    // closing bracket or the first ellipsis.
    val metaFieldRegexMap = result.map { case (k, _) =>
      (k, s"($k): \\[(.*?)(\\.\\.\\.|\\])".r)
    }
    metaFieldRegexMap.foreach { case (k, v) =>
      v.findFirstMatchIn(value).foreach { m =>
        // if group(3) is an ellipsis then we should append it to the result.
        val ellipse = if (m.group(3).equals("...")) "..." else ""
        result += (k -> s"${m.group(2) + ellipse}")
      }
    }
    result
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
        if (node.name.startsWith("Gpu")) {
          s"${format}(GPU)"
        } else {
          format
        }
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
}
