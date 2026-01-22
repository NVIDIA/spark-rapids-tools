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
package com.nvidia.spark.rapids.tool.planparser.iceberg

import org.apache.spark.sql.rapids.tool.store.WriteOperationMetadataTrait
import org.apache.spark.sql.rapids.tool.util.StringUtils

/**
 * Extractor for all Iceberg write operations metadata.
 *
 * Handles three types of Iceberg write operations:
 *
 * 1. **AppendData**: Metadata is in `simpleString` (node.desc)
 * {{{
 *   "simpleString": "AppendData ..., IcebergWrite(table=local.db.table, format=PARQUET)"
 * }}}
 *
 * 2. **ReplaceData** (copy-on-write): Metadata is in `physicalPlanDescription`
 * {{{
 *   (12) ReplaceData
 *   Input [16]: [_c0#523, _c1#524, ...]
 *   Arguments: IcebergWrite(table=spark_catalog.default.my_table, format=PARQUET)
 * }}}
 *
 * 3. **WriteDelta** (merge-on-read): Metadata is in `physicalPlanDescription`
 * {{{
 *   (16) WriteDelta
 *   Input [21]: [__row_operation#10023, ...]
 *   Arguments: org.apache.iceberg.spark.source.SparkPositionDeltaWrite@5c5feaaa
 * }}}
 *
 * Note: WriteDelta only shows the class reference, so we mark format as "IcebergPositionDelete"
 * and cannot extract table name.
 */
object IcebergWriteExtract {

  // Regex to extract properties from IcebergWrite(table=..., format=...)
  private val icebergWriteRegex = """IcebergWrite\(([^)]+)\)""".r
  private val propertyRegex = """(\w+)=([^,)]+)""".r

  /**
   * Determines if the given operation name corresponds to an Iceberg write operation.
   * This includes AppendData, ReplaceData, and WriteDelta.
   *
   * @param opName the operation name to check
   * @return true if this extractor handles the operation
   */
  def accepts(opName: String): Boolean = {
    opName.equals(IcebergHelper.EXEC_APPEND_DATA) ||
      opName.equals(IcebergHelper.EXEC_REPLACE_DATA) ||
      opName.equals(IcebergHelper.EXEC_WRITE_DELTA)
  }

  /**
   * Builds write operation metadata for all Iceberg write operations.
   * Dispatches to the appropriate extraction method based on the operation name.
   *
   * @param opName the operation name (AppendData, ReplaceData, or WriteDelta)
   * @param nodeDescr the node description (simpleString) - used for AppendData
   * @param physicalPlanDescription optional physical plan description - for ReplaceData/WriteDelta
   * @param nodeId the node ID - used for ReplaceData/WriteDelta
   * @return Some(WriteOperationMetadataTrait) if extraction succeeds, None otherwise
   */
  def buildWriteOp(
      opName: String,
      nodeDescr: String,
      physicalPlanDescription: Option[String] = None,
      nodeId: Long = -1): Option[WriteOperationMetadataTrait] = {
    opName match {
      // AppendData: metadata is in simpleString (nodeDescr)
      // Delegate to AppendDataIcebergExtract which extends InsertIntoHadoopExtract
      case IcebergHelper.EXEC_APPEND_DATA =>
        Some(AppendDataIcebergExtract.buildWriteOp(nodeDescr))

      // ReplaceData: metadata is in physicalPlanDescription Arguments
      case IcebergHelper.EXEC_REPLACE_DATA =>
        physicalPlanDescription.flatMap { physPlan =>
          extractReplaceDataMeta(physPlan, nodeId)
        }

      // WriteDelta: metadata is in physicalPlanDescription Arguments
      case IcebergHelper.EXEC_WRITE_DELTA =>
        physicalPlanDescription.flatMap { physPlan =>
          extractWriteDeltaMeta(physPlan, nodeId)
        }

      case _ =>
        None
    }
  }

  /**
   * Extracts the Arguments string for a specific node from physicalPlanDescription.
   *
   * @param physicalPlanDescription the full physical plan description string
   * @param nodeId the node ID to find (e.g., 12 for "(12) ReplaceData")
   * @param nodeName the node name to match (e.g., "ReplaceData")
   * @return the Arguments string if found, None otherwise
   */
  private def extractArgumentsForNode(
      physicalPlanDescription: String,
      nodeId: Long,
      nodeName: String): Option[String] = {
    // Multi-line search for the node section
    // Using (?s) flag (DOTALL mode) to make . match newlines, since there may be
    // multiple lines (e.g., Input [...]) between the node header and Arguments line.
    // Using [^\n]+ for the capture group to only get the Arguments content on that line.
    val nodePattern = s"""(?s)\\($nodeId\\)\\s+$nodeName\\s*\\n.*?Arguments:\\s*([^\\n]+)""".r
    nodePattern.findFirstMatchIn(physicalPlanDescription).map(_.group(1).trim)
  }

  /**
   * Parses properties from an IcebergWrite(...) string.
   *
   * @param icebergWriteStr the IcebergWrite string (e.g., "IcebergWrite(table=..., format=...)")
   * @return a map of property name to value
   */
  private def parseIcebergWriteProperties(icebergWriteStr: String): Map[String, String] = {
    icebergWriteRegex.findFirstMatchIn(icebergWriteStr) match {
      case Some(m) =>
        val propsStr = m.group(1)
        propertyRegex.findAllMatchIn(propsStr).map { pm =>
          pm.group(1) -> pm.group(2).trim
        }.toMap
      case None =>
        Map.empty
    }
  }

  /**
   * Extracts write metadata for ReplaceData from physicalPlanDescription.
   *
   * @param physicalPlanDescription the full physical plan description
   * @param nodeId the node ID
   * @return Some(WriteOperationMetadataTrait) if metadata was extracted, None otherwise
   */
  private def extractReplaceDataMeta(
      physicalPlanDescription: String,
      nodeId: Long): Option[WriteOperationMetadataTrait] = {
    extractArgumentsForNode(physicalPlanDescription, nodeId,
        IcebergHelper.EXEC_REPLACE_DATA).flatMap { args =>
      if (args.contains("IcebergWrite(")) {
        val props = parseIcebergWriteProperties(args)
        val format = props.get("format").map { f =>
          val fLower = f.toLowerCase
          if (!fLower.startsWith("iceberg")) s"Iceberg${fLower.capitalize}" else fLower.capitalize
        }.getOrElse(StringUtils.UNKNOWN_EXTRACT)

        val (dbName, tableName) = props.get("table").map { fullTable =>
          val parts = fullTable.split("\\.")
          if (parts.length >= 2) {
            (parts(parts.length - 2), parts.last)
          } else {
            (StringUtils.UNKNOWN_EXTRACT, parts.lastOption.getOrElse(StringUtils.UNKNOWN_EXTRACT))
          }
        }.getOrElse((StringUtils.UNKNOWN_EXTRACT, StringUtils.UNKNOWN_EXTRACT))

        Some(IcebergWriteMetadata(
          execNameVal = IcebergHelper.EXEC_REPLACE_DATA,
          dataFormatVal = format,
          tableNameVal = tableName,
          databaseNameVal = dbName
        ))
      } else {
        None
      }
    }
  }

  /**
   * Extracts write metadata for WriteDelta from physicalPlanDescription.
   * Note: WriteDelta Arguments only contains a class reference, not IcebergWrite(...).
   * We can only indicate it's an Iceberg position delete operation.
   *
   * @param physicalPlanDescription the full physical plan description
   * @param nodeId the node ID
   * @return Some(WriteOperationMetadataTrait) with minimal metadata
   */
  private def extractWriteDeltaMeta(
      physicalPlanDescription: String,
      nodeId: Long): Option[WriteOperationMetadataTrait] = {
    extractArgumentsForNode(physicalPlanDescription, nodeId,
        IcebergHelper.EXEC_WRITE_DELTA).map { args =>
      // WriteDelta shows: org.apache.iceberg.spark.source.SparkPositionDeltaWrite@...
      // We can't extract table/format, but we know it's Iceberg position delete
      val format = if (args.contains("SparkPositionDeltaWrite")) {
        "IcebergPositionDelete"
      } else {
        "IcebergMergeOnRead"
      }

      IcebergWriteMetadata(
        execNameVal = IcebergHelper.EXEC_WRITE_DELTA,
        dataFormatVal = format,
        tableNameVal = StringUtils.UNKNOWN_EXTRACT,
        databaseNameVal = StringUtils.UNKNOWN_EXTRACT
      )
    }
  }
}

/**
 * Simple case class implementing WriteOperationMetadataTrait for Iceberg write operations.
 */
case class IcebergWriteMetadata(
    execNameVal: String,
    dataFormatVal: String,
    tableNameVal: String,
    databaseNameVal: String
) extends WriteOperationMetadataTrait {

  override def execName(): String = execNameVal
  override def dataFormat(): String = dataFormatVal
  override def table(): String = tableNameVal
  override def dataBase(): String = databaseNameVal

  override def execNameCSV: String = execNameVal
  override def formatCSV: String = dataFormatVal
}
