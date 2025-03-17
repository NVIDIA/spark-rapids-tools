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

package org.apache.spark.sql.rapids.tool.store


import java.util.concurrent.ConcurrentHashMap

import scala.util.control.NonFatal

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.rapids.tool.util.StringUtils

/**
 * Represents a unique reference for a name, with a reformatted CSV value.
 * @param value The original name value.
 */
case class UniqueNameRef(value: String) {
  // Lazily reformats the name value into a CSV-compatible string.
  lazy val csvValue: String = StringUtils.reformatCSVString(value)
}

/**
 * Trait defining metadata for write operations.
 * This trait provides default implementations for metadata fields
 * related to write operations, which can be overridden by subclasses.
 */
trait WriteOperationMetadataTrait {
  def execName(): String = StringUtils.UNKNOWN_EXTRACT // Name of the execution
  def dataFormat(): String = StringUtils.UNKNOWN_EXTRACT // Data format (e.g., CSV, Parquet)
  def outputPath(): String = StringUtils.UNKNOWN_EXTRACT // Output path for the write operation
  def outputColumns(): String = StringUtils.UNKNOWN_EXTRACT // Output columns involved
  def writeMode(): String = StringUtils.UNKNOWN_EXTRACT // Save mode (e.g., Overwrite, Append)
  def table(): String = StringUtils.UNKNOWN_EXTRACT // Table name (if applicable)
  def dataBase(): String = StringUtils.UNKNOWN_EXTRACT // Database name (if applicable)
  def fullDescr(): String = "..." // Full description of the operation
  def execNameCSV: String // CSV-compatible execution name
  def formatCSV: String // CSV-compatible data format
  def partitions(): String = StringUtils.UNKNOWN_EXTRACT // Partitions involved in the operation
}

/**
 * Metadata implementation for write operations with a specific format.
 * @param writeExecName The execution name reference.
 * @param format The data format reference.
 * @param descr Optional description of the operation.
 */
class WriteOperationMetaWithFormat(
    val writeExecName: UniqueNameRef,
    val format: UniqueNameRef,
    val descr: Option[String]) extends WriteOperationMetadataTrait {
  override def dataFormat(): String = format.value
  override def fullDescr(): String = descr.getOrElse("")
  override def execName(): String = writeExecName.value
  override def execNameCSV: String = writeExecName.csvValue
  override def formatCSV: String = format.csvValue
}

/**
 * Metadata implementation for write operations with additional details.
 * @param writeExecName The execution name reference.
 * @param format The data format reference.
 * @param outputPathValue Optional output path.
 * @param outputColumnsValue Optional output columns.
 * @param saveMode Optional save mode.
 * @param tableName Table name (if applicable).
 * @param dataBaseName Database name (if applicable).
 * @param descr Optional description of the operation.
 */
case class WriteOperationMeta(
    override val writeExecName: UniqueNameRef,
    override val format: UniqueNameRef,
    outputPathValue: Option[String],
    outputColumnsValue: Option[String],
    saveMode: Option[SaveMode],
    tableName: String,
    dataBaseName: String,
    partitionCols: Option[String],
    override val descr: Option[String]) extends WriteOperationMetaWithFormat(
      writeExecName, format, descr) {
  override def writeMode(): String = {
    saveMode match {
      case Some(w) => w.toString
      case _ => StringUtils.UNKNOWN_EXTRACT
    }
  }
  override def outputPath(): String = outputPathValue.getOrElse(StringUtils.UNKNOWN_EXTRACT)
  override def outputColumns(): String = outputColumnsValue.getOrElse(StringUtils.UNKNOWN_EXTRACT)
  override def table(): String = tableName
  override def dataBase(): String = dataBaseName
  override def partitions(): String = partitionCols.getOrElse(StringUtils.UNKNOWN_EXTRACT)
}

/**
 * Represents a record of a write operation.
 * @param sqlID The SQL ID associated with the operation.
 * @param version The version of the operation.
 * @param nodeId The node ID in the execution plan.
 * @param operationMeta Metadata for the write operation.
 * @param fromFinalPlan Indicates if the metadata is from the final execution plan.
 */
case class WriteOperationRecord(
    sqlID: Long,
    version: Int,
    nodeId: Long,
    operationMeta: WriteOperationMetadataTrait,
    fromFinalPlan: Boolean = true)

/**
 * Builder object for creating instances of WriteOperationMetadataTrait.
 * Provides utility methods to construct metadata objects with various levels of detail.
 */
object WriteOperationMetaBuilder {
  // Default unknown name reference
  private val UNKNOWN_NAME_REF = UniqueNameRef(StringUtils.UNKNOWN_EXTRACT)

  // Default unknown metadata
  private val UNKNOWN_WRITE_META =
    new WriteOperationMetaWithFormat(UNKNOWN_NAME_REF, UNKNOWN_NAME_REF, None)

  // Concurrent hash map to store data format references
  private val DATA_FORMAT_TABLE: ConcurrentHashMap[String, UniqueNameRef] = {
    val initMap = new ConcurrentHashMap[String, UniqueNameRef]()
    initMap.put(StringUtils.UNKNOWN_EXTRACT, UNKNOWN_NAME_REF)
    initMap
  }

  // Concurrent hash map to store execution name references
  private val WRITE_EXEC_TABLE: ConcurrentHashMap[String, UniqueNameRef] = {
    val initMap = new ConcurrentHashMap[String, UniqueNameRef]()
    initMap.put(StringUtils.UNKNOWN_EXTRACT, UNKNOWN_NAME_REF)
    initMap
  }

  /**
   * Returns a default value if the input string is null or empty.
   * @param value The input string.
   * @return The default value or the input string.
   */
  private def defaultIfUnknown(value: String): String = {
    if (value == null || value.isEmpty) StringUtils.UNKNOWN_EXTRACT else value
  }

  /**
   * Retrieves or creates a UniqueNameRef for the given data format.
   * @param name The data format name.
   * @return A UniqueNameRef for the data format.
   */
  private def getOrCreateFormatRef(name: String): UniqueNameRef = {
    DATA_FORMAT_TABLE.computeIfAbsent(defaultIfUnknown(name), k => UniqueNameRef(k))
  }

  /**
   * Retrieves or creates a UniqueNameRef for the given execution name.
   * @param name The execution name.
   * @return A UniqueNameRef for the execution name.
   */
  private def getOrCreateExecRef(name: String): UniqueNameRef = {
    WRITE_EXEC_TABLE.computeIfAbsent(defaultIfUnknown(name), k => UniqueNameRef(k))
  }

  /**
   * Converts a string to a SaveMode, if possible.
   * @param name The string representation of the save mode.
   * @return An Option containing the SaveMode, or None if conversion fails.
   */
  private def getSaveModeFromString(name: String): Option[SaveMode] = {
    val str = defaultIfUnknown(name)
    try {
      Some(SaveMode.valueOf(str))
    } catch { // Failed to convert the string to SaveMode.
      case NonFatal(_) => None
    }
  }

  /**
   * Builds a WriteOperationMetadataTrait with detailed metadata.
   * @param execName The execution name.
   * @param dataFormat The data format.
   * @param outputPath Optional output path.
   * @param outputColumns Optional output columns.
   * @param writeMode The save mode.
   * @param tableName The table name.
   * @param dataBaseName The database name.
   * @param partitionCols Optional partition columns.
   * @param fullDescr Optional full description.
   * @return A WriteOperationMetadataTrait instance.
   */
  def build(execName: String, dataFormat: String, outputPath: Option[String],
    outputColumns: Option[String],
    writeMode: String,
    tableName: String,
    dataBaseName: String,
    partitionCols: Option[String],
    fullDescr: Option[String]): WriteOperationMetadataTrait = {
    WriteOperationMeta(getOrCreateExecRef(execName), getOrCreateFormatRef(dataFormat),
      outputPath, outputColumns, getSaveModeFromString(writeMode),
      defaultIfUnknown(tableName), defaultIfUnknown(dataBaseName),
      partitionCols, fullDescr)
  }

  /**
   * Builds a WriteOperationMetadataTrait with minimal metadata.
   * @param execName The execution name.
   * @param dataFormat The data format.
   * @param fullDescr Optional full description.
   * @return A WriteOperationMetadataTrait instance.
   */
  def build(execName: String, dataFormat: String,
    fullDescr: Option[String]): WriteOperationMetadataTrait = {
    new WriteOperationMetaWithFormat(getOrCreateExecRef(execName),
      getOrCreateFormatRef(dataFormat), fullDescr)
  }

  /**
   * Builds a WriteOperationMetadataTrait with no metadata.
   * @param fullDescr Optional full description.
   * @return A WriteOperationMetadataTrait instance with unknown metadata.
   */
  def buildNoMeta(fullDescr: Option[String]): WriteOperationMetadataTrait = {
    if (fullDescr.isDefined) {
      new WriteOperationMetaWithFormat(UNKNOWN_NAME_REF, UNKNOWN_NAME_REF, fullDescr)
    } else {
      UNKNOWN_WRITE_META
    }
  }
}
