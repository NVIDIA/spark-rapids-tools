/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.tool.planparser.{ExecInfo, GenericExecParser, SupportedOpStub}
import com.nvidia.spark.rapids.tool.planparser.ops.{OpTypes, UnsupportedExprOpRef, UnsupportedReasonRef}
import com.nvidia.spark.rapids.tool.qualification.PluginTypeChecker

import org.apache.spark.sql.rapids.tool.AppBase
import org.apache.spark.sql.rapids.tool.plangraph.SparkPlanGraphNode
import org.apache.spark.sql.rapids.tool.store.{CompressionCodec, WriteOperationMetadataTrait}
import org.apache.spark.sql.rapids.tool.util.StringUtils

/**
 * A parser for Iceberg AppendData operations.
 * @param node the Spark plan graph node
 * @param checker the plugin type checker
 * @param sqlID SQL ID associated with the node
 * @param opStub the SupportedOpStub for this operation
 * @param app optional application context
 */
class AppendDataIcebergParser(
    override val node: SparkPlanGraphNode,
    override val checker: PluginTypeChecker,
    override val sqlID: Long,
    opStub: SupportedOpStub,
    override val app: Option[AppBase] = None
) extends GenericExecParser(
    node = node,
    checker = checker,
    sqlID = sqlID,
    execName = Some(opStub.nodeName),
    app = app) {

  override lazy val fullExecName: String = opStub.execID
  // Extract the write operation metadata from the node description
  // This will parse the node description to extract format, compression, etc.
  lazy val writeOpMeta: WriteOperationMetadataTrait = {
    AppendDataIcebergExtract.buildWriteOp(nodeDescr = node.desc)
  }
  private lazy val compressionSupported: Boolean = {
    writeOpMeta.compressOption() match {
      case StringUtils.UNKNOWN_EXTRACT | CompressionCodec.UNCOMPRESSED =>
        true
      case codec => // the output depends on the format
        writeOpMeta.dataFormat() match {
          case StringUtils.UNKNOWN_EXTRACT =>
            true // unknown format, so return true when it comes to compression.
          case f if f.toLowerCase.contains("parquet") =>
            // Parquet, IcebergParquet
            // Parquet supports compression, so we need to check if the compression is supported.
            checkCompressionParquet(codec)
          case f if f.toLowerCase.contains("orc") =>
            // ORC, HiveORC, IcebergORC
            checkCompressionORC(codec)
          case _ =>
            // Other formats do not support compression, so return false.
            false
        }
    }
  }

  override def pullSpeedupFactor(registeredName: Option[String] = None): Double = 1.5

  override def pullSupportedFlag(registeredName: Option[String] = None): Boolean = {
    // Order matters: the runtime/config gates are cheaper than the format/catalog/compression
    // checks and surface clearer reasons when they fail.
    opStub.isSupported && super.pullSupportedFlag() &&
      checkIcebergRuntimeGates && isWriteSupported
  }

  protected def checkCompressionORC(codec: String): Boolean = {
    // IcebergORC compression is not supported
    false
  }

  protected def checkCompressionParquet(codec: String): Boolean = {
    // IcebergParquet compression is supported for zstd, snappy, gzip
    codec match {
      case "zstd" | "snappy" | "gzip" =>
        true
      case _ => false // something does not match, so return false.
    }
  }

  protected def checkCatalogSupport: Boolean = {
    val props = app.map(_.sparkProperties).getOrElse(Map.empty[String, String])
    IcebergGpuSupport.firstUnsupportedCatalogReason(props) match {
      case Some(reason) =>
        setUnsupportedReason(reason)
        false
      case None => true
    }
  }

  protected def checkCompression: Boolean = {
    if (!compressionSupported) {
      setUnsupportedReason("Unsupported compression")
    }
    compressionSupported
  }

  protected def isWriteSupported: Boolean = {
    if (isIcebergWriteFormatSupported) {
      // check if the compression is supported
      checkCatalogSupport && checkCompression
    } else {
      setUnsupportedReason(UnsupportedReasonRef.UNSUPPORTED_IO_FORMAT)
      false
    }
  }

  /**
   * Delegates to `IcebergGpuSupport.isSupportedDataFileFormat` against the format
   * extracted from `node.desc` by `AppendDataIcebergExtract`.
   */
  protected def isIcebergWriteFormatSupported: Boolean = {
    IcebergGpuSupport.isSupportedDataFileFormat(writeOpMeta.dataFormat())
  }

  /**
   * Plumbs `IcebergGpuSupport.firstUnsupportedWritePrerequisite` to the parser's
   * `setUnsupportedReason`.
   */
  protected def checkIcebergRuntimeGates: Boolean = {
    val props = app.map(_.sparkProperties).getOrElse(Map.empty[String, String])
    val sparkVer = app.map(_.sparkVersion).getOrElse("")
    IcebergGpuSupport.firstUnsupportedWritePrerequisite(props, sparkVer) match {
      case Some(reason) =>
        setUnsupportedReason(reason)
        false
      case None => true
    }
  }

  // The value that will be reported as ExecName in the ExecInfo object created by this parser.
  override def reportedExecName: String = s"$trimmedNodeName ${writeOpMeta.dataFormat()}"

  override def createExecInfo(
      speedupFactor: Double,
      isSupported: Boolean,
      duration: Option[Long],
      notSupportedExprs: Seq[UnsupportedExprOpRef],
      expressions: Array[String]): ExecInfo = {
    // We do not want to parse the node description to avoid mistakenly marking the node as RDD/UDF.
    ExecInfo.createExecNoNode(
      sqlID,
      exec = reportedExecName,
      s"Format: ${writeOpMeta.dataFormat()}",
      speedupFactor, duration, node.id,
      opType = OpTypes.WriteExec,
      isSupported = isSupported,
      children = None,
      unsupportedExecReason = unsupportedReason,
      expressions = Seq.empty
    )
  }
}
