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

import com.nvidia.spark.rapids.tool.planparser.{ExecInfo, GenericExecParser, SQLPlanParser, SupportedOpStub}
import com.nvidia.spark.rapids.tool.planparser.ops.{UnsupportedExprOpRef, UnsupportedReasonRef}
import com.nvidia.spark.rapids.tool.qualification.PluginTypeChecker

import org.apache.spark.sql.rapids.tool.AppBase
import org.apache.spark.sql.rapids.tool.plangraph.SparkPlanGraphNode
import org.apache.spark.sql.rapids.tool.store.WriteOperationMetadataTrait
import org.apache.spark.sql.rapids.tool.util.StringUtils

/**
 * Parser for three sibling Iceberg execs:
 *
 *   - `MergeRows`   — intermediate row classifier used by MERGE INTO. Emits
 *     `keep` / `discard` / `split` expressions; not a write operator.
 *   - `ReplaceData` — copy-on-write write exec; rewrites data files for CoW
 *     DELETE / UPDATE / MERGE.
 *   - `WriteDelta`  — merge-on-read write exec; writes position-delete files for
 *     MoR DELETE / UPDATE / MERGE.
 *
 * The three execs are siblings in Spark's plan tree, not a hierarchy. Each branch
 * has its own gate set:
 *
 *   - MergeRows: parses merge expressions; unsupported.
 *   - ReplaceData: runtime/config + catalog + Parquet data-file format gates from
 *     [[IcebergGpuSupport]].
 *   - WriteDelta: unsupported; controlled at the plugin by
 *     `spark.rapids.sql.exec.WriteDeltaExec`.
 *
 * DAG shapes:
 *
 * {{{
 *   Copy-on-write MERGE:        Merge-on-read MERGE:
 *     ReplaceData                  WriteDelta
 *     +- Project                   +- Exchange
 *        +- MergeRows                 +- MergeRows
 *           +- SortMergeJoin             +- SortMergeJoin
 * }}}
 *
 * TODO: split into per-exec parser classes; each branch has independent logic.
 *
 * @param node the Spark plan graph node
 * @param checker the plugin type checker
 * @param sqlID SQL ID associated with the node
 * @param opStub the SupportedOpStub for this operation
 * @param app optional application context
 */
class MergeRowsIcebergParser(
    override val node: SparkPlanGraphNode,
    override val checker: PluginTypeChecker,
    override val sqlID: Long,
    opStub: SupportedOpStub,
    override val app: Option[AppBase] = None
) extends GenericExecParser(
    node = node,
    checker = checker,
    sqlID = sqlID,
    execName = Some(opStub.execID),
    app = app) {

  override lazy val fullExecName: String = opStub.execID

  override def pullSpeedupFactor(registeredName: Option[String] = None): Double = 1.0

  override def pullSupportedFlag(registeredName: Option[String] = None): Boolean = {
    // The opStub gates the operator as a whole (MergeRows and WriteDelta stay false here).
    // For ReplaceData we additionally require the supportedExecs.csv baseline (via
    // super), then the per-write gates: the runtime/config cascade and catalog
    // allowlist shared with AppendDataIcebergParser, plus a Parquet data-file format.
    if (!opStub.isSupported) {
      false
    } else if (node.name == IcebergHelper.EXEC_REPLACE_DATA) {
      super.pullSupportedFlag() && checkIcebergRuntimeGates && checkCatalogSupport &&
        isReplaceDataWriteSupported
    } else {
      true
    }
  }

  /**
   * Extracts the ReplaceData data-file format from `physicalPlanDescription` and
   * delegates to `IcebergGpuSupport.isSupportedDataFileFormat`. If the format string
   * is unavailable the helper stays optimistic, mirroring AppendData behavior.
   */
  protected def isReplaceDataWriteSupported: Boolean = {
    val meta: Option[WriteOperationMetadataTrait] = for {
      appInst <- app
      physPlan <- appInst.sqlManager.applyToPlanModel(sqlID)(_.plan.physicalPlanDescription)
      if physPlan.nonEmpty
      m <- IcebergWriteExtract.buildWriteOp(IcebergHelper.EXEC_REPLACE_DATA, node.desc,
        Some(physPlan))
    } yield m

    val fmt = meta.map(_.dataFormat()).getOrElse(StringUtils.UNKNOWN_EXTRACT)
    val ok = IcebergGpuSupport.isSupportedDataFileFormat(fmt)
    if (!ok) {
      setUnsupportedReason(UnsupportedReasonRef.UNSUPPORTED_IO_FORMAT)
    }
    ok
  }

  /**
   * Plumbs `IcebergGpuSupport.firstUnsupportedWritePrerequisite` to the parser's
   * `setUnsupportedReason`.
   */
  protected def checkIcebergRuntimeGates: Boolean = {
    val props = app.map(_.sparkProperties).getOrElse(Map.empty[String, String])
    val sparkVer = app.map(_.sparkVersion).getOrElse("")
    val isDB = app.exists(_.dbPlugin.isEnabled)
    IcebergGpuSupport.firstUnsupportedWritePrerequisite(props, sparkVer, isDB) match {
      case Some(reason) =>
        setUnsupportedReason(reason)
        false
      case None => true
    }
  }

  /**
   * Plumbs `IcebergGpuSupport.firstUnsupportedCatalogReason` to the parser's
   * `setUnsupportedReason`.
   */
  protected def checkCatalogSupport: Boolean = {
    val props = app.map(_.sparkProperties).getOrElse(Map.empty[String, String])
    IcebergGpuSupport.firstUnsupportedCatalogReason(props) match {
      case Some(reason) =>
        setUnsupportedReason(reason)
        false
      case None => true
    }
  }

  // The value that will be reported as ExecName in the ExecInfo object created by this parser.
  override def reportedExecName: String = trimmedNodeName

  /**
   * Parse expressions from physicalPlanDescription for MergeRows operator.
   * Extracts keep, discard, and split expressions from the Arguments line.
   */
  override protected def parseExpressions(): Array[String] = {
    // Only MergeRows has merge-specific expressions (keep, discard, split)
    if (node.name != IcebergHelper.EXEC_MERGE_ROWS) {
      return Array.empty[String]
    }

    // Unwrap the optional app instance, then look up the physical plan description
    // for this SQL ID. Both `app` and `applyToPlanModel` return Option, so the
    // for-comprehension short-circuits to None if either is absent or empty.
    val physPlanOpt = for {
      appInst <- app
      physPlan <- appInst.sqlManager.applyToPlanModel(sqlID)(_.plan.physicalPlanDescription)
      if physPlan.nonEmpty
    } yield physPlan

    // Parse merge expressions from the physical plan description
    physPlanOpt.map { physPlan =>
      SQLPlanParser.parseMergeRowsExpressions(physPlan)
    }.getOrElse(Array.empty[String])
  }

  /**
   * Overrides createExecInfo to set the correct opType from the opStub.
   *
   * This parser handles MergeRows (OpType: Exec), ReplaceData, and WriteDelta
   * (both OpType: WriteExec). The default GenericExecParser always uses OpTypes.Exec,
   * so we must set opType = opStub.pullOpType to preserve the write operator classification
   * for ReplaceData and WriteDelta.
   */
  override protected def createExecInfo(
      speedupFactor: Double,
      isSupported: Boolean,
      duration: Option[Long],
      notSupportedExprs: Seq[UnsupportedExprOpRef],
      expressions: Array[String]): ExecInfo = {
    ExecInfo(
      node,
      sqlID,
      reportedExecName,
      reportedExpr,
      speedupFactor,
      duration,
      node.id,
      isSupported,
      children = getChildren,
      unsupportedExecReason = unsupportedReason,
      unsupportedExprs = notSupportedExprs,
      opType = opStub.pullOpType,
      expressions = expressions
    )
  }
}
