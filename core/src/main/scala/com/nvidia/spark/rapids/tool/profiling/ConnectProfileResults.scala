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

package com.nvidia.spark.rapids.tool.profiling

import com.nvidia.spark.rapids.tool.views.OutHeaderRegistry

import org.apache.spark.sql.rapids.tool.util.StringUtils

/**
 * CSV row for a Spark Connect session. Serializes the lifecycle metadata for a
 * single session into the columns registered under
 * `ConnectSessionProfileResult` in [[OutHeaderRegistry]].
 *
 * `durationMs` is `endTime - startTime` when `endTime` is defined, else `-1`
 * (matches the convention used for open/unfinished sessions in other result
 * classes).
 */
case class ConnectSessionProfileResult(
    appId: String,
    sessionId: String,
    userId: String,
    startTime: Long,
    endTime: Option[Long],
    operationCount: Long) extends ProfileResult {

  override def outputHeaders: Array[String] = {
    OutHeaderRegistry.outputHeaders("ConnectSessionProfileResult")
  }

  override def convertToSeq(): Array[String] = {
    Array(
      appId,
      sessionId,
      userId,
      startTime.toString,
      endTime.map(_.toString).orNull,
      endTime.map(e => (e - startTime).toString).getOrElse("-1"),
      operationCount.toString)
  }

  override def convertToCSVSeq(): Array[String] = {
    Array(
      StringUtils.reformatCSVString(appId),
      StringUtils.reformatCSVString(sessionId),
      StringUtils.reformatCSVString(userId),
      startTime.toString,
      endTime.map(_.toString).orNull,
      endTime.map(e => (e - startTime).toString).getOrElse("-1"),
      operationCount.toString)
  }
}

/**
 * CSV row for a single Spark Connect operation. Captures the full lifecycle
 * (start/analyze/readyForExec/finish/close/fail/cancel timestamps), the
 * derived phase durations, status, producedRowCount, error message, and the
 * joined sqlIDs/jobIDs. Also captures statement-file provenance for the
 * separate `statements/<operationId>.txt` artifact.
 *
 * sqlIds and jobIds are serialized semicolon-separated (to keep the CSV
 * single-column and avoid quoting issues).
 */
case class ConnectOperationProfileResult(
    appId: String,
    operationId: String,
    sessionId: String,
    userId: String,
    jobTag: String,
    startTime: Long,
    analyzeTime: Option[Long],
    readyForExecTime: Option[Long],
    finishTime: Option[Long],
    closeTime: Option[Long],
    failTime: Option[Long],
    cancelTime: Option[Long],
    durationMs: Long,
    analyzePhaseMs: Long,
    planPhaseMs: Long,
    execPhaseMs: Long,
    resultDeliveryPhaseMs: Long,
    status: String,
    producedRowCount: Option[Long],
    errorMessage: Option[String],
    sqlIds: Seq[Long],
    jobIds: Seq[Int],
    statementFile: Option[String],
    statementBytes: Long,
    statementTruncated: Boolean) extends ProfileResult {

  override def outputHeaders: Array[String] = {
    OutHeaderRegistry.outputHeaders("ConnectOperationProfileResult")
  }

  override def convertToSeq(): Array[String] = {
    Array(
      appId,
      operationId,
      sessionId,
      userId,
      jobTag,
      startTime.toString,
      analyzeTime.map(_.toString).orNull,
      readyForExecTime.map(_.toString).orNull,
      finishTime.map(_.toString).orNull,
      closeTime.map(_.toString).orNull,
      failTime.map(_.toString).orNull,
      cancelTime.map(_.toString).orNull,
      durationMs.toString,
      analyzePhaseMs.toString,
      planPhaseMs.toString,
      execPhaseMs.toString,
      resultDeliveryPhaseMs.toString,
      status,
      producedRowCount.map(_.toString).orNull,
      errorMessage.getOrElse(""),
      sqlIds.mkString(";"),
      jobIds.mkString(";"),
      statementFile.getOrElse(""),
      statementBytes.toString,
      statementTruncated.toString)
  }

  override def convertToCSVSeq(): Array[String] = {
    Array(
      StringUtils.reformatCSVString(appId),
      StringUtils.reformatCSVString(operationId),
      StringUtils.reformatCSVString(sessionId),
      StringUtils.reformatCSVString(userId),
      StringUtils.reformatCSVString(jobTag),
      startTime.toString,
      analyzeTime.map(_.toString).orNull,
      readyForExecTime.map(_.toString).orNull,
      finishTime.map(_.toString).orNull,
      closeTime.map(_.toString).orNull,
      failTime.map(_.toString).orNull,
      cancelTime.map(_.toString).orNull,
      durationMs.toString,
      analyzePhaseMs.toString,
      planPhaseMs.toString,
      execPhaseMs.toString,
      resultDeliveryPhaseMs.toString,
      StringUtils.reformatCSVString(status),
      producedRowCount.map(_.toString).orNull,
      StringUtils.reformatCSVString(errorMessage.getOrElse("")),
      StringUtils.reformatCSVString(sqlIds.mkString(";")),
      StringUtils.reformatCSVString(jobIds.mkString(";")),
      StringUtils.reformatCSVString(statementFile.getOrElse("")),
      statementBytes.toString,
      statementTruncated.toString)
  }
}

object ConnectOperationProfileResult {

  /**
   * Marker substring embedded by the Spark Connect server-side abbreviator when
   * a statement/plan text exceeds its configured limit. Presence of this marker
   * in `statementText` indicates the artifact we persist is a truncated
   * representation of the original plan.
   */
  private[profiling] val TruncationMarker: String = "[truncated(size="

  /**
   * Returns `b - a` when both are defined, otherwise `-1`. Used to derive
   * phase durations where an absent timestamp means the operation never
   * reached that phase.
   */
  private def diff(a: Option[Long], b: Option[Long]): Long = {
    (a, b) match {
      case (Some(av), Some(bv)) => bv - av
      case _ => -1L
    }
  }

  /**
   * Derives operation status from the observed lifecycle timestamps.
   * Priority: CANCELED -> FAILED -> SUCCEEDED -> RUNNING.
   * CANCELED precedes FAILED because server-side cancellation sometimes
   * surfaces a trailing failure event we should not misattribute.
   */
  private def deriveStatus(op: ConnectOperationInfo): String = {
    if (op.cancelTime.isDefined) "CANCELED"
    else if (op.failTime.isDefined) "FAILED"
    else if (op.finishTime.isDefined || op.closeTime.isDefined) "SUCCEEDED"
    else "RUNNING"
  }

  def from(
      appId: String,
      op: ConnectOperationInfo,
      sqlIds: Seq[Long],
      jobIds: Seq[Int],
      statementFile: Option[String]): ConnectOperationProfileResult = {
    val endForDuration =
      op.closeTime.orElse(op.finishTime).orElse(op.failTime).orElse(op.cancelTime)
    val durationMs = endForDuration.map(_ - op.startTime).getOrElse(-1L)
    val statementBytes = op.statementText.getBytes("UTF-8").length.toLong
    val statementTruncated = op.statementText.contains(TruncationMarker)
    ConnectOperationProfileResult(
      appId = appId,
      operationId = op.operationId,
      sessionId = op.sessionId,
      userId = op.userId,
      jobTag = op.jobTag,
      startTime = op.startTime,
      analyzeTime = op.analyzeTime,
      readyForExecTime = op.readyForExecTime,
      finishTime = op.finishTime,
      closeTime = op.closeTime,
      failTime = op.failTime,
      cancelTime = op.cancelTime,
      durationMs = durationMs,
      analyzePhaseMs = diff(Some(op.startTime), op.analyzeTime),
      planPhaseMs = diff(op.analyzeTime, op.readyForExecTime),
      execPhaseMs = diff(op.readyForExecTime, op.finishTime),
      resultDeliveryPhaseMs = diff(op.finishTime, op.closeTime),
      status = deriveStatus(op),
      producedRowCount = op.producedRowCount,
      errorMessage = op.errorMessage,
      sqlIds = sqlIds,
      jobIds = jobIds,
      statementFile = statementFile,
      statementBytes = statementBytes,
      statementTruncated = statementTruncated)
  }
}
