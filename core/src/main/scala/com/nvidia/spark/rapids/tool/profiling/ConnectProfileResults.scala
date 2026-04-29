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
 * `ConnectSessionProfileResult` in [[com.nvidia.spark.rapids.tool.views.OutHeaderRegistry]].
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
 * CSV row for a single Spark Connect operation. Captures the core lifecycle
 * (start/finish/close/fail/cancel timestamps), derived status, error message,
 * and the joined sqlIDs/jobIDs. Also records the sidecar basename for the
 * `connect_statements/<operationId>.txt` artifact.
 *
 * sqlIds and jobIds are serialized semicolon-separated to keep the CSV
 * single-column and avoid quoting issues.
 */
case class ConnectOperationProfileResult(
    appId: String,
    operationId: String,
    sessionId: String,
    userId: String,
    jobTag: String,
    startTime: Long,
    finishTime: Option[Long],
    closeTime: Option[Long],
    failTime: Option[Long],
    cancelTime: Option[Long],
    durationMs: Long,
    status: String,
    errorMessage: Option[String],
    sqlIds: Seq[Long],
    jobIds: Seq[Int],
    statementFile: Option[String],
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
      finishTime.map(_.toString).orNull,
      closeTime.map(_.toString).orNull,
      failTime.map(_.toString).orNull,
      cancelTime.map(_.toString).orNull,
      durationMs.toString,
      status,
      errorMessage.getOrElse(""),
      sqlIds.mkString(";"),
      jobIds.mkString(";"),
      statementFile.getOrElse(""),
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
      finishTime.map(_.toString).orNull,
      closeTime.map(_.toString).orNull,
      failTime.map(_.toString).orNull,
      cancelTime.map(_.toString).orNull,
      durationMs.toString,
      StringUtils.reformatCSVString(status),
      StringUtils.reformatCSVString(errorMessage.getOrElse("")),
      StringUtils.reformatCSVString(sqlIds.mkString(";")),
      StringUtils.reformatCSVString(jobIds.mkString(";")),
      StringUtils.reformatCSVString(statementFile.getOrElse("")),
      statementTruncated.toString)
  }
}

object ConnectOperationProfileResult {

  /**
   * Marker substring embedded by the Spark Connect server-side abbreviator when
   * a statement/plan text exceeds its configured limit. Presence of this marker
   * in `statementText` indicates the artifact we persist is a truncated
   * representation of the original plan.
   *
   * Note: Spark 4.x also performs depth-based structural elision by collapsing
   * subtrees to `{}` once the protobuf text formatter exceeds its nesting cap.
   * Those cases do not emit a textual marker, so `statementTruncated` is
   * intentionally limited to marker-based truncation only.
   */
  private[profiling] val TruncationMarker: String = "[truncated(size="

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
    val statementTruncated = op.statementText.contains(TruncationMarker)
    ConnectOperationProfileResult(
      appId = appId,
      operationId = op.operationId,
      sessionId = op.sessionId,
      userId = op.userId,
      jobTag = op.jobTag,
      startTime = op.startTime,
      finishTime = op.finishTime,
      closeTime = op.closeTime,
      failTime = op.failTime,
      cancelTime = op.cancelTime,
      durationMs = durationMs,
      status = deriveStatus(op),
      errorMessage = op.errorMessage,
      sqlIds = sqlIds,
      jobIds = jobIds,
      statementFile = statementFile,
      statementTruncated = statementTruncated)
  }
}
