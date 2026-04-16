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

/**
 * Storage classes for Spark Connect event data (Spark 3.5+).
 * Populated reflectively by [[org.apache.spark.sql.rapids.tool.util.ConnectEventHandler]].
 */

/**
 * Session lifecycle data from Connect session events.
 *
 * @param sessionId UUID assigned by Connect to identify the client session.
 * @param userId    User who owns the session.
 * @param startTime Epoch millis when the session was created.
 * @param endTime   Epoch millis when the session was closed (None if still open or
 *                  server was killed before the close event).
 */
case class ConnectSessionInfo(
    sessionId: String,
    userId: String,
    startTime: Long,
    var endTime: Option[Long] = None)

/**
 * Full lifecycle of a single Connect operation. Populated incrementally
 * as events arrive. The lifecycle is:
 *
 * Started → Analyzed → ReadyForExecution → Finished → Closed
 *                                        └→ Failed
 *                                        └→ Canceled
 *
 * @param operationId     UUID assigned by Connect for this operation.
 * @param sessionId       The session this operation belongs to.
 * @param userId          User who submitted the operation.
 * @param jobTag          Correlation key linking to
 *                        SQLExecutionStart.jobTags and
 *                        JobStart.Properties["spark.job.tags"].
 * @param statementText   Protobuf text format of the ExecutePlanRequest logical plan.
 * @param startTime       Epoch millis when the operation was received.
 * @param analyzeTime     Epoch millis after plan analysis completed.
 * @param readyForExecTime Epoch millis when planning completed and execution can start.
 * @param finishTime      Epoch millis when execution completed (before results sent).
 * @param closeTime       Epoch millis when results were sent and operation cleaned up.
 * @param producedRowCount Number of result rows (from OperationFinished).
 * @param errorMessage    Error message if the operation failed.
 * @param failTime        Epoch millis when the operation failed.
 * @param isCanceled      True if the operation was explicitly canceled.
 * @param cancelTime      Epoch millis when the operation was canceled.
 */
class ConnectOperationInfo(
    val operationId: String,
    val sessionId: String,
    val userId: String,
    val jobTag: String,
    val statementText: String,
    val startTime: Long,
    var analyzeTime: Option[Long] = None,
    var readyForExecTime: Option[Long] = None,
    var finishTime: Option[Long] = None,
    var closeTime: Option[Long] = None,
    var producedRowCount: Option[Long] = None,
    var errorMessage: Option[String] = None,
    var failTime: Option[Long] = None,
    var isCanceled: Boolean = false,
    var cancelTime: Option[Long] = None)
