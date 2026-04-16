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

package org.apache.spark.sql.rapids.tool.util

import com.nvidia.spark.rapids.tool.profiling.{ConnectOperationInfo, ConnectSessionInfo}

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.sql.rapids.tool.AppBase

/**
 * Handles Spark Connect listener events using reflection.
 *
 * Connect event classes live in the spark-connect jar (org.apache.spark.sql.connect.service)
 * which is only available for Spark 3.5+. Since the tools compile against a single shared
 * source tree for Spark 3.2–3.5+, we cannot import Connect classes directly.
 *
 * Instead, this handler:
 * 1. Checks the event class name to identify Connect events (cheap string prefix check)
 * 2. Uses cached reflective method accessors to extract field values
 * 3. Stores the extracted data in tool-owned types (ConnectSessionInfo, ConnectOperationInfo)
 *
 * This follows the established patterns in EventUtils (modifiedConfigsField,
 * rootExecutionIdField) and EventProcessorBase (doSparkListenerResourceProfileAddedReflect).
 * Reflection is preferred over version checks because vendors may backport features.
 */
object ConnectEventHandler extends Logging {

  private val CONNECT_EVENT_PREFIX =
    "org.apache.spark.sql.connect.service.SparkListenerConnect"

  /**
   * Quick class-name check to avoid reflection overhead for non-Connect events.
   * Called from the default case in EventProcessorBase.processAnyEvent.
   */
  def isConnectEvent(event: SparkListenerEvent): Boolean = {
    event.getClass.getName.startsWith(CONNECT_EVENT_PREFIX)
  }

  /**
   * Process a Connect event by extracting data via reflection and storing it
   * in the app's Connect storage maps.
   *
   * @return true if the event was a recognized Connect event, false otherwise
   */
  def processConnectEvent(app: AppBase, event: SparkListenerEvent): Boolean = {
    if (!isConnectEvent(event)) {
      return false
    }
    val suffix = event.getClass.getName.stripPrefix(CONNECT_EVENT_PREFIX)
    try {
      suffix match {
        case "SessionStarted" => handleSessionStarted(app, event)
        case "SessionClosed" => handleSessionClosed(app, event)
        case "OperationStarted" => handleOperationStarted(app, event)
        case "OperationAnalyzed" => handleOperationAnalyzed(app, event)
        case "OperationReadyForExecution" => handleOperationReadyForExecution(app, event)
        case "OperationFinished" => handleOperationFinished(app, event)
        case "OperationClosed" => handleOperationClosed(app, event)
        case "OperationFailed" => handleOperationFailed(app, event)
        case "OperationCanceled" => handleOperationCanceled(app, event)
        case _ =>
          logDebug(s"Unrecognized Connect event suffix: $suffix")
          return false
      }
      true
    } catch {
      case e @ (_: NoSuchMethodException | _: SecurityException) =>
        logWarning(s"Connect event reflection failed for $suffix: ${e.getMessage}")
        false
    }
  }

  // --- Session event handlers ---

  private def handleSessionStarted(app: AppBase, event: SparkListenerEvent): Unit = {
    val info = ConnectSessionInfo(
      sessionId = getString(event, "sessionId"),
      userId = getString(event, "userId"),
      startTime = getLong(event, "eventTime"))
    app.connectSessions.put(info.sessionId, info)
    logDebug(s"Connect session started: ${info.sessionId} user=${info.userId}")
  }

  private def handleSessionClosed(app: AppBase, event: SparkListenerEvent): Unit = {
    val sessionId = getString(event, "sessionId")
    val eventTime = getLong(event, "eventTime")
    app.connectSessions.get(sessionId).foreach { session =>
      session.endTime = Some(eventTime)
    }
    logDebug(s"Connect session closed: $sessionId")
  }

  // --- Operation event handlers ---

  private def handleOperationStarted(app: AppBase, event: SparkListenerEvent): Unit = {
    val info = new ConnectOperationInfo(
      operationId = getString(event, "operationId"),
      sessionId = getString(event, "sessionId"),
      userId = getString(event, "userId"),
      jobTag = getString(event, "jobTag"),
      statementText = getString(event, "statementText"),
      startTime = getLong(event, "eventTime"))
    app.connectOperations.put(info.operationId, info)
    app.jobTagToConnectOpId.put(info.jobTag, info.operationId)
    logDebug(s"Connect operation started: ${info.operationId} " +
      s"session=${info.sessionId} user=${info.userId}")
  }

  private def handleOperationAnalyzed(app: AppBase, event: SparkListenerEvent): Unit = {
    updateOperation(app, event) { op =>
      op.analyzeTime = Some(getLong(event, "eventTime"))
    }
  }

  private def handleOperationReadyForExecution(app: AppBase, event: SparkListenerEvent): Unit = {
    updateOperation(app, event) { op =>
      op.readyForExecTime = Some(getLong(event, "eventTime"))
    }
  }

  private def handleOperationFinished(app: AppBase, event: SparkListenerEvent): Unit = {
    updateOperation(app, event) { op =>
      op.finishTime = Some(getLong(event, "eventTime"))
      op.producedRowCount = getOptLong(event, "producedRowCount")
    }
  }

  private def handleOperationClosed(app: AppBase, event: SparkListenerEvent): Unit = {
    updateOperation(app, event) { op =>
      op.closeTime = Some(getLong(event, "eventTime"))
    }
  }

  private def handleOperationFailed(app: AppBase, event: SparkListenerEvent): Unit = {
    updateOperation(app, event) { op =>
      op.errorMessage = Some(getString(event, "errorMessage"))
    }
  }

  private def handleOperationCanceled(app: AppBase, event: SparkListenerEvent): Unit = {
    updateOperation(app, event) { op =>
      op.isCanceled = true
    }
  }

  /**
   * Looks up the ConnectOperationInfo for the event's operationId and applies
   * the update function. Logs a warning if the operation wasn't found (e.g.,
   * OperationStarted was missing or events arrived out of order).
   */
  private def updateOperation(app: AppBase, event: SparkListenerEvent)(
      f: ConnectOperationInfo => Unit): Unit = {
    val opId = getString(event, "operationId")
    app.connectOperations.get(opId) match {
      case Some(op) => f(op)
      case None =>
        logDebug(s"Connect operation $opId not found for " +
          s"${event.getClass.getSimpleName} event (OperationStarted may be missing)")
    }
  }

  // Reflective field accessors are delegated to EventUtils which provides
  // generic cached method invocation (getString, getLong, getOptLong).
  // This avoids duplicating the reflection/caching logic.

  private def getString(event: SparkListenerEvent, methodName: String): String =
    EventUtils.getStringFromEvent(event, methodName)

  private def getLong(event: SparkListenerEvent, methodName: String): Long =
    EventUtils.getLongFromEvent(event, methodName)

  private def getOptLong(event: SparkListenerEvent, methodName: String): Option[Long] =
    EventUtils.getOptLongFromEvent(event, methodName)
}
