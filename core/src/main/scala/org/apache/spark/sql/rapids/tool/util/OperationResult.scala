/*
 * Copyright (c) 2021-2025, NVIDIA CORPORATION.
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

import org.apache.spark.internal.Logging

/**
 *  Represents a result specific to `AppBase` class. This is a base class for
 *  Success, Failure and Unknown types.
 */
abstract class AppResult(path: String, message: String) extends Logging {
  /**
   * Logs the message along with an optional exception, if provided.
   */
  def logMessage(exp: Option[Exception] = None): Unit = {
    val messageToLog = s"File: $path, Message: $message"
    exp match {
      case Some(e) => logWarning(messageToLog, e)
      case None => logWarning(messageToLog)
    }
  }

  val status: String
}

case class SuccessAppResult(
    path: String,
    appId: String,
    attemptId: Int,
    appName: String,
    message: String = "") extends AppResult(path, message) {
  override def logMessage(exp: Option[Exception] = None): Unit = {
    logInfo(s"[SUCCESS] File: $path, appName: $appName, appId: $appId, attemptId: $attemptId")
  }

  override val status: String = "SUCCESS"
}

case class FailureAppResult(path: String, message: String)
  extends AppResult(path, message) {
  override val status: String = "FAILURE"
}

case class UnknownAppResult(path: String, appId: String, message: String)
  extends AppResult(path, message) {
  override val status: String = "UNKNOWN"
}

case class SkippedAppResult(path: String, message: String)
  extends AppResult(path, message) {
  override val status: String = "SKIPPED"
}

object SkippedAppResult {
  def fromAppAttempt(path: String, appId: String, attemptId: Int): SkippedAppResult = {
    SkippedAppResult(path, s"For App ID '$appId'; skipping this " +
      s"attempt $attemptId as a newer attemptId is being processed")
  }
}
