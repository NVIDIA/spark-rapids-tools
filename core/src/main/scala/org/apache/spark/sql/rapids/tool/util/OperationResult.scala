/*
 * Copyright (c) 2021-2024, NVIDIA CORPORATION.
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
class AppResult(path: String, message: String) extends Logging {
  /**
   * Logs the message along with an optional exception, if provided.
   */
  def logMessage(exp: Option[Exception] = None): Unit = {
    val messageToLog = s"File: $path, Message: $message"
    exp match {
      case Some(e) => logWarning(messageToLog, e)
      case None    => logWarning(messageToLog)
    }
  }
}

case class SuccessAppResult(
    path: String,
    appId: String,
    message: String = "") extends AppResult(path, message) {
  override def logMessage(exp: Option[Exception] = None): Unit = {
    logInfo(s"File: $path, Message: $message")
  }
}

case class FailureAppResult(path: String, message: String)
  extends AppResult(path, message) {}

case class UnknownAppResult(path: String, appId: String, message: String)
  extends AppResult(path, message) {}

case class SkippedAppResult(path: String, message: String)
  extends AppResult(path, message) {}
