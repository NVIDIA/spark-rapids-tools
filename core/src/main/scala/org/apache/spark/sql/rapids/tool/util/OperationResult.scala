/*
 * Copyright (c) 2021-2023, NVIDIA CORPORATION.
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
import org.apache.spark.sql.rapids.tool.qualification.QualificationAppInfo

sealed trait OperationResult[+T]
case class SuccessResult[T](result: T) extends OperationResult[T]
case class FailureResult(errorMessage: String) extends OperationResult[Nothing]

class QualAppResult(path: String, message: String)
  extends OperationResult[QualificationAppInfo] with Logging {
  def logMessage(exp: Option[Exception] = None): Unit = {
    val messageToLog = s"File: $path, Message: $message"
    exp match {
      case Some(e) => logWarning(messageToLog, e)
      case None    => logWarning(messageToLog)
    }
  }
}
case class SuccessQualAppResult(
    path: String,
    appId: String,
    message: String = "") extends QualAppResult(path, message) {
  override def logMessage(exp: Option[Exception] = None): Unit = {
    logInfo(s"File: $path, Message: $message")
  }
}
case class FailureQualAppResult(path: String, message: String)
  extends QualAppResult(path, message) {}
case class UnknownQualAppResult(path: String, appId: String, message: String)
  extends QualAppResult(path, message) {}
