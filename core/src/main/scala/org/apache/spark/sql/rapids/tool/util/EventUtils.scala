/*
 * Copyright (c) 2023-2024, NVIDIA CORPORATION.
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

import java.lang.reflect.InvocationTargetException

import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import com.nvidia.spark.rapids.tool.profiling.TaskStageAccumCase
import org.json4s.jackson.JsonMethods.parse

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.AccumulableInfo
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart

/**
 * Utility containing the implementation of helpers used for parsing data from event.
 */
object EventUtils extends Logging {

  /**
   * Used to parse (value/update) fields of the AccumulableInfo object. If the data is not
   * a valid long, it tries to parse it as a duration in the format of "hour:mm:ss.SSS".
   *
   * @param data value stored in the (value/update) of the AccumulableInfo
   * @return valid parsed long of the content or the duration
   */
  @throws[NullPointerException]
  private def parseAccumFieldToLong(data: Any): Option[Long] = {
    val strData = data.toString
    try {
      Some(strData.toLong)
    } catch {
      case _ : NumberFormatException =>
        StringUtils.parseFromDurationToLongOption(strData)
      case NonFatal(_) =>
        None
    }
  }

  /**
   * Given AccumulableInfo object, this method tries to parse value/update fields into long.
   * It is common to have one of those two fields set to None. That's why, we are not skipping the
   * entire accumulable if one of those fields fail to parse.
   *
   * @param accuInfo object of AccumulableInfo to be processed
   * @param stageId the tageId to which the metric belongs
   * @param attemptId the ID of the current execution
   * @param taskId the task-id to which the metric belongs , if any.
   * @return option(TaskStageAccumCase) if art least one of the two fields can be parsed to Long.
   */
  def buildTaskStageAccumFromAccumInfo(accuInfo: AccumulableInfo,
      stageId: Int, attemptId: Int, taskId: Option[Long] = None): Option[TaskStageAccumCase] = {
    val value = accuInfo.value.flatMap(parseAccumFieldToLong)
    val update = accuInfo.update.flatMap(parseAccumFieldToLong)
    if (!(value.isDefined || update.isDefined)) {
      // we could not get any valid number from both value/update
      if (log.isDebugEnabled()) {
        if ((accuInfo.value.isDefined && value.isEmpty) ||
          (accuInfo.update.isDefined && update.isEmpty)) {
          // in this case we failed to parse
          logDebug(s"Failed to parse accumulable for stageId=$stageId, taskId=$taskId." +
            s"The problematic accumulable is: $accuInfo")
        }
      }
      // No need to return a new object to save memory consumption
      None
    } else {
      Some(TaskStageAccumCase(
        stageId, attemptId,
        taskId, accuInfo.id, accuInfo.name, value, update, accuInfo.internal))
    }
  }

  // A utility function used to read Spark properties and compare it to a given target.
  // Note that it takes a default argument as well in case the property is not available.
  def isPropertyMatch(properties: collection.Map[String, String], propKey: String,
      defValue: String, targetValue: String): Boolean = {
    properties.getOrElse(propKey, defValue).equals(targetValue)
  }


  // Reads the root execution ID from a SparkListenerSQLExecutionStart event using reflection.
  // Reflection is used here to maintain compatibility with different versions of Spark,
  // as the rootExecutionId field is introduced in Spark 3.4. This allows the
  // code to access the field dynamically at runtime, and maintaining backward compatibility.
  def readRootIDFromSQLStartEvent(event: SparkListenerSQLExecutionStart): Option[Long] = {
    Try(rootExecutionIdField.get(event).asInstanceOf[Option[Long]]).getOrElse(None)
  }

  private lazy val rootExecutionIdField = {
    val field = classOf[SparkListenerSQLExecutionStart].getDeclaredField("rootExecutionId")
    field.setAccessible(true)
    field
  }

  lazy val getEventFromJsonMethod:
    String => Option[org.apache.spark.scheduler.SparkListenerEvent] = {
    // Spark 3.4 and Databricks changed the signature on sparkEventFromJson
    // Note that it is preferred we use reflection rather than checking Spark-runtime
    // because some vendors may back-port features.
    val c = Class.forName("org.apache.spark.util.JsonProtocol")
    val m = Try {
      // versions prior to spark3.4
      c.getDeclaredMethod("sparkEventFromJson", classOf[org.json4s.JValue])
    } match {
      case Success(a) =>
        (line: String) =>
          a.invoke(null, parse(line)).asInstanceOf[org.apache.spark.scheduler.SparkListenerEvent]
      case Failure(_) =>
        // Spark3.4+ and databricks
        val b = c.getDeclaredMethod("sparkEventFromJson", classOf[String])
        (line: String) =>
          b.invoke(null, line).asInstanceOf[org.apache.spark.scheduler.SparkListenerEvent]
    }
    // At this point, the method is already defined.
    // Note that the Exception handling is moved within the method to make it easier
    // to isolate the exception reason.
    (line: String) => Try {
      m.apply(line)
    } match {
      case Success(i) => Some(i)
      case Failure(e) =>

        e match {
          case i: InvocationTargetException =>
            val targetEx = i.getTargetException
            if (targetEx != null) {
              targetEx match {
                case j: com.fasterxml.jackson.core.io.JsonEOFException =>
                  // Spark3.41+ embeds JsonEOFException in the InvocationTargetException
                  // We need to show a warning message instead of failing the entire app.
                  logWarning(s"Incomplete eventlog, ${j.getMessage}")
                case k: com.fasterxml.jackson.core.JsonParseException =>
                  // this is a parser error thrown by spark-3.4+ which indicates the log is
                  // malformed
                  throw k
                case z: ClassNotFoundException if z.getMessage != null =>
                  logWarning(s"ClassNotFoundException while parsing an event: ${z.getMessage}")
                case t: Throwable =>
                  // We do not want to swallow unknown exceptions so that we can handle later
                  logError(s"Unknown exception while parsing an event", t)
              }
            } else {
              // Normally it should not happen that invocation target is null.
              logError(s"Unknown exception while parsing an event", i)
            }
          case j: com.fasterxml.jackson.core.io.JsonEOFException =>
            // Note that JsonEOFException is child of JsonParseException
            // In case the eventlog is incomplete (i.e., inprogress), we show a warning message
            // because we do not want to cause the entire app to fail.
            logWarning(s"Incomplete eventlog, ${j.getMessage}")
          case k: com.fasterxml.jackson.core.JsonParseException =>
            // this is a parser error thrown by version prior to spark-3.4+ which indicates the
            // log is malformed
            throw k
        }
        None
    }
  }
}
