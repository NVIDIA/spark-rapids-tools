/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

import java.util

import scala.beans.BeanProperty
import scala.collection.JavaConverters._

class EventToolsConf(
    // The simple class name of the tool
    @BeanProperty var className: String,
    // A list of events supported by the tool
    @BeanProperty var supportedEvents: util.List[String]) {
  def this() = this("N/A", new java.util.ArrayList[String]())
}

class EventLogReaderConf(
    // A map between eventLabel and its string representation in Spark.
    // Some events in spark are identified by their simpleClassName,
    // while the majority are using the fully qualified class names.
    @BeanProperty var eventsTable: util.LinkedHashMap[String, String],
    // A list of tools configuration and their supported events
    @BeanProperty var toolsConfig: util.List[EventToolsConf]) extends ValidatableProperties {
  def this() = this(
    new util.LinkedHashMap[String, String](),
    new java.util.ArrayList[EventToolsConf]()
  )

  override def validate(): Unit = {
    // TODO: Add any validation logic if needed
  }

  def getSupportedEvents(toolName: String): List[String] = {
    toolsConfig.asScala.find(_.className == toolName)
      .map(_.supportedEvents).getOrElse(new java.util.ArrayList[String]())
      .asScala
      .map(eventsTable.get(_)).filter(_ != null).toList
  }
}

/**
 * Utility class to load configurations of the eventlog readers.
 * The configuration file is a yaml file that contains the following information:
 * - list of events handled by the event processor.
 * - for each tool, define a list of supported events.
 *   If the list is empty, the tool supports all events.
 */
object EventLogReaderConf {
  // any event starts with the following string
  private val EVENT_PREFIX = "{\"Event\":\""
  private val EVENT_SUFFIX = "\","

  lazy val conf: EventLogReaderConf = {
    val yamlSource = UTF8Source.fromResource("parser/eventlog-parser.yaml").mkString
    PropertiesLoader[EventLogReaderConf].loadFromContent(yamlSource).get
  }

  private def getEventLineStart(evName: String): String = {
    // Typically, any line starts with {\"Event\":\"SparkListenerStageSubmitted\","
    s"$EVENT_PREFIX$evName$EVENT_SUFFIX"
  }

  /**
   * For a given tool name, returns a list of strings representing the starting lines
   * of the events accepted by the tool.
   * @param toolName the simple class name of the tool
   * @return a list of strings
   */
  def getAcceptedLines(toolName: String): List[String] = {
    conf.getSupportedEvents(toolName).map(getEventLineStart)
  }
}
