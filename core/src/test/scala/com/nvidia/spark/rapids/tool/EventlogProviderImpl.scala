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

package com.nvidia.spark.rapids.tool

import java.io.{File, FileNotFoundException}
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import com.nvidia.spark.rapids.tool.ToolTestUtils.listFilesMatching

import org.apache.spark.sql.{DataFrame, SparkSession, TrampolineUtil}

/**
 * A class the implements EventlogProviderTrait. It adopts the builder pattern to allow
 * flexibility in driving the content of the eventlog.
 * The implementation accepts functions to generate the eventlog content.
 * @param descr description of the context. this is used to identify the purpose of the eventlog
 *              instead of relying on comments in the source code.
 */
case class EventlogProviderImpl(descr: String) extends EventlogProviderTrait {
  // Configuration used to start the spark session/application
  var sparkConfigs: Option[Map[String, String]] = None
  // whether is hive eneabled
  private var hiveEnabled: Boolean = false
  // The function passed to create the app and generate the DF. It references the provider object
  // in order to allow the function to access some information about the context of the test.
  // TODO: this can be merged into the contentGenerators since the latter is more generic.
  private var selfRefFunc: Option[(EventlogProviderImpl, SparkSession) => DataFrame] = None
  // A sequence of functions to generate the content of the eventlog.
  private var contentGenerators: Seq[EventlogProviderImpl => EventlogContentGenMeta] = Seq.empty

  // the work dir of the running test.
  private var _rootDir: Option[File] = None
  def rootDir: Option[File] = _rootDir
  def rootDir_=(value: Option[File]): Unit = {
    _rootDir = value
  }

  // the log directory where the eventlog is generated
  private var _logDir: Option[File] = None
  def logDir: Option[File] = _logDir
  def logDir_=(value: Option[File]): Unit = {
    _logDir = value
  }

  // The spark application name
  private var _appName: Option[String] = None
  def appName: Option[String] = _appName
  def appName_=(value: Option[String]): Unit = {
    _appName = value
  }
  // the appId which is set by Spark
  var appId: String = "UNKNOWN"
  // the resulting eventlog.
  var eventlog: String = "UNKNOWN"

  def withContentGenerator(
    f: EventlogProviderImpl => EventlogContentGenMeta): EventlogProviderImpl = {
    contentGenerators :+= f
    this
  }

  // add multiple content generators
  def withContentGenerators(
    fns: Seq[EventlogProviderImpl => EventlogContentGenMeta]): EventlogProviderImpl = {
    contentGenerators ++= fns
    this
  }

  def withAppName(name: String): EventlogProviderImpl = {
    appName = Some(name)
    this
  }

  def withFunc(f: (EventlogProviderImpl, SparkSession) => DataFrame): EventlogProviderImpl = {
    selfRefFunc = Some(f)
    this
  }

  def withHiveEnabled(): EventlogProviderImpl = {
    hiveEnabled = true
    this
  }

  def withSparkConfig(confKey: String, confValue: String): EventlogProviderImpl = {
    sparkConfigs =
      Some(sparkConfigs.getOrElse(Map.empty[String, String]) ++ Map(confKey -> confValue))
    this
  }

  def withSparkConfigs(configs: Map[String, String]): EventlogProviderImpl = {
    sparkConfigs = Some(configs)
    this
  }

  def withAppID(providedAppID: String): EventlogProviderImpl = {
    this.appId = providedAppID
    this
  }

  def isHiveEnabled: Boolean = {
    this.hiveEnabled
  }

  override def build(rootDirectory: File): EventlogProviderImpl = {
    rootDir = Some(rootDirectory)
    if (logDir.isEmpty) {
      // create the logDir
      val eventlogdir = new File(rootDir.get, "logDir")
      if (!eventlogdir.exists()) {
        eventlogdir.mkdirs()
      }
      _logDir = Some(eventlogdir)
    }
    this
  }

  override def doFire(): Unit = {
    val (evLogPath, appId) = generateEventLog()
    this.eventlog = evLogPath
    this.appId = appId
  }

  override def eventlogs: Array[String] = {
    if (contentGenerators.nonEmpty) {
      generateEventLogWithGenerators(logDir.get).toArray
    } else {
      doFire()
      Array(this.eventlog)
    }
  }

  private def generateEventLogWithSpark(eventLogDir: File): (String, String) = {
    // we need to close any existing sessions to ensure that we can
    // create a session with a new event log dir
    TrampolineUtil.cleanupAnyExistingSession()
    val eventlogDirAbsPath = eventLogDir.getAbsolutePath

    lazy val sparkBuilder = SparkSession
      .builder()
      .master("local[*]")
      .appName(this.appName.get)
      .config("spark.eventLog.enabled", "true")
      .config("spark.eventLog.dir", eventlogDirAbsPath)

    sparkConfigs.foreach(_.foreach { case (k, v) => sparkBuilder.config(k, v) })

    if (isHiveEnabled) {
      sparkBuilder.enableHiveSupport()
    }

    lazy val spark = sparkBuilder.getOrCreate()
    var newAppId = "UNKNOWN"
    try {
      // execute the query and generate events
      val df = selfRefFunc.get(this, spark)
      df.collect()
      newAppId = spark.sparkContext.applicationId
    } finally {
      // close the event log
      spark.close()
    }
    // find the event log
    val files = listFilesMatching(eventLogDir, !_.startsWith("."))
    if (files.length != 1) {
      throw new FileNotFoundException(s"Could not find event log in $eventlogDirAbsPath")
    }
    (files.head.getAbsolutePath, newAppId)
  }

  private def generateEventLogWithGenerators(eventLogDir: File): Seq[String] = {
    contentGenerators.map { gen =>
      val res = gen(this)
      val eventlogFile = new File(eventLogDir, res.applicationId)
      Files.write(eventlogFile.toPath, res.evLogCont.getBytes(StandardCharsets.UTF_8))
      res.modifiedTime.foreach { modTime =>
        eventlogFile.setLastModified(modTime)
      }
      eventlogFile.toString
    }
  }

  def generateEventLog(): (String, String) = {
    if (selfRefFunc.isDefined) {
      generateEventLogWithSpark(logDir.get)
    } else {
      throw new IllegalStateException("No event log generator function defined")
    }
  }
}
