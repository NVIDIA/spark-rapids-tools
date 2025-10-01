/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION.
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

import scala.jdk.CollectionConverters._

import com.nvidia.spark.rapids.tool.planparser.HiveParseHelper
import com.nvidia.spark.rapids.tool.planparser.delta.DeltaLakeHelper
import com.nvidia.spark.rapids.tool.planparser.iceberg.IcebergHelper
import com.nvidia.spark.rapids.tool.profiling.ProfileUtils

import org.apache.spark.scheduler.{SparkListenerEnvironmentUpdate, SparkListenerJobStart, SparkListenerLogStart}
import org.apache.spark.sql.rapids.tool.AppEventlogProcessException
import org.apache.spark.util.Utils.REDACTION_REPLACEMENT_TEXT


/**
 * SparkRuntime enumeration is used to identify the specific runtime environment
 * in which the application is being executed.
 */
object SparkRuntime extends Enumeration {
  type SparkRuntime = Value

  /**
   * Represents the default Apache Spark runtime environment.
   */
  val SPARK: SparkRuntime = Value

  /**
   * Represents the Spark RAPIDS runtime environment.
   */
  val SPARK_RAPIDS: SparkRuntime = Value

  /**
   * Represents the Photon runtime environment on Databricks.
   */
  val PHOTON: SparkRuntime = Value

  /**
   * Returns the SparkRuntime value based on the given parameters.
   * @param isPhoton Boolean flag indicating whether the application is running on Photon.
   * @param isGpu    Boolean flag indicating whether the application is running on GPU.
   * @return
   */
  def getRuntime(isPhoton: Boolean, isGpu: Boolean): SparkRuntime.SparkRuntime = {
    if (isPhoton) {
      PHOTON
    } else if (isGpu) {
      SPARK_RAPIDS
    } else {
      SPARK
    }
  }
}

// Handles updating and caching Spark Properties for a Spark application.
// Properties stored in this container can be accessed to make decision about certain analysis
// that depends on the context of the Spark properties.
trait CacheablePropsHandler {
  /**
   * Important system properties that should be retained.
   */
  protected def getRetainedSystemProps: Set[String] = Set(
    "file.encoding", "java.version", "os.arch", "os.name",
    "os.version", "sun.java.command", "user.timezone")

  // Patterns to be used to redact sensitive values from Spark Properties.
  private val REDACTED_PROPERTIES = Set[String](
    // S3
    "spark.hadoop.fs.s3a.secret.key",
    "spark.hadoop.fs.s3a.access.key",
    "spark.hadoop.fs.s3a.session.token",
    "spark.hadoop.fs.s3a.encryption.key",
    "spark.hadoop.fs.s3a.bucket.nightly.access.key",
    "spark.hadoop.fs.s3a.bucket.nightly.secret.key",
    "spark.hadoop.fs.s3a.bucket.nightly.session.token",
    // ABFS
    "spark.hadoop.fs.azure.account.oauth2.client.secret",
    "spark.hadoop.fs.azure.account.oauth2.client.id",
    "spark.hadoop.fs.azure.account.oauth2.refresh.token",
    "spark.hadoop.fs.azure.account.key\\..*",
    "spark.hadoop.fs.azure.account.auth.type\\..*",
    // GCS
    "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
    "spark.hadoop.fs.gs.auth.client.id",
    "spark.hadoop.fs.gs.encryption.key",
    "spark.hadoop.fs.gs.auth.client.secret",
    "spark.hadoop.fs.gs.auth.refresh.token",
    "spark.hadoop.fs.gs.auth.impersonation.service.account.for.user\\..*",
    "spark.hadoop.fs.gs.auth.impersonation.service.account.for.group\\..*",
    "spark.hadoop.fs.gs.auth.impersonation.service.account",
    "spark.hadoop.fs.gs.proxy.username",
    // matches on any key that contains password in it.
    "(?i).*password.*"
  )

  // caches the spark-version from the eventlogs
  var sparkVersion: String = ""
  // A flag to indicate whether the eventlog is an eventlog with Spark RAPIDS runtime.
  var gpuMode = false
  // A flag to indicate whether the eventlog is an eventlog from Photon runtime.
  var isPhoton = false
  // A flag whether hive is enabled or not. Note that we assume that the
  // property is global to the entire application once it is set. a.k.a, it cannot be disabled
  // once it was set to true.
  var hiveEnabled = false
  // A flag to indicate whether the spark App is configured to use Iceberg.
  // Note that this is only a best-effort flag based on the spark properties.
  var icebergEnabled = false
  // A flag to indicate whether the spark App is configured to use DeltaLake.
  // Note that this is only a best-effort flag based on the spark properties.
  var deltaLakeEnabled = false
  // Indicates the ML eventlogType (i.e., Scala or pyspark). It is set only when MLOps are detected.
  // By default, it is empty.
  var mlEventLogType = ""
  // A flag to indicate that the eventlog is ML
  var pysparkLogFlag = false

  var sparkProperties = Map[String, String]()
  var classpathEntries = Map[String, String]()
  // set the fileEncoding to UTF-8 by default
  var systemProperties = Map[String, String]()

  private def processPropKeys(srcMap: Map[String, String]): Map[String, String] = {
    // Redact the sensitive values in the given map.
    val redactedKeys = REDACTED_PROPERTIES.collect {
      case rK if srcMap.keySet.exists(_.matches(rK)) => rK -> REDACTION_REPLACEMENT_TEXT
    }
    srcMap ++ redactedKeys
  }

  /**
   * Used to validate that the eventlog is allowed to be processed by the Tool
   * @throws org.apache.spark.sql.rapids.tool.AppEventlogProcessException if the eventlog fails the
   *                                                                      validation step
   */
  @throws(classOf[AppEventlogProcessException])
  def validateAppEventlogProperties(): Unit = { }

  def updatePredicatesFromSparkProperties(): Unit = {
    gpuMode ||= ProfileUtils.isPluginEnabled(sparkProperties)
    icebergEnabled ||= IcebergHelper.isIcebergEnabled(sparkProperties)
    deltaLakeEnabled ||= DeltaLakeHelper.isDeltaLakeEnabled(sparkProperties)
    hiveEnabled ||= HiveParseHelper.isHiveEnabled(sparkProperties)
  }

  def handleEnvUpdateForCachedProps(event: SparkListenerEnvironmentUpdate): Unit = {
    sparkProperties ++= processPropKeys(event.environmentDetails("Spark Properties").toMap)
    classpathEntries ++= event.environmentDetails("Classpath Entries").toMap

    updatePredicatesFromSparkProperties()

    // Update the properties if system environments are set.
    // No need to capture all the properties in memory. We only capture important ones.
    systemProperties ++= event.environmentDetails("System Properties").toMap.filterKeys(
      getRetainedSystemProps.contains(_))

    // After setting the properties, validate the properties.
    validateAppEventlogProperties()
  }

  def handleJobStartForCachedProps(event: SparkListenerJobStart): Unit = {
    // TODO: we need to improve this in order to support per-job-level
    icebergEnabled ||= IcebergHelper.isIcebergEnabled(event.properties.asScala)
    deltaLakeEnabled ||= DeltaLakeHelper.isDeltaLakeEnabled(event.properties.asScala)
    hiveEnabled ||= HiveParseHelper.isHiveEnabled(event.properties.asScala)
  }

  def handleLogStartForCachedProps(event: SparkListenerLogStart): Unit = {
    sparkVersion = event.sparkVersion
  }

  def isGPUModeEnabledForJob(event: SparkListenerJobStart): Boolean = {
    gpuMode || ProfileUtils.isPluginEnabled(event.properties.asScala)
  }

  /**
   * Returns the SparkRuntime environment in which the application is being executed.
   * This is calculated based on other cached properties.
   */
  def getSparkRuntime: SparkRuntime.SparkRuntime = {
    SparkRuntime.getRuntime(isPhoton, gpuMode)
  }
}
