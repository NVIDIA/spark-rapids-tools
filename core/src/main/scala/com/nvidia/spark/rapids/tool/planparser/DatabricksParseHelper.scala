/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.tool.planparser

import java.nio.file.Paths
import java.util.concurrent.TimeUnit

import scala.util.control.NonFatal
import scala.util.matching.Regex

import com.nvidia.spark.rapids.tool.profiling.SQLAccumProfileResults
import com.nvidia.spark.rapids.tool.views.IoMetrics
import org.json4s.{DefaultFormats, Formats}
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.JsonMethods.parse

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.UnsupportedMetricNameException
import org.apache.spark.sql.rapids.tool.util.UTF8Source

// Utilities used to handle Databricks and Photon Ops
object DatabricksParseHelper extends Logging {
  // A photon app is identified using the following properties from spark properties.
  private val PHOTON_SPARK_PROPS = Map(
    "spark.databricks.clusterUsageTags.sparkVersion" -> ".*-photon-.*",
    "spark.databricks.clusterUsageTags.effectiveSparkVersion" -> ".*-photon-.*",
    "spark.databricks.clusterUsageTags.sparkImageLabel" -> ".*-photon-.*",
    "spark.databricks.clusterUsageTags.runtimeEngine" -> "PHOTON"
  )
  val PROP_ALL_TAGS_KEY = "spark.databricks.clusterUsageTags.clusterAllTags"
  val PROP_TAG_CLUSTER_ID_KEY = "spark.databricks.clusterUsageTags.clusterId"
  val PROP_TAG_CLUSTER_NAME_KEY = "spark.databricks.clusterUsageTags.clusterName"
  val PROP_TAG_CLUSTER_SPARK_VERSION_KEY = "spark.databricks.clusterUsageTags.sparkVersion"
  val PROP_WORKER_TYPE_ID_KEY = "spark.databricks.workerNodeTypeId"
  val PROP_DRIVER_TYPE_ID_KEY = "spark.databricks.driverNodeTypeId"

  val SUB_PROP_CLUSTER_ID = "ClusterId"
  val SUB_PROP_JOB_ID = "JobId"
  val SUB_PROP_RUN_NAME = "RunName"

  // Labels for Photon metrics that
  val PHOTON_METRIC_CUMULATIVE_TIME_LABEL = "cumulative time"
  val PHOTON_METRIC_PEAK_MEMORY_LABEL = "peak memory usage"
  val PHOTON_METRIC_SHUFFLE_WRITE_TIME_LABEL = "part of shuffle file write"

  private val PHOTON_PATTERN: Regex = "Photon[a-zA-Z]*".r
  private val PHOTON_OPS_MAPPING_DIR = "photonOperatorMappings"
  // TODO: Create separate mapping file for different Photon/Databricks versions
  private val DEFAULT_PHOTON_OPS_MAPPING_FILE = "databricks-13_3.json"
  /**
   * Checks if the properties indicate that the application is a Photon app.
   * This ca be checked by looking for keywords in one of the keys defined in PHOTON_SPARK_PROPS
   *
   * @param properties spark properties captured from the eventlog environment details
   * @return true if the properties indicate that it is a photon app.
   */
  def isPhotonApp(properties: collection.Map[String, String]): Boolean = {
    PHOTON_SPARK_PROPS.exists { case (key, value) =>
      properties.get(key).exists(_.matches(value))
    }
  }

  def getSparkVersion(properties: collection.Map[String, String]): String = {
    properties.getOrElse(PROP_TAG_CLUSTER_SPARK_VERSION_KEY, "")
  }

  /**
   * Try to get the JobId from the cluster name. Parse the clusterName string which
   * looks like:
   * "spark.databricks.clusterUsageTags.clusterName":"job-***-run-****"
   * and look for job-XXXXX where XXXXX represents the JobId.
   *
   * @param clusterNameString String which contains property clusterUsageTags.clusterName
   * @return Optional JobId if found
   */
  def parseClusterNameForJobId(clusterNameString: String): Option[String] = {
    var jobId: Option[String] = None
    val splitArr = clusterNameString.split("-")
    val jobIdx = splitArr.indexOf("job")
    if (jobIdx != -1 && splitArr.length > jobIdx + 1) {
      jobId = Some(splitArr(jobIdx + 1))
    }
    jobId
  }

  /**
   * Parses the string which contains configs in JSON format ( key : value ) pairs and
   * returns the Map of [String, String]
 *
   * @param clusterTag  String which contains property clusterUsageTags.clusterAllTags in
   *                    JSON format
   * @return Map of ClusterTags
   */
  def parseClusterTags(clusterTag: String): Map[String, String] = {
    // clusterTags will be in this format -
    // [{"key":"Vendor","value":"Databricks"},
    // {"key":"Creator","value":"abc@company.com"},{"key":"ClusterName",
    // "value":"job-215-run-1"},{"key":"ClusterId","value":"0617-131246-dray530"},
    // {"key":"JobId","value":"215"},{"key":"RunName","value":"test73longer"},
    // {"key":"DatabricksEnvironment","value":"workerenv-7026851462233806"}]

    // case class to hold key -> value pairs
    case class ClusterTags(key: String, value: String)
    implicit val formats = DefaultFormats
    try {
      val listOfClusterTags = parse(clusterTag)
      val clusterTagsMap = listOfClusterTags.extract[List[ClusterTags]].map(
        x => x.key -> x.value).toMap
      clusterTagsMap
    } catch {
      case NonFatal(_) =>
        logWarning(s"There was an exception parsing cluster tags string: $clusterTag, skipping")
        Map.empty
    }
  }

  /**
   * Maps the Photon operator names to Spark operator names using a mapping JSON file.
   */
  private lazy val photonToSparkMapping: Map[String, String] = {
    val mappingFile = Paths.get(PHOTON_OPS_MAPPING_DIR, DEFAULT_PHOTON_OPS_MAPPING_FILE).toString
    val jsonString = UTF8Source.fromResource(mappingFile).mkString
    val json = JsonMethods.parse(jsonString)
    // Implicitly define JSON formats for deserialization using DefaultFormats
    implicit val formats: Formats = DefaultFormats
    // Extract and deserialize the JValue object into a Map[String, String]
    // TODO: Currently, only the first mapping in the list is used.
    //       This limitation exists because we cannot differentiate between
    //       these operators in the SparkPlan.
    json.extract[Map[String, List[String]]].mapValues(_.head)
  }

  def isPhotonNode(nodeName: String): Boolean = PHOTON_PATTERN.findFirstIn(nodeName).isDefined

  /**
   * Replaces all occurrences in the input string that match the PHOTON_PATTERN
   * with corresponding values from the photonToSparkMapping map.
   *
   * @param inputStr the node name, potentially containing a Photon identifier
   * @return an `Option[String]` with the Spark node name, or `None` if no match is found
   */
  def mapPhotonToSpark(inputStr: String): String = {
    PHOTON_PATTERN.replaceAllIn(inputStr, m => photonToSparkMapping.getOrElse(m.matched, m.matched))
  }

  /**
   * Checks if 'accum' is a Photon I/O metric.
   */
  def isPhotonIoMetric(accum: SQLAccumProfileResults): Boolean =
    accum.name == PHOTON_METRIC_CUMULATIVE_TIME_LABEL && accum.nodeName.contains("Scan")

  /**
   * Updates the I/O metrics for Photon apps based on the accumulator values.
   */
  def updatePhotonIoMetric(accum: SQLAccumProfileResults, ioMetrics: IoMetrics): Unit = {
    accum.name match {
      case PHOTON_METRIC_CUMULATIVE_TIME_LABEL if accum.nodeName.contains("Scan") =>
        ioMetrics.scanTime = TimeUnit.NANOSECONDS.toMillis(accum.total)
      case _ => throw UnsupportedMetricNameException(accum.name)
    }
  }
}
