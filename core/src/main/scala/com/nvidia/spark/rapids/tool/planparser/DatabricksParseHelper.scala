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

import scala.util.control.NonFatal

import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

import org.apache.spark.internal.Logging

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
    if (splitArr.contains("job")) {
      val jobIdx = splitArr.indexOf("job")
      // indexes are 0 based so adjust to compare to length
      if (splitArr.length > jobIdx + 1) {
        jobId = Some(splitArr(jobIdx + 1))
      }
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
}
