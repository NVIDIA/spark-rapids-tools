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

package org.apache.spark.sql.rapids.tool

import com.nvidia.spark.rapids.tool.planparser.DatabricksParseHelper

import org.apache.spark.scheduler.SparkListenerJobStart
import org.apache.spark.sql.rapids.tool.util.CacheablePropsHandler

// Maintains and handles cluster tags for a Spark application.
trait ClusterTagPropHandler extends CacheablePropsHandler {
  // clusterTags can be redacted so try to get ClusterId and ClusterName separately
  var clusterTags: String = ""
  var clusterTagClusterId: String = ""
  var clusterTagClusterName: String = ""

  // A flag to indicate whether the eventlog being processed is an eventlog from Photon.
  var isPhoton = false

  // Flag used to indicate that the App was a Databricks App.
  def isDatabricks: Boolean = {
    clusterTags.nonEmpty && clusterTagClusterId.nonEmpty && clusterTagClusterName.nonEmpty
  }

  private def updateClusterTagsFromSparkProperties(): Unit = {
    // Update the clusterTags if any.
    if (clusterTags.isEmpty) {
      clusterTags = sparkProperties.getOrElse(DatabricksParseHelper.PROP_ALL_TAGS_KEY, "")
    }
    if (clusterTagClusterId.isEmpty) {
      clusterTagClusterId =
        sparkProperties.getOrElse(DatabricksParseHelper.PROP_TAG_CLUSTER_ID_KEY, "")
    }
    if (clusterTagClusterName.isEmpty) {
      clusterTagClusterName =
        sparkProperties.getOrElse(DatabricksParseHelper.PROP_TAG_CLUSTER_NAME_KEY, "")
    }
    // update the photon Flag
    if (isDatabricks) {
      // update photonFlag
      isPhoton ||= DatabricksParseHelper.isPhotonApp(sparkProperties)
      // set the spark version
      sparkVersion = DatabricksParseHelper.getSparkVersion(sparkProperties)
    }
  }

  override def handleJobStartForCachedProps(event: SparkListenerJobStart): Unit = {
    super.handleJobStartForCachedProps(event)
    // If the confs are set after SparkSession initialization, it is captured in this event.
    val dbFlagPreprocess = isDatabricks
    if (!isDatabricks) {
      // Add the clusterTags to SparkProperties if any
      Seq(DatabricksParseHelper.PROP_ALL_TAGS_KEY,
          DatabricksParseHelper.PROP_TAG_CLUSTER_ID_KEY,
          DatabricksParseHelper.PROP_TAG_CLUSTER_NAME_KEY
      ).foreach { tagKey =>
        sparkProperties += (tagKey -> event.properties.getProperty(tagKey, ""))
      }
    }

    if (!dbFlagPreprocess && isDatabricks) {
      updateClusterTagsFromSparkProperties()
    }
  }

  override def updatePredicatesFromSparkProperties(): Unit = {
    super.updatePredicatesFromSparkProperties()
    updateClusterTagsFromSparkProperties()
  }

  protected def prepareClusterTags: Map[String, String] = {
    val initialClusterTagsMap = if (clusterTags.nonEmpty) {
      DatabricksParseHelper.parseClusterTags(clusterTags)
    } else {
      Map.empty[String, String]
    }

    val tagsMapWithClusterId =
      if (!initialClusterTagsMap.contains(DatabricksParseHelper.SUB_PROP_CLUSTER_ID)
          && clusterTagClusterId.nonEmpty) {
        initialClusterTagsMap + (DatabricksParseHelper.SUB_PROP_CLUSTER_ID -> clusterTagClusterId)
      } else {
        initialClusterTagsMap
      }

    if(!tagsMapWithClusterId.contains(DatabricksParseHelper.SUB_PROP_JOB_ID)
        && clusterTagClusterName.nonEmpty) {
      val clusterTagJobId = DatabricksParseHelper.parseClusterNameForJobId(clusterTagClusterName)
      clusterTagJobId.map { jobId =>
        tagsMapWithClusterId + (DatabricksParseHelper.SUB_PROP_JOB_ID -> jobId)
      }.getOrElse(tagsMapWithClusterId)
    } else {
      tagsMapWithClusterId
    }
  }
}
