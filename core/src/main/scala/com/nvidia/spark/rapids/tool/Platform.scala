/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

import scala.annotation.tailrec

import org.apache.spark.internal.Logging

/**
 *  Utility object containing constants for various platform names.
 */
object PlatformNames {
  val DATABRICKS_AWS = "databricks-aws"
  val DATABRICKS_AZURE = "databricks-azure"
  val DATAPROC = "dataproc"
  val DATAPROC_GKE_L4 = "dataproc-gke-l4"
  val DATAPROC_GKE_T4 = "dataproc-gke-t4"
  val DATAPROC_L4 = "dataproc-l4"
  val DATAPROC_SL_L4 = "dataproc-serverless-l4"
  val DATAPROC_T4 = "dataproc-t4"
  val EMR = "emr"
  val EMR_A10 = "emr-a10"
  val EMR_T4 = "emr-t4"
  val ONPREM = "onprem"
  val DEFAULT: String = ONPREM

  /**
   * Return a list of all platform names.
   */
  def getAllNames: List[String] = List(
    DATABRICKS_AWS, DATABRICKS_AZURE, DATAPROC, DATAPROC_GKE_L4, DATAPROC_GKE_T4,
    DATAPROC_L4, DATAPROC_SL_L4, DATAPROC_T4, EMR, EMR_A10, EMR_T4, ONPREM
  )
}

/**
 * Represents a platform and its associated recommendations.
 *
 * @param platformName Name of the platform. See [[PlatformNames]] for supported platform names.
 */
class Platform(platformName: String) {
  /**
   * Recommendations to be excluded from the list of recommendations.
   * These have the highest priority.
   */
  val recommendationsToExclude: Seq[String] = Seq.empty
  /**
   * Recommendations to be included in the final list of recommendations.
   * These properties should be specific to the platform and not general Spark properties.
   * For example: "spark.databricks.optimizer.dynamicFilePruning" for the Databricks platform.
   *
   * Represented as a tuple of (propertyKey, propertyValue).
   */
  val recommendationsToInclude: Seq[(String, String)] = Seq.empty
  /**
   * Dynamically calculates the recommendation for a specific Spark property by invoking
   * the appropriate function based on `sparkProperty`.
   * TODO: Implement this function and integrate with existing code in AutoTuner
   *
   * @param sparkProperty The Spark property for which the recommendation is calculated.
   * @param args Variable list of arguments passed to the calculation function for dynamic
   *             processing.
   * @return Optional string containing the recommendation, or `None` if unavailable.
   */
  def getRecommendation(sparkProperty: String, args: Any*): Option[String] = None

  /**
   * Checks if the `property` is valid:
   * 1. It should not be in exclusion list
   *   OR
   * 2. It should be in the inclusion list
   */
  def isValidRecommendation(property: String): Boolean = {
    !recommendationsToExclude.contains(property) ||
      recommendationsToInclude.map(_._1).contains(property)
  }

  /**
   * Checks if the `comment` is valid:
   * 1. It should not have any property from the exclusion list
   */
  def isValidComment(comment: String): Boolean = {
    recommendationsToExclude.forall(excluded => !comment.contains(excluded))
  }

  def getName: String = platformName

  def getOperatorScoreFile: String = {
    s"operatorsScore-$platformName.csv"
  }
}

class DatabricksPlatform(platformType: String) extends Platform(platformType) {
  override val recommendationsToExclude: Seq[String] = Seq(
    "spark.executor.cores",
    "spark.executor.instances",
    "spark.executor.memory",
    "spark.executor.memoryOverhead"
  )
  override val recommendationsToInclude: Seq[(String, String)] = Seq(
    ("spark.databricks.optimizer.dynamicFilePruning", "false")
  )
}

class DataprocPlatform(platformType: String) extends Platform(platformType)

class EmrPlatform(platformType: String) extends Platform(platformType)

class OnPremPlatform extends Platform(PlatformNames.ONPREM)

/**
 * Factory for creating instances of different platforms.
 * This factory supports various platforms and provides methods for creating
 * corresponding platform instances.
 */
object PlatformFactory extends Logging {
  /**
   * Creates an instance of a platform based on the specified platform key.
   * If platform key is not defined, returns an instance of onprem platform.
   *
   * @param platformKey The key representing the desired platform.
   * @return An instance of the specified platform.
   */
  @throws[IllegalArgumentException]
  @tailrec
  def createInstance(platformKey: String = PlatformNames.DEFAULT): Platform = {
    platformKey match {
      case PlatformNames.DATABRICKS_AWS | PlatformNames.DATABRICKS_AZURE =>
        new DatabricksPlatform(platformKey)
      case PlatformNames.DATAPROC | PlatformNames.DATAPROC_T4 =>
        // if no GPU specified, then default to dataproc-t4 for backward compatibility
        new DataprocPlatform(PlatformNames.DATAPROC_T4)
      case PlatformNames.DATAPROC_L4 | PlatformNames.DATAPROC_SL_L4 |
           PlatformNames.DATAPROC_GKE_L4 | PlatformNames.DATAPROC_GKE_T4 =>
        new DataprocPlatform(platformKey)
      case PlatformNames.EMR | PlatformNames.EMR_T4 =>
        // if no GPU specified, then default to emr-t4 for backward compatibility
        new EmrPlatform(PlatformNames.EMR_T4)
      case PlatformNames.EMR_A10 => new EmrPlatform(PlatformNames.EMR_A10)
      case PlatformNames.ONPREM => new OnPremPlatform
      case p if p.isEmpty =>
        logInfo(s"Platform is not specified. Using ${PlatformNames.DEFAULT} " +
          "as default.")
        PlatformFactory.createInstance(PlatformNames.DEFAULT)
      case _ => throw new IllegalArgumentException(s"Unsupported platform: $platformKey. " +
        s"Options include ${PlatformNames.getAllNames.mkString(", ")}.")
    }
  }
}
