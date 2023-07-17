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
package com.nvidia.spark.rapids.tool.profiling

/**
 * Represents a platform and its associated recommendations.
 */
class Platform {
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
   *
   * @param sparkProperty The Spark property for which the recommendation is calculated.
   * @param args Variable list of arguments passed to the calculation function for dynamic
   *             processing.
   * @return Optional string containing the recommendation, or `None` if unavailable.
   */
  def getRecommendation(sparkProperty: String, args: Any*): Option[String] = None
}
class DatabricksPlatform extends Platform {
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
class DataprocPlatform extends Platform {}
class EmrPlatform extends Platform {}
class OnPremPlatform extends Platform {}