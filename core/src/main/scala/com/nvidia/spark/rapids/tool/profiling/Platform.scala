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
package com.nvidia.spark.rapids.tool.profiling
abstract class Platform {
  val skipRecommendations: Seq[String]
  val includeRecommendations: Map[String, String]
}
class DatabricksPlatform extends Platform {
  val skipRecommendations: Seq[String] = Seq(
    "spark.executor.cores",
    "spark.executor.instances",
    "spark.executor.memory",
    "spark.executor.memoryOverhead"
  )

  val includeRecommendations: Map[String, String] = Map(
    "spark.databricks.optimizer.dynamicFilePruning" -> "false"
  )
}
class DataprocPlatform extends Platform {
  val skipRecommendations: Seq[String] = Seq.empty
  val includeRecommendations: Map[String, String] = Map.empty
}
class EmrPlatform extends Platform {
  val skipRecommendations: Seq[String] = Seq.empty
  val includeRecommendations: Map[String, String] = Map.empty
}
class OnPremPlatform extends Platform {
  val skipRecommendations: Seq[String] = Seq.empty
  val includeRecommendations: Map[String, String] = Map.empty
}