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

package org.apache.spark.sql.rapids.tool.util.stubs

/**
 * A trait that provides platform-specific information for execution plans.
 *
 * This trait is designed to be mixed into classes that represent execution plans
 * which need to be aware of the underlying platform they are running on. This is
 * particularly useful in the context of RAPIDS Spark tools where execution plans
 * may need to differentiate behavior or provide information based on the specific
 * platform (e.g., Databricks, EMR, Dataproc, on-premises).
 *
 * Implementing classes should provide concrete implementations of the platform
 * name and description to identify the execution environment.
 */
trait PlatformAwarePlanTrait {
  /**
   * The actual name that belongs to the platform's entity. For example, in Photon plans,
   * this would be the Photon-specific operator name. For spark, it would return the same operator
   * name.
   *
   * @return A string identifier for the platform's entity.
   */
  def platformName: String

  /**
   * A human-readable description of the platform's entity. Same as the platformName, it represents
   * the true name as per the platform's terminology. Oter wise, it should provide the same details
   * as Spark OSS. i.e., simpleString for planInfo, and nodeDesc for planGraphNode.
   *
   * @return A string representing the description of the platform's entity.
   */
  def platformDesc: String
}
