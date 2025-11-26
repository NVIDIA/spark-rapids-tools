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

package com.nvidia.spark.rapids.tool.planparser.delta

import com.nvidia.spark.rapids.tool.plugins.{AppPropVersionExtractorTrait, BaseAppPropPlug, PropConditionTrait}

/**
 * Plugin implementation for Delta OSS usage in Spark applications.
 */
class DeltaLakeOSSPlugin(override val id: String) extends BaseAppPropPlug(id = id) {

  /**
   * Callback function to evaluate the property based on application properties.
   */
  override val propCondition: PropConditionTrait = DeltaLakeHelper
  /**
   * Callback function to extract the version of Delta OSS from application properties.
   */
  override val versionExtractor: Option[AppPropVersionExtractorTrait] = Some(DeltaLakeHelper)

}
