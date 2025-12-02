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
package com.nvidia.spark.rapids.tool.plugins

/**
 * A condition that checks for the presence of specific Spark extensions
 * by matching property keys and regex patterns in the application properties.
 * The condition evaluates to true if any of the specified properties match their
 * corresponding regex patterns.
 */
trait PropConditionOnSparkExtTrait extends PropConditionTrait {
  /**
   * A map where keys are Spark extension property names and values are
   * regex patterns to match against the property values.
   */
  val extensionRegxMap: Map[String, String]

  /**
   * Evaluates the condition against the provided application properties.
   * @param props A map of application properties.
   * @return true if any property matches its corresponding regex pattern, false otherwise.
   */
  override def eval(props: collection.Map[String, String]): Boolean = {
    extensionRegxMap.exists { case (key, value) =>
      props.get(key).exists(_.matches(value))
    }
  }
}
