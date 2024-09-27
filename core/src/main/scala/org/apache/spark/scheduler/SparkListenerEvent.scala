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

package org.apache.spark.scheduler

import com.fasterxml.jackson.annotation.JsonTypeInfo

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.rapids.tool.annotation.ToolsReflection

/**
 * This code is mostly copied from org.apache.spark.scheduler.SparkListenerEvent
 * to make it compatible at runtime with custom Spark implementation that defines abstract methods
 * in the trait.
 */
@ToolsReflection("BD-3.2.1", "Tools jar needs to come first in the classpath")
@DeveloperApi
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "Event")
trait SparkListenerEvent {
  /* Whether output this event to the event log */
  protected[spark] def logEvent: Boolean = true
}
