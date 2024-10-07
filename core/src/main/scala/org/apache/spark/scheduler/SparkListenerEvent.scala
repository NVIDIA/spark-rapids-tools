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

/**
 * This code is mostly copied from org.apache.spark.scheduler.SparkListenerEvent
 * to make it compatible at runtime with custom Spark implementation that defines abstract methods
 * in the trait.
 *
 * This class is packaged due to a bug in Scala 2.12 that links the method
 * to the abstract trait, which might not exist in the classpath.
 * See the related Scala issues:
 *   https://github.com/scala/bug/issues/10477
 *   https://github.com/scala/scala-dev/issues/219
 *   https://github.com/scala/scala-dev/issues/268
 */
@DeveloperApi
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "Event")
trait SparkListenerEvent extends ToolsListenerEventExtraAPIs {
  /* Whether output this event to the event log */
  protected[spark] def logEvent: Boolean = true
}
