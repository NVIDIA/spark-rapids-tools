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

package org.apache.spark.sql.rapids.tool.util.stubs

import org.apache.spark.sql.rapids.tool.annotation.ToolsReflection

/**
 * Base trait for events related to SparkRapids build info. This used as a stub to be compatible in
 * runtime with custom Spark implementations that define abstract methods in the trait.
 */
trait ToolsSparkListenerEvent {
  @ToolsReflection("BD-3.2.1",
    "Ignore the implementation: The definition is to needed to override abstract field the Trait.")
  val eventTime: Long = 0
  @ToolsReflection("BD-3.2.1",
    "Ignore the implementation: The definition is to needed to override abstract field the Trait.")
  val eventType: String = ""
}
