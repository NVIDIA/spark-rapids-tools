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

package com.nvidia.spark.rapids.tool.planparser.config

import scala.beans.BeanProperty

/**
 * Configuration for a single SQL Plan Parser.
 * This class uses JavaBean properties for YAML deserialization:
 * - name: The name of the parser (e.g., "Auron", "Photon")
 * - className: The fully qualified class name of the parser implementation
 * - enabled: Whether the parser is enabled
 * - description: A description of the parser
 * - nodePattern: Optional regex pattern to identify nodes handled by this parser
 */
class ParserConfig(
    @BeanProperty var name: String,
    @BeanProperty var className: String,
    @BeanProperty var enabled: Boolean,
    @BeanProperty var description: String,
    @BeanProperty var nodePattern: String
) {

  def this() = this("", "", true, "", "")

  override def toString: String = {
    s"ParserConfig(name=$name, className=$className, enabled=$enabled, " +
      s"description=$description, nodePattern=$nodePattern)"
  }
}
