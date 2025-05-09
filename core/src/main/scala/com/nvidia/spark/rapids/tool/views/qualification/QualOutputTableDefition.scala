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

package com.nvidia.spark.rapids.tool.views.qualification

/**
 * This file contains the definition of the tables that are used to generate the
 * qualification report.
 *
 * The definition of the tables is stored in a YAML file. The YAML file is
 * expected to have the following structure:
 *
 * `qualTableDefinitions`:
 *   - `label`: The label of the table.
 *   - `description`: The description of the table.
 *   - `fileName`: The name of the file where the table should be written.
 *   - `scope`: The scope of the table. This can be either "global" or "per-app".
 *   - `columns`: The columns of the table. Each column is represented as a
 *     `QualOutputTableColumn` object.
 *
 * The main function `loadConfig` is used to load the YAML file and parse it
 * into a `QualTableYaml` object.
 *
 * The `generateMarkdown` function can be used to generate a markdown file
 * that contains the documentation for the tables.
 */

case class QualOutputTableColumn(
    name: String,
    dataType: String,
    description: String)

case class QualOutputTableDefinition(
    label: String,
    description: Option[String],
    fileName: String,
    fileFormat: String,
    scope: String,
    columns: Seq[QualOutputTableColumn]) {
  def isPerApp : Boolean = scope == "per-app"
  def isGlobal: Boolean = scope == "global"
}

case class QualTableYaml(qualTableDefinitions: Seq[QualOutputTableDefinition]) {
  def generateMarkdown(outputPath: String): Unit = {
    val sb = new StringBuilder

    sb.append("# Tables and Columns\n\n")

    qualTableDefinitions.foreach { definition =>
      sb.append(s"## ${definition.label}\n")
      sb.append(s"_Scope_: ${definition.scope}\n\n")
      sb.append(s"_Description_: ${definition.description.getOrElse("N/A")}\n")
      sb.append("| Column Name | Description |\n")
      sb.append("|---|---|\n")

      definition.columns.foreach { col =>
        sb.append(s"| ${col.name} | ${col.description} |\n")
      }
      sb.append("\n")
    }

    import java.nio.file.{Files, Paths}
    import java.nio.charset.StandardCharsets

    Files.write(Paths.get(outputPath), sb.toString().getBytes(StandardCharsets.UTF_8))
  }
}
