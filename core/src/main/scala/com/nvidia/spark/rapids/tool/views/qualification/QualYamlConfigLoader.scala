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

import scala.collection.JavaConverters._

import org.yaml.snakeyaml.{DumperOptions, Yaml}
import org.yaml.snakeyaml.representer.Representer

import org.apache.spark.sql.rapids.tool.util.UTF8Source

object QualYamlConfigLoader {
  def loadConfig(path: String): QualTableYaml = {
    val yamlSource = UTF8Source.fromResource(path).mkString
    val yaml = new Yaml()
    val representer = new Representer(new DumperOptions())
    representer.getPropertyUtils.setSkipMissingProperties(true)
    val data = yaml.loadAs(yamlSource, classOf[java.util.Map[String, Any]])
    val definitionsData =
      data.get("qualTableDefinitions")
        .asInstanceOf[java.util.List[java.util.Map[String, Any]]]
        .asScala

    val definitions = definitionsData.map { entry =>
      val label = entry.get("label").asInstanceOf[String]
      val description = Option(entry.get("description").asInstanceOf[String])
      val fileName = entry.get("fileName").asInstanceOf[String]
      val fileFormat = Option(entry.get("fileFormat").asInstanceOf[String]).getOrElse("CSV")
      val scope = entry.get("scope").asInstanceOf[String]

      val columnsData =
        entry.get("columns")
          .asInstanceOf[java.util.List[java.util.Map[String, Any]]]
          .asScala

      val columns = columnsData.map { col =>
        QualOutputTableColumn(
          name = col.get("name").asInstanceOf[String],
          dataType = col.get("dataType").asInstanceOf[String],
          description = col.get("description").asInstanceOf[String]
        )
      }
      QualOutputTableDefinition(label, description, fileName, fileFormat, scope, columns)
    }

    QualTableYaml(definitions)
  }

  def main(args: Array[String]): Unit = {
    val config = loadConfig(QualReportGenConfProvider.TABLES_CONFIG_PATH)
    // scalastyle:off println
    println(config)
    // scalastyle:on println
    config.generateMarkdown("qualOutputTable.md")
  }
}
