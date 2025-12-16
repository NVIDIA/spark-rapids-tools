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

import java.io.{BufferedReader, InputStreamReader}

import scala.beans.BeanProperty
import scala.collection.JavaConverters._
import scala.util.Try
import scala.util.control.NonFatal

import com.nvidia.spark.rapids.tool.planparser.SQLPlanParserTrait

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.util.{PropertiesLoader, ValidatableProperties}

/**
 * Root configuration class for SQL Plan Parsers.
 * This class will be instantiated from the `sql-plan-parsers.yaml` file using SnakeYAML.
 *
 * Example YAML format:
 * {{{
 * parsers:
 *   - name: Auron
 *     className: com.nvidia.spark.rapids.tool.planparser.auron.AuronPlanParser
 *     enabled: true
 *     description: Parser for Auron (Native Spark) execution nodes
 *     nodePattern: Native[A-Z][a-zA-Z]+|ConvertToNative
 *   - name: Photon
 *     className: com.nvidia.spark.rapids.tool.planparser.photon.PhotonPlanParser
 *     enabled: true
 *     description: Parser for Databricks Photon execution nodes
 *     nodePattern: Photon[A-Z][a-zA-Z]+
 * }}}
 *
 * @see [[org.apache.spark.sql.rapids.tool.util.PropertiesLoader]]
 */
class SQLPlanParserConfig(
    @BeanProperty var parsers: java.util.List[ParserConfig]
) extends ValidatableProperties with Logging {

  /** No-arg constructor required by SnakeYAML for deserialization. */
  def this() = this(new java.util.ArrayList[ParserConfig]())

  /**
   * Validates the parser configuration.
   * Checks for duplicate parser names and ensures all required fields are populated.
   * During YAML deserialization, this may be called with empty parsers list.
   */
  override def validate(): Unit = {
    // During YAML deserialization and trait initialization, parsers might be null or empty
    // We allow this and validation will be done explicitly after YAML loading
    if (parsers == null || parsers.isEmpty) {
      return  // Skip validation during construction
    }

    // Check for duplicate parser names
    val parserNames = parsers.asScala.map(_.name)
    val duplicateNames = parserNames.groupBy(identity).filter(_._2.size > 1).keys
    if (duplicateNames.nonEmpty) {
      throw new IllegalArgumentException(
        s"Duplicate parser names found: ${duplicateNames.mkString(", ")}")
    }

    // Validate each parser's required fields
    parsers.asScala.foreach { parser =>
      if (parser.name == null || parser.name.isEmpty) {
        throw new IllegalArgumentException("Parser name cannot be null or empty")
      }
      if (parser.className == null || parser.className.isEmpty) {
        throw new IllegalArgumentException(
          s"Parser className cannot be null or empty for parser: ${parser.name}")
      }
    }
  }

  /**
   * Get all enabled parsers from the configuration.
   */
  def getEnabledParsers: Seq[ParserConfig] = {
    parsers.asScala.filter(_.enabled).toSeq
  }

  override def toString: String = {
    s"SQLPlanParserConfig(parsers=${parsers.asScala.mkString("[", ", ", "]")})"
  }

  /**
   * Instantiates a parser from its configuration using reflection.
   * Handles both Scala objects (via MODULE$ field) and regular classes.
   *
   * @param parserConf The parser configuration containing className
   * @return Try containing the instantiated parser or failure
   */
  def instantiateParser(parserConf: ParserConfig): Try[SQLPlanParserTrait] = Try {
    val pObj = try {
      // For Scala objects, the compiled class name has a $ suffix
      // Try with $ first (for objects), then without (for classes)
      val clazz: Class[_] = try {
        Class.forName(parserConf.className + "$")
      } catch {
        case _: ClassNotFoundException => Class.forName(parserConf.className)
      }

      // For Scala objects (singletons), access the MODULE$ field to get the instance
      try {
        clazz.getField("MODULE$").get(null).asInstanceOf[SQLPlanParserTrait]
      } catch {
        case _: NoSuchFieldException =>
          // If MODULE$ doesn't exist, try to instantiate as a regular class
          clazz.getDeclaredConstructor().newInstance().asInstanceOf[SQLPlanParserTrait]
      }
    } catch {
      case NonFatal(e) =>
        throw new IllegalArgumentException(
          s"Failed to instantiate Parser $parserConf: ${e.getMessage}", e)
    }
    pObj
  }

  /**
   * Instantiates all enabled parsers from the configuration.
   * Logs warnings for parsers that fail to instantiate and returns only successful instances.
   *
   * @return Sequence of successfully instantiated parser instances
   */
  def getRegisteredParsers: Seq[SQLPlanParserTrait] = {
    getEnabledParsers.flatMap { config =>
      instantiateParser(config) match {
        case scala.util.Success(parser) => Some(parser)
        case scala.util.Failure(exception) =>
          // Log failure and filter out failed parsers
          logWarning(
            s"Failed to instantiate parser ${config.name} with class ${config.className}: " +
              s"${exception.getMessage}", exception)
          None
      }
    }

  }
}

/**
 * Companion object for SQLPlanParserConfig providing factory methods and default configuration.
 */
object SQLPlanParserConfig extends Logging {
  /** Path to the default YAML configuration file in resources. */
  private val DEFAULT_CONFIG_PATH = "parser/sql-plan-parsers.yaml"

  /** Lazily loaded default configuration from resources. */
  val DEFAULT_CONFIG: SQLPlanParserConfig = loadFromResources()

  /** Pre-instantiated parsers from the default configuration. */
  val REGISTERED_PARSERS: Seq[SQLPlanParserTrait] = DEFAULT_CONFIG.getRegisteredParsers

  /**
   * Loads the SQL plan parser configuration from the default resource file.
   * Validates that at least one parser is configured.
   *
   * @return SQLPlanParserConfig loaded from default YAML file
   */
  @throws[IllegalStateException]
  @throws[IllegalArgumentException]
  def loadFromResources(): SQLPlanParserConfig = {
    val configStream = getClass.getClassLoader.getResourceAsStream(DEFAULT_CONFIG_PATH)
    if (configStream == null) {
      throw new IllegalStateException(
        s"Could not find parser configuration file: $DEFAULT_CONFIG_PATH")
    }
    try {
      // Read YAML content from resource stream
      val reader = new BufferedReader(new InputStreamReader(configStream))
      val content = Iterator.continually(reader.readLine()).takeWhile(_ != null).mkString("\n")
      val config = PropertiesLoader[SQLPlanParserConfig].loadFromContent(content).getOrElse(
        throw new IllegalStateException(
          s"Failed to load parser configuration from: $DEFAULT_CONFIG_PATH"))

      // Explicit validation after loading - ensure at least one parser is defined
      if (config.parsers == null || config.parsers.isEmpty) {
        throw new IllegalArgumentException("At least one parser configuration must be defined")
      }

      config
    } finally {
      configStream.close()
    }
  }

  /**
   * Loads the SQL plan parser configuration from a custom file path.
   *
   * @param filePath Path to the YAML configuration file
   * @return Option containing the loaded configuration, or None if loading fails
   */
  def loadFromFile(filePath: String): Option[SQLPlanParserConfig] = {
    PropertiesLoader[SQLPlanParserConfig].loadFromFile(filePath)
  }
}
