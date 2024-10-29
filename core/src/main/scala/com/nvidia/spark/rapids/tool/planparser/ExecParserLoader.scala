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

package com.nvidia.spark.rapids.tool.planparser

import java.nio.file.Paths

import scala.collection.mutable
import scala.reflect.runtime.universe._

import org.json4s._
import org.json4s.jackson.JsonMethods

import org.apache.spark.sql.rapids.tool.util.UTF8Source

object ReflectionCache {
  private val methodCache = mutable.Map[String, MethodSymbol]()

  def getMethodSymbol(classType: Type, methodName: String): MethodSymbol = {
    methodCache.getOrElseUpdate(methodName, {
      classType.decl(TermName(methodName)).asMethod
    })
  }
}

object ExecParserLoader {
  case class ParserConfig(
      execName: String,
      parseExpressions: Boolean,
      expressionParserMethod: Option[MethodSymbol],
      durationMetrics: Option[List[String]]
  )

  val EXEC_PARSER_DIRECTORY= "execParserMappings"
  val DEFAULT_EXEC_PARSER_FILE = "execParser.json"

  implicit val formats: Formats = DefaultFormats

  lazy val mirror = runtimeMirror(getClass.getClassLoader)
  lazy val sqlPlanParserModule = mirror.staticModule(
    "com.nvidia.spark.rapids.tool.planparser.SQLPlanParser")
  lazy val sqlPlanParserType = sqlPlanParserModule.moduleClass.asType.toType

  val mappingFile = Paths.get(EXEC_PARSER_DIRECTORY, DEFAULT_EXEC_PARSER_FILE).toString
  val jsonString = UTF8Source.fromResource(mappingFile).mkString
  val json = JsonMethods.parse(jsonString)
  val parsersList = (json \ "parsers").extract[List[Map[String, Any]]]

  lazy val parsersConfig: Map[String, ParserConfig] = {
    parsersList.map { parserMap =>
      val execName = parserMap("execName").asInstanceOf[String]
      val parseExpressions = parserMap.get(
        "parseExpressions").map(_.asInstanceOf[Boolean]).getOrElse(false)
      val expressionParserMethodName = parserMap.get(
        "expressionParserMethod").map(_.asInstanceOf[String])
      val expressionParserMethod = expressionParserMethodName.map { methodName =>
        ReflectionCache.getMethodSymbol(sqlPlanParserType, methodName)
      }
      val durationMetrics = parserMap.get("durationMetrics").map(_.asInstanceOf[List[String]])

      execName -> ParserConfig(
        execName,
        parseExpressions,
        expressionParserMethod,
        durationMetrics
      )
    }.toMap
  }

  def getConfig(execName: String): Option[ParserConfig] = parsersConfig.get(execName)
}
