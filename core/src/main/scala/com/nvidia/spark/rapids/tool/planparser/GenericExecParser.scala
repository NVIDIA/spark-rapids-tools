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

import scala.reflect.runtime.universe._

import com.nvidia.spark.rapids.tool.qualification.PluginTypeChecker

import org.apache.spark.sql.execution.ui.SparkPlanGraphNode
import org.apache.spark.sql.rapids.tool.{AppBase, UnsupportedExpr}

class GenericExecParser(
    val node: SparkPlanGraphNode,
    val checker: PluginTypeChecker,
    val sqlID: Long,
    val app: Option[AppBase] = None
) extends ExecParser {

  val fullExecName: String = node.name + "Exec"
  val parserConfig: Option[ExecParserLoader.ParserConfig] =
    ExecParserLoader.getConfig(node.name)
  // Cache to store reflected methods to avoid redundant reflection calls
  private val methodCache = scala.collection.mutable.Map[String, MethodMirror]()

  override def parse: ExecInfo = {
    val duration = computeDuration
    val expressions = parseExpressions()
    val notSupportedExprs = getNotSupportedExprs(expressions)
    val isExecSupported = checker.isExecSupported(fullExecName) &&
      notSupportedExprs.isEmpty &&
      isSupportedByDefault

    val (speedupFactor, isSupported) = if (isExecSupported) {
      (checker.getSpeedupFactor(fullExecName), true)
    } else {
      (1.0, false)
    }

    createExecInfo(speedupFactor, isSupported, duration, notSupportedExprs)
  }

  protected def parseExpressions(): Array[String] = {
    parserConfig match {
      case Some(config) if config.parseExpressions =>
        val exprString = getExprString
        val methodSymbol = config.expressionParserMethod.getOrElse {
          throw new IllegalArgumentException(
            s"Expression parser method not specified for ${node.name}")
        }
        invokeCachedParserMethod(methodSymbol, exprString)

      case _ => Array.empty[String] // Default behavior when parsing is not required
    }
  }

  // Helper method to invoke the parser method with caching
  private def invokeCachedParserMethod(
      methodSymbol: MethodSymbol, exprString: String): Array[String] = {
    // This is to  check if the method is already cached, else reflect and cache it
    val cachedMethod = methodCache.getOrElseUpdate(methodSymbol.fullName, {
      val mirror = runtimeMirror(getClass.getClassLoader)
      val module = mirror.reflectModule(mirror.staticModule(
        "com.nvidia.spark.rapids.tool.planparser.SQLPlanParser"))
      val instanceMirror = mirror.reflect(module.instance)
      instanceMirror.reflectMethod(methodSymbol) // Cache this reflected method
    })

    cachedMethod(exprString) match {
      case expressions: Array[String] => expressions
      case _ => throw new IllegalArgumentException(
        s"Unexpected return type from method: ${methodSymbol.name}")
    }
  }

  protected def getExprString: String = {
    node.desc.replaceFirst(s"${node.name} ", "")
  }

  protected def getNotSupportedExprs(expressions: Array[String]): Seq[UnsupportedExpr] = {
    checker.getNotSupportedExprs(expressions)
  }

  // Compute duration based on the node metrics of that ExecNode
  protected def computeDuration: Option[Long] = {
    None
  }

  protected def isSupportedByDefault: Boolean = true

  protected def createExecInfo(
      speedupFactor: Double,
      isSupported: Boolean,
      duration: Option[Long],
      notSupportedExprs: Seq[UnsupportedExpr]
  ): ExecInfo = {
    ExecInfo(
      node,
      sqlID,
      node.name,
      "",
      speedupFactor,
      duration,
      node.id,
      isSupported,
      None,
      unsupportedExprs = notSupportedExprs
    )
  }
}
