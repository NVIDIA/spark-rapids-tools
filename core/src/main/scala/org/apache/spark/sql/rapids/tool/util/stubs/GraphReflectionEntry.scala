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

import scala.reflect.runtime.universe.{ClassSymbol, MethodSymbol, Mirror}

/**
 * A reflection entry for a class that can be instantiated at runtime.
 * @param mirror The mirror to use for reflection (created once per classloader)
 * @param className The name of the class to instantiate
 * @param paramsSize The number of parameters to the constructor, if known. Some Spark releases
 *                   like Databricks have different overrides that differ by count of arguments.
 * @tparam T The class to instantiate
 */
class GraphReflectionEntry[T](mirror: Mirror, className: String, paramsSize: Option[Int] = None) {
  // Get the class symbol
  private val classSymbol = mirror.staticClass(className)
  private val reflectiveClass = mirror.reflectClass(classSymbol)
  // Get the constructor method symbol
  val constr: MethodSymbol = createConstructor(classSymbol, paramsSize)

  // If the paramsCount is defined, we select the constructor that has parameters size equal to
  // that value
  private def createConstructor(symbol: ClassSymbol, paramsCount: Option[Int]): MethodSymbol = {
    paramsCount match {
      case None =>
        // return the primary constructor
        symbol.primaryConstructor.asMethod
      case Some(count) =>
        // return the constructor with given parameter size
        val constructors = symbol.info.decls.filter(_.isConstructor)
          .map(_.asMethod)
          .filter(_.paramLists.flatten.size == count)
        val constructor = constructors.headOption.getOrElse {
          throw new IllegalArgumentException(
            s"No constructor found with exactly $count parameters for class[$className]")
        }
        constructor
    }
  }

  def createInstanceFromList(args: List[_]): T = {
    reflectiveClass
      .reflectConstructor(constr)(args: _*)
      .asInstanceOf[T]
  }
}
