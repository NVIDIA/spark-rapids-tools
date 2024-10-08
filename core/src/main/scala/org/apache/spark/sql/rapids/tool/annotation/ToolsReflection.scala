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

package org.apache.spark.sql.rapids.tool.annotation

import scala.annotation.StaticAnnotation
import scala.annotation.meta.{beanGetter, beanSetter, field, getter, param, setter}


/**
 * This code is mostly copied from org.apache.spark.annotation.Since
 * Reason is copied here because it is being private to Spark packages which makes it
 * inaccessible for Non-Spark packages.
 *
 * A Scala annotation that indicates entities that are used for reeflection in Tools to match
 * different Spark runtime APIs
 */
@param @field @getter @setter @beanGetter @beanSetter
class ToolsReflection(source: String, comment: String) extends StaticAnnotation
