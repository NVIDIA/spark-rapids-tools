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
 * A Scala annotation that specifies whether the type of duration: wallClockTime Vs. CPUTime
 */
@param @field @getter @setter @beanGetter @beanSetter
class WallClock extends StaticAnnotation
