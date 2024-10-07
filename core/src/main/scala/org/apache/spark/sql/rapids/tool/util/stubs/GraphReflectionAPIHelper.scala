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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.util.stubs.bd.BDGraphReflectionAPI
import org.apache.spark.sql.rapids.tool.util.stubs.db.DBGraphReflectionAPI

/**
 * Helper object to load the correct runtime GraphReflectionAPI implementation.
 * This is used to improve performance by avoiding try-catch blocks on every single component.
 */
object GraphReflectionAPIHelper extends Logging {
  /**
   * Iterate through the available GraphReflectionAPI implementations and return the first one that
   * can be used to construct a SparkPlan Graph Node.
   * @return the first GraphReflectionAPI that is compatible with the current Spark runtime.
   */
  private def loadRuntimeGraphAPI(): Option[GraphReflectionAPI] = {
    // defines the available GraphReflectionAPI implementations
    val allAPIS = Map(
      "Default Graph API" -> new DefaultGraphReflectionAPI(),
      "BD Graph API" -> BDGraphReflectionAPI(),
      "DB Graph API" -> DBGraphReflectionAPI()
    )
    // Finds the first compatible API by creating a dummy node.
    val res = allAPIS.find { entry =>
      try {
        // Create a dummy node and captures the exception if the API is not compatible
        entry._2.constructNode(0, "node1", "descr", Seq.empty)
        true
      } catch {
        case _: Throwable => false
      }
    }
    // Log this information or show an error to be aware of incompatible runtimes.
    if (res.isDefined) {
      logInfo(s"Using runtime API [${res.get._1}] to Construct SparkPlan Graph")
    } else {
      logError("No runtime Graph API found. Falling to the spark runtime constructor")
    }
    res.map(_._2)
  }
  // caches the API to avoid re-creating it multiple times.
  lazy val api: Option[GraphReflectionAPI] = loadRuntimeGraphAPI()
}
