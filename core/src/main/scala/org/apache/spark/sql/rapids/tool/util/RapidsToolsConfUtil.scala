/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids.tool.util

import org.apache.hadoop.conf.Configuration

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
 * Contains util methods to interact with Spark/Hadoop configurations.
 */
object RapidsToolsConfUtil extends Logging {
  private val RAPIDS_TOOLS_HADOOP_CONF_PREFIX = s"${RAPIDS_TOOLS_SYS_PROP_PREFIX}hadoop."

  /**
   * Creates a sparkConfiguration object with system properties applied on-top.
   * @return a hadoop configuration object
   */
  def newHadoopConf(): Configuration = {
    newHadoopConf(loadConfFromSystemProperties)
  }

  /**
   * Creates a new Configuration object after applying the properties on top.
   * The configuration map keys should be prefixed with "rapids.tools.hadoop."
   * @param confMap a key value pair of properties to be applied to the default configurations.
   * @return a hadoop configuration object
   */
  def newHadoopConf(confMap: Map[String, String]): Configuration = {
    // Note that we do not want to use SparkHadoopConfUtil.get.newConfiguration because
    // spark applies spark configurations on top of the hadoop configs.
    // A use-case is when the runtime is running as a Java command (not spark),
    // then the default spark configurations override the actual hadoop configurations.
    // For more details, see https://github.com/NVIDIA/spark-rapids-tools/issues/350
    val hadoopConf = SparkSession.getActiveSession match {
      case Some(spark) =>
        // get the hadoop configuration attached to the session
        new Configuration(spark.sparkContext.hadoopConfiguration)
      case _ =>
        new Configuration()
    }
    // append the configuration map
    appendRapidsToolsHadoopConfigs(confMap, hadoopConf)
    hadoopConf
  }

  /**
   * Appends rapids.tools.hadoop.* configurations from a Map to another without
   * the rapids.tools.hadoop. prefix.
   */
  def appendRapidsToolsHadoopConfigs(
      srcMap: Map[String, String],
      destMap: Configuration): Unit = {
    // Copy any "rapids.tools.hadoop.foo=bar" system properties into destMap as "foo=bar"
    for ((key, value) <- srcMap if key.startsWith(RAPIDS_TOOLS_HADOOP_CONF_PREFIX)) {
      val k = key.substring(RAPIDS_TOOLS_HADOOP_CONF_PREFIX.length)
      destMap.set(k, value)
    }
  }
}
