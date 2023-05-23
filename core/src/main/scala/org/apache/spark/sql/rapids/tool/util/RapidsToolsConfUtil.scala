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

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
 * Contains util methods to interact with Spark/Hadoop configurations.
 */
class RapidsToolsConfUtil extends Logging {
  val hadoopConf: Configuration = newHadoopConf

  def newHadoopConf: Configuration = {
    val hConf = SparkHadoopUtil.get.conf
    RapidsToolsConfUtil.appendRapidsToolsHadoopConfigs(hConf)
    hConf
  }
}


object RapidsToolsConfUtil extends Logging {
  val hadoopConfPrefix = s"${RAPIDS_TOOLS_SYS_PROP_PREFIX}hadoop."
  val sparkConfPrefix = s"${RAPIDS_TOOLS_SYS_PROP_PREFIX}spark."

  private lazy val configMap = loadConfFromSystemProperties
  private lazy val instance = new RapidsToolsConfUtil

  def get: RapidsToolsConfUtil = instance

  /**
   * Creates a sparkConfiguration object from the existing sparkSession if any.
   * Then it will call [[RapidsToolsConfUtil.newHadoopConf(SparkConf)]]
   * @return a hadoop configuration object
   */
  def newHadoopConf(): Configuration = {
    val sparkConf = SparkSession.getActiveSession match {
      case Some(spark) => spark.sparkContext.getConf
      case None => new SparkConf()
    }
    newHadoopConf(sparkConf)
  }

  /**
   * Returns a Configuration object with Spark configuration applied on top.
   * It also applies system properties with prefix "rapids.tools.hadoop" on top of all.
   *
   * @param conf a spark configuration instance
   * @return a hadoop configuration object
   */
  def newHadoopConf(conf: SparkConf): Configuration = {
    appendRapidsToolsSparkConfigs(conf)
    val hadoopConf = SparkHadoopUtil.newConfiguration(conf)
    appendRapidsToolsHadoopConfigs(hadoopConf)
    hadoopConf
  }

  private def appendRapidsToolsHadoopConfigs(hadoopConf: Configuration): Unit = {
    // Copy any "rapids.tools.hadoop.prop=val" system properties into hadoopConf as "prop=val"
    for ((key, value) <- configMap if key.startsWith(hadoopConfPrefix)) {
      val keyVal = key.substring(hadoopConfPrefix.length)
      hadoopConf.set(keyVal, value,
        s"Set by Rapids Tools from keys starting with '$hadoopConfPrefix'")
    }
  }

  private def appendRapidsToolsSparkConfigs(sparkConf: SparkConf): Unit = {
    // Copy any "rapids.tools.spark.prop=val" system properties into sparkConf as "spark.prop=val"
    for ((key, value) <- configMap if key.startsWith(sparkConfPrefix)) {
      val keyVal = key.substring(RAPIDS_TOOLS_SYS_PROP_PREFIX.length)
      sparkConf.set(keyVal, value)
    }
  }
}