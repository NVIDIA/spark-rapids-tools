/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.tool.tuning.plugins.emr

import com.nvidia.spark.rapids.tool.PlatformNames
import com.nvidia.spark.rapids.tool.plugins.ConditionTrait
import com.nvidia.spark.rapids.tool.tuning.AutoTuner
import com.nvidia.spark.rapids.tool.tuning.plugins.{BaseTuningRule, TuningCondPredicates}

/**
 * EMR Tuning Rules Module
 *
 * This module contains tuning rules specifically designed for optimizing Apache Spark
 * applications running on Amazon EMR.
 *
 * ==Rules Included==
 * This module defines two tuning rules for Transparent Huge Pages (THP):
 *
 * '''EmrThpDriverRule''': Manages THP settings for Spark driver
 * '''EmrThpExecutorRule''': Manages THP settings for Spark executors
 *
 * EMR >= 7.12 has THP enabled by default for Spark applications.
 * Disabling THP can improve performance and reduce execution time variance.
 *
 * The recommendation is to set both `spark.driver.extraJavaOptions` and
 * `spark.executor.extraJavaOptions` to `-XX:-UseTransparentHugePages` when running Spark on EMR.
 *
 *
 * ==Configuration Entries==
 * These rules reference the following tuning configuration entries:
 * - EMR_THP_DISABLE_FLAG: The JVM flag to disable THP
 * - EMR_THP_RECOMMENDATION_COMMENT: Comment explaining the THP recommendation
 *
 * ==References==
 * - Kernel documentation: https://docs.kernel.org/admin-guide/mm/transhuge.html
 *
 * @see [[com.nvidia.spark.rapids.tool.tuning.plugins.emr.EmrTuningPlugin]]
 * @see [[com.nvidia.spark.rapids.tool.tuning.plugins.BaseTuningRule]]
 */

/**
 * Base rule for disabling Transparent Huge Pages in Spark JVM options for EMR.
 */
abstract class BaseEmrThpRule extends BaseTuningRule {
  protected val javaOptionsProp: String
  protected val componentName: String

  /**
   * Checks if THP flag needs to be updated.
   */
  override val condition: ConditionTrait[AutoTuner] = (tunerInst: AutoTuner) => {
    if (!tunerInst.platform.platformName.startsWith(PlatformNames.EMR)) {
      // Apply this rule only on EMR platforms.
      false
    } else if (TuningCondPredicates.rawPropertyEnforced(tunerInst, javaOptionsProp)) {
      // Respect target-cluster enforced JVM options and do not override them.
      false
    } else {
      // Apply only when the THP disable flag is not already present.
      val disableFlag = tunerInst.configProvider.getEntry("EMR_THP_DISABLE_FLAG").default
      tunerInst.getPropertyValue(javaOptionsProp)
        .forall(value => !value.trim.split("\\s+").exists(_ == disableFlag))
    }
  }

  /**
   * Applies the THP flag and comment.
   */
  override def apply(tuner: AutoTuner): Unit = {
    val disableFlag = tuner.configProvider.getEntry("EMR_THP_DISABLE_FLAG").default
    tuner.appendRecommendation(javaOptionsProp, disableFlag)
    val comment = tuner.configProvider
      .getEntry("EMR_THP_RECOMMENDATION_COMMENT").default
      .replace("[COMPONENT]", componentName)
    tuner.appendComment(comment)
  }
}

/**
 * Rule to disable THP in Spark driver JVM options.
 */
class EmrThpDriverRule extends BaseEmrThpRule {
  override protected val javaOptionsProp: String = "spark.driver.extraJavaOptions"
  override protected val componentName: String = "driver"
}

/**
 * Rule to disable THP in Spark executor JVM options.
 */
class EmrThpExecutorRule extends BaseEmrThpRule {
  override protected val javaOptionsProp: String = "spark.executor.extraJavaOptions"
  override protected val componentName: String = "executor"
}
