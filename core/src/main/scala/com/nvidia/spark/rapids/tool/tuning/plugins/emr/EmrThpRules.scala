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
 * EMR THP (Transparent Huge Pages) Tuning Rules Module
 *
 * This module contains tuning rules specifically designed for optimizing Apache Spark applications
 * running on Amazon EMR by disabling Transparent Huge Pages (THP) in the JVM.
 *
 * ==Overview==
 * EMR >= 7.12 has THP enabled by default for Spark applications.
 * Disabling THP can improve performance and reduce execution time variance.
 *
 * The recommendation is to append `-XX:-UseTransparentHugePages` to both driver and executor
 * JVM options when running Spark on EMR.
 *
 * ==Rules Included==
 * This module defines two main tuning rules:
 *
 * 1. '''EmrThpDriverRule''': Manages THP settings for the Spark driver
 *    - Checks spark.driver.extraJavaOptions for THP configuration
 *    - Recommends disabling THP if it's enabled or not configured
 *    - Handles existing JVM options by appending to them
 *
 * 2. '''EmrThpExecutorRule''': Manages THP settings for Spark executors
 *    - Checks spark.executor.extraJavaOptions for THP configuration
 *    - Recommends disabling THP if it's enabled or not configured
 *    - Handles existing JVM options by appending to them
 *
 * ==Technical Notes==
 * - The `-XX:-UseTransparentHugePages` flag has been removed from modern JDK versions
 * - This flag should work with Java 11, which is commonly used on EMR
 * - On more modern JDKs, this flag will be obsolete/dropped (no harm if present)
 * - Enabling THP for Java uses a `+` sign: `-XX:+UseTransparentHugePages`
 * - Disabling THP uses a `-` sign: `-XX:-UseTransparentHugePages`
 *
 * ==Configuration Entries==
 * These rules reference the following tuning configuration entries:
 * - EMR_THP_DISABLE_FLAG: The JVM flag to disable THP
 * - EMR_THP_RECOMMENDATION_COMMENT: Comment explaining the THP recommendation
 *
 * ==References==
 * - Kernel documentation: https://docs.kernel.org/admin-guide/mm/transhuge.html
 *
 * @see [[com.nvidia.spark.rapids.tool.tuning.plugins.emr.EmrThpTuningPlugin]]
 * @see [[com.nvidia.spark.rapids.tool.tuning.plugins.BaseTuningRule]]
 */

/**
 * Base class for EMR THP tuning rules that handles common logic for both driver and executor.
 */
abstract class BaseEmrThpRule extends BaseTuningRule {
  protected val thpDisableFlag = "-XX:-UseTransparentHugePages"
  protected val thpEnableFlag = "-XX:+UseTransparentHugePages"

  protected def javaOptionsProp: String
  protected def componentName: String

  /**
   * Checks if THP flag needs to be updated.
   * Returns true if:
   * - Running on EMR platform
   * - Property is not enforced by user
   * - THP is not already disabled
   */
  override val condition: ConditionTrait[AutoTuner] = (tunerInst: AutoTuner) => {
    // Only apply on EMR platform
    if (!tunerInst.platform.platformName.startsWith(PlatformNames.EMR)) {
      false
    } else if (TuningCondPredicates.rawPropertyEnforced(tunerInst, javaOptionsProp)) {
      // Don't apply if property is user-enforced
      false
    } else {
      // Check if THP disable flag is not already present
      tunerInst.getPropertyValue(javaOptionsProp) match {
        case Some(value) =>
          // Don't apply if already has disable flag
          !value.contains(thpDisableFlag)
        case None =>
          // No existing value, should apply
          true
      }
    }
  }

  /**
   * Merges JVM options by adding the THP disable flag.
   * Removes any THP enable flag if present and handles deduplication.
   */
  private def mergeJvmOptions(existingOptions: String): String = {
    if (existingOptions == null || existingOptions.trim.isEmpty) {
      thpDisableFlag
    } else {
      // Split on whitespace, filter out empty strings, and deduplicate while preserving order
      val options = existingOptions.trim.split("\\s+").filter(_.nonEmpty).distinct.toBuffer

      // Remove THP enable flag if present
      options --= Seq(thpEnableFlag)

      // Add THP disable flag if not already present
      if (!options.contains(thpDisableFlag)) {
        options += thpDisableFlag
      }

      options.mkString(" ")
    }
  }

  /**
   * Applies the THP disable recommendation.
   */
  override def apply(tuner: AutoTuner): Unit = {
    val currentValue = tuner.getPropertyValue(javaOptionsProp).getOrElse("")
    val newValue = mergeJvmOptions(currentValue)

    tuner.appendRecommendation(javaOptionsProp, newValue)

    // Generate comment from config template
    val comment = tuner.configProvider
      .getEntry("EMR_THP_RECOMMENDATION_COMMENT").default
      .replace("[COMPONENT]", componentName)
    tuner.appendComment(comment)
  }
}

/**
 * Rule to disable Transparent Huge Pages in the Spark driver JVM options.
 */
class EmrThpDriverRule extends BaseEmrThpRule {
  override protected val javaOptionsProp = "spark.driver.extraJavaOptions"
  override protected val componentName = "driver"
}

/**
 * Rule to disable Transparent Huge Pages in Spark executor JVM options.
 */
class EmrThpExecutorRule extends BaseEmrThpRule {
  override protected val javaOptionsProp = "spark.executor.extraJavaOptions"
  override protected val componentName = "executor"
}
