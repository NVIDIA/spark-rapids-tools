/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.tool.tuning

import org.apache.spark.internal.Logging

/**
 * A wrapper to the hold the tuning entry information.
 * @param name the name of the property
 * @param originalValue the value from the eventlog
 * @param tunedValue the value recommended by the AutoTuner
 * @param definition the definition of the tuning entry.
 */
class TuningEntry(
    override val name: String,
    override var originalValue: Option[String],
    override var tunedValue: Option[String],
    definition: Option[TuningEntryDefinition] = None) extends TuningEntryTrait {

  /**
   * Set the original value from the default value in Spark if it exists.
   * This is needed because some properties may not be set relying on the default value defined by
   * Spark configurations.
   */
  override def setOriginalValueFromDefaultSpark(): Unit = {
    originalValue match {
      case Some(_) => // Do Nothing
      case None =>
        definition match {
          case Some(defn) =>
            if (defn.hasDefaultSpark()) {
              originalValue = Some(defn.defaultSpark)
            }
          case None =>  // Do Nothing
        }
    }
  }

  override def isBootstrap(): Boolean = {
    definition match {
      case Some(defn) => defn.isBootstrap()
      case None => name.startsWith("spark.rapids.")
    }
  }

  override def isEnabled(): Boolean = {
    val globalFlag = definition match {
      case Some(defn) => defn.isEnabled()
      case None => true
    }
    globalFlag && enabled
  }

  /////////////////////////
  // Initialization Code //
  /////////////////////////

  setOriginalValueFromDefaultSpark()
}

object TuningEntry extends Logging {
  /**
   * Build a TuningEntry object and automatically pull the information from Tuning Entry Table.
   * @param name the property label
   * @param originalValue the original value from the eventlog
   * @param tunedValue the value recommended by the AutoTuner
   * @return a TuningEntry object
   */
  def build(
      name: String,
      originalValue: Option[String],
      tunedValue: Option[String]): TuningEntry = {
    // pul the information from Tuning Entry Table
    val tuningDefinition = TuningEntryDefinition.TUNING_TABLE.get(name)
    // for debugging purpose
    if (tuningDefinition.isEmpty) {
      logInfo("Tuning Entry is not defined for " + name)
    }
    new TuningEntry(name, originalValue, tunedValue, tuningDefinition)
  }
}
