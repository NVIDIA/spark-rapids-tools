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

import scala.collection.mutable.ListBuffer

import com.nvidia.spark.rapids.tool.tuning.TuningOpTypes.TuningOpType

import org.apache.spark.sql.rapids.tool.util.StringUtils

/**
 * A trait that defines the behavior of the Tuning Entry.
 */
trait TuningEntryTrait {
  val name: String
  // The value recommended by the AutoTuner
  var tunedValue: Option[String]
  // The original value of the property from the event log
  val originalValue: Option[String]
  var enabled: Boolean = true

  // The type of tuning operation to be performed
  var tuningOpType: TuningOpType = TuningOpTypes.UNKNOWN
  // Comments specific to the property
  val comments: ListBuffer[String] = ListBuffer[String]()
  // The value to be used when the property is unresolved.
  var fillUnresolved: Option[String] = Some("[FILL_IN_VALUE]")

  def toConfString: String = {
    "--conf %s=%s".format(name, getTuneValue())
  }

  /**
   * Indicates if the property is resolved by the AutoTuner or not. This is used to distinguish
   * the properties that were not tuned due to a failure in the AutoTuner. For example,
   * shuffleManager was not able to find the className but we still want to include this
   * property in the bootstrap configuration.
   * @param fill the value to be used to fill in the gap if necessary.
   */
  def markAsUnresolved(fill: Option[String] = None): Unit = {
    setTuningOpType(TuningOpTypes.UNRESOLVED)
    if (fill.isDefined) {
      fillUnresolved = fill
    }
  }

  /**
   * Disables the property. This is used to turnoff the property if it is not applicable.
   */
  def disable(): Unit = {
    enabled = false
  }

  /**
   * Returns the value of the property as a string.
   * When the tunedValue is not set, it will set it as [Fill_IN_VALUE] so that it can be replaced
   * by the user. This is used because the AutoTuner may not be able to successfully make
   * recommendations. Yet, we want to include that in the final tuning report.
   * Note that the name is not "getTunedValue" because the purpose is different.
   *
   * @param fillIfBlank the value of the content if the TunedValue is empty
   * @return the value of the property as a string.
   */
  def getTuneValue(fillIfBlank: Option[String] = None): String = {
    if (isUnresolved()) {
      fillIfBlank.getOrElse(fillUnresolved.get)
    } else {
      // It is possible the the propery was not tuned. However, we should not be in that case
      // because by calling commit we must have copied the tuned from the original.
      tunedValue.getOrElse(fillIfBlank.getOrElse(originalValue.getOrElse("[UNDEFINED]")))
    }
  }

  /**
   * Indicates that a specific configuration should be removed from the configuration.
   */
  def markAsRemoved(): Unit = {
    tunedValue = None
    setTuningOpType(TuningOpTypes.REMOVE)
  }

  def setRecommendedValue(value: String): Unit = {
    tunedValue = Option(value)
    updateOpType()
  }

  /**
   * Indicates if the property is tuned. This is used to filter out the entries that stayed the
   * same.
   * @return true if it was changed by the AutoTuner or false otherwise.
   */
  def isTuned(): Boolean = {
    isEnabled() && TuningOpTypes.isTuned(tuningOpType)
  }

  def isUnresolved(): Boolean = {
    tuningOpType == TuningOpTypes.UNRESOLVED
  }

  /**
   * Indicates if the property is removed by the AutoTuner
   */
  def isRemoved(): Boolean = {
    tuningOpType == TuningOpTypes.REMOVE
  }

  /**
   * Indicates if the property is a bootstrap property.
   * A bootstrap property is a property that is required to be set by the AutoTuner
   */
  def isBootstrap(): Boolean

  /**
   * Indicates if the property is enabled.
   */
  def isEnabled(): Boolean

  /**
   * Used to compare between two properties by converting memory units to
   * a equivalent representations.
   * @param propValue property to be processed.
   * @return the uniform representation of property.
   *         For Memory, the value is converted to bytes.
   */
  private def getRawValue(propValue: Option[String]): Option[String] = {
    propValue match {
      case None => None
      case Some(value) =>
        if (StringUtils.isMemorySize(value)) {
          // if it is memory return the bytes unit
          Some(s"${StringUtils.convertMemorySizeToBytes(value)}")
        } else {
          propValue
        }
    }
  }

  def setTuningOpType(opType: TuningOpType): Unit = {
    tuningOpType = opType
  }

  /**
   * Updates the tuning operation type based on the original and tuned values.
   */
  def updateOpType(): Unit = {
    if (!(isRemoved() || isUnresolved())) {
      val originalVal = getRawValue(originalValue)
      val recommendedVal = getRawValue(tunedValue)
      (originalVal, recommendedVal) match {
        case (None, None) => setTuningOpType(TuningOpTypes.UNKNOWN)
        case (Some(orig), Some(rec)) =>
          if (orig != rec) {
            setTuningOpType(TuningOpTypes.UPDATE)
          } else {
            setTuningOpType(TuningOpTypes.CLONE)
          }
        case (None, Some(_)) => setTuningOpType(TuningOpTypes.ADD)
        case (Some(orig), None) =>
          // It is possible that the property was not set bu the AutoTuner, then it means it should
          // be copied from the original configuration.
          setRecommendedValue(orig)
      }
    }
  }

  def commit(): Unit = {
    updateOpType()
  }
}
