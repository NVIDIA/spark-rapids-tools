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

/**
 * A trait that defines the behavior of the Tuning Entry.
 */
trait TuningEntryTrait {
  val name: String
  // The value recommended by the AutoTuner
  private var _tunedValue: Option[String] = None
  // Define getter and setter for the tunedValue
  protected def tunedValue: Option[String] = _tunedValue
  protected def tunedValue_=(value: Option[String]): Unit = {
    // Values are normalized before being set.
    _tunedValue = value.map(normalizeValue)
  }

  // The original value of the property from the event log
  private var _originalValue: Option[String] = None
  // Define getter and setter for the originalValue
  protected def originalValue: Option[String] = _originalValue
  protected def originalValue_=(value: Option[String]): Unit = {
    // Values are normalized before being set.
    _originalValue = value.map(normalizeValue)
  }

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

  def setOriginalValueFromDefaultSpark(): Unit

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
      // It is possible the property was not tuned. However, we should not be in that case
      // because by calling commit we must have copied the tuned from the original.
      val finalTunedValue = tunedValue.map(formatOutput)
      val finalOriginalValue = originalValue.map(formatOutput)
      finalTunedValue.orElse(fillIfBlank).orElse(finalOriginalValue)
        .getOrElse("[UNDEFINED]")
    }
  }

  /**
   * Returns the original value as a string.
   * @return the value of the property as a string.
   */
  def getOriginalValue: Option[String] = {
    originalValue.map(formatOutput)
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
   * Returns a normalized representation of a property value, useful for consistent comparison.
   *
   * This base implementation returns the property value as-is. Subclasses can override this method
   * to apply specific normalization logic.
   * @return the uniform representation of property.
   */
  def normalizeValue(propValue: String): String = {
    propValue
  }

  /**
   * Returns a formatted representation of a property value, useful for consistent output.
   *
   * This base implementation returns the property value as-is. Subclasses can override this method
   * to apply specific formatting logic.
   * @return the formatted representation of property.
   */
  def formatOutput(propValue: String): String = {
    propValue
  }

  def setTuningOpType(opType: TuningOpType): Unit = {
    tuningOpType = opType
  }

  /**
   * Updates the tuning operation type based on the original and tuned values.
   */
  def updateOpType(): Unit = {
    if (!(isRemoved() || isUnresolved())) {
      (originalValue, tunedValue) match {
        case (None, None) => setTuningOpType(TuningOpTypes.UNKNOWN)
        case (Some(orig), Some(rec)) =>
          if (orig != rec) {
            setTuningOpType(TuningOpTypes.UPDATE)
          } else {
            setTuningOpType(TuningOpTypes.CLONE)
          }
        case (None, Some(_)) => setTuningOpType(TuningOpTypes.ADD)
        case (Some(orig), None) =>
          // It is possible that the property is not set by the AutoTuner, then it means it should
          // be copied from the original configuration.
          setRecommendedValue(orig)
      }
    }
  }

  def commit(): Unit = {
    updateOpType()
  }
}
