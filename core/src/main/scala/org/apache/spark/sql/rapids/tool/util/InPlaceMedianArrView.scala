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

package org.apache.spark.sql.rapids.tool.util

import scala.annotation.tailrec
import scala.language.postfixOps

/**
 * Allows for in-place partitioning and finding the median.
 * The tools used to find the median of a sequence by sorting the entire sequence, then returning
 * the elements in the middle. As we started to capture all the accumulators in Spark plans,
 * sorting is inefficient for large eventlogs that contain huge number of tasks and
 * Accumulables. Thus, this class is an optimized version to get the median in a linear
 * complexity while doing it in place to avoid allocating new array to store the sorted elements.
 * The code is copied from a Stackoverflow thread:
 * https://stackoverflow.com/questions/4662292/scala-median-implementation
 *
 * Notes:
 * - The implementation assumes that the array is not empty.
 */
case class InPlaceMedianArrView(arr: Array[Long], from: Int, until: Int) {
  def apply(n: Int): Long = {
    if (from + n < until) {
      arr(from + n)
    } else {
      throw new ArrayIndexOutOfBoundsException(n)
    }
  }

  /**
   * Returns a new view of the array with the same elements but a different range.
   * @param p a predicate to apply on the elements to proceed with the partitioning.
   * @return a tuple of 2 views, the first one contains the elements that satisfy the predicate,
   *         and the second one contains the rest.
   */
  def partitionInPlace(p: Long => Boolean): (InPlaceMedianArrView, InPlaceMedianArrView) = {
    var upper = until - 1
    var lower = from
    while (lower < upper) {
      while (lower < until && p(arr(lower))) lower += 1
      while (upper >= from && !p(arr(upper))) upper -= 1
      if (lower < upper) { val tmp = arr(lower); arr(lower) = arr(upper); arr(upper) = tmp }
    }
    (copy(until = lower), copy(from = lower))
  }

  def size: Int = {
    until - from
  }

  def isEmpty: Boolean = {
    size <= 0
  }

  override def toString = {
    arr mkString ("ArraySize(", ", ", ")")
  }
}

/**
 * Companion object for InPlaceMedianArrView.
 */
object InPlaceMedianArrView {

  def apply(arr: Array[Long]): InPlaceMedianArrView = {
    InPlaceMedianArrView(arr, 0, arr.size)
  }

  /**
   * Finds the median of the array in place.
   * @param arr the Array[Long] to be processed
   * @param k the index of the median
   * @param choosePivot a function to choose the pivot index. This useful to choose different
   *                    strategies. For example, choosing the midpoint works better for sorted
   *                    arrays.
   * @return the median of the array.
   */
  @tailrec
  def findKMedianInPlace(arr: InPlaceMedianArrView, k: Int)
    (implicit choosePivot: InPlaceMedianArrView => Long): Long = {
    val a = choosePivot(arr)
    val (s, b) = arr partitionInPlace (a >)
    if (s.size == k) {
      a
    } else if (s.isEmpty) {
      val (s, b) = arr partitionInPlace (a ==)
      if (s.size > k) {
        a
      } else {
        findKMedianInPlace(b, k - s.size)
      }
    } else if (s.size < k) {
      findKMedianInPlace(b, k - s.size)
    } else {
      findKMedianInPlace(s, k)
    }
  }

  /**
   * Choose a random pivot in the array. This can lead to worst case for sorted arrays.
   * @param arr the array to choose the pivot from.
   * @return a random element from the array.
   */
  def chooseRandomPivotInPlace(arr: InPlaceMedianArrView): Long = {
    arr(scala.util.Random.nextInt(arr.size))
  }

  /**
   * Choose the element in the middle as a pivot. This works better to find median of sorted arrays.
   * @param arr the array to choose the pivot from.
   * @return the element in the middle of the array.
   */
  def chooseMidpointPivotInPlace(arr: InPlaceMedianArrView): Long = {
    arr((arr.size - 1) / 2)
  }

  /**
   * Finds the median of the array in place.
   * @param arr the Array[Long] to be processed.
   * @param choosePivot a function to choose the pivot index.
   * @return the median of the array.
   */
  def findMedianInPlace(
    arr: Array[Long])(implicit choosePivot: InPlaceMedianArrView => Long): Long = {
    val midIndex = (arr.size - 1) / 2
    if (arr.size % 2 == 0) {
      // For even-length arrays, find the two middle elements and compute their average
      val mid1 = findKMedianInPlace(InPlaceMedianArrView(arr), midIndex)
      val mid2 = findKMedianInPlace(InPlaceMedianArrView(arr), midIndex + 1)
      (mid1 + mid2) / 2
    } else {
      // For odd-length arrays, return the middle element
      findKMedianInPlace(InPlaceMedianArrView(arr), midIndex)
    }
  }
}
