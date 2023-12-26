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

import java.io.{IOException, OutputStream}
import java.util.{Collections, Comparator, Enumeration, Map, Properties, Set, TreeSet}


/**
 * This is an implementation of Java Properties that stores the properties
 * into a file after sorting them by key.
 * Another approach would be load the properties into a hashMap. However, this
 * won't take into consideration serialization rules and comments.
 * This implementation works for Java8+.
 * See the following answer on StackOverflow:
 * https://stackoverflow.com/questions/10275862/how-to-sort-properties-in-java/55957344#55957344
 */
class SortedJProperties extends Properties {
  @throws[IOException]
  override def store(out: OutputStream, comments: String): Unit = {
    val sortedProps: Properties = new Properties() {
      override def entrySet: Set[Map.Entry[AnyRef, AnyRef]] = {
        /*
         * Using comparator to avoid the following exception on jdk >=9:
         * java.lang.ClassCastException: java.base/java.util.concurrent.ConcurrentHashMap$MapEntry
         * cannot be cast to java.base/java.lang.Comparable
         */
        val sortedSet: Set[Map.Entry[AnyRef, AnyRef]] =
          new TreeSet[Map.Entry[AnyRef, AnyRef]](
            new Comparator[Map.Entry[AnyRef, AnyRef]]() {
              override def compare(o1: Map.Entry[AnyRef, AnyRef],
                  o2: Map.Entry[AnyRef, AnyRef]): Int =
                o1.getKey.toString.compareTo(o2.getKey.toString)
            })
        sortedSet.addAll(super.entrySet)
        sortedSet
      }

      override def keySet: Set[AnyRef] = new TreeSet[AnyRef](super.keySet)

      override def keys: Enumeration[AnyRef] =
        Collections.enumeration(new TreeSet[AnyRef](super.keySet))
    }
    sortedProps.putAll(this)
    sortedProps.store(out, comments)
  }
}
