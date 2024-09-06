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

package com.nvidia.spark.rapids.tool.qualification

class AppSubscriber(val appId: String) {
  val lock = new Object()
  private var attemptID: Option[Int] = None

  def unsafeSetAttemptId(newAttempt: Int): Boolean = {
    attemptID match {
      case Some(a) =>
        if (newAttempt > a) {
          attemptID = Some(newAttempt)
        }
      case None => attemptID = Some(newAttempt)
    }
    newAttempt == attemptID.get
  }

  def safeSetAttemptId(newAttempt: Int): Boolean = {
    lock.synchronized {
      unsafeSetAttemptId(newAttempt)
    }
  }
}

object AppSubscriber {
  private val APP_SUBSCRIBERS = new java.util.concurrent.ConcurrentHashMap[String, AppSubscriber]()

  def getOrCreate(appId: String): AppSubscriber = {
    APP_SUBSCRIBERS.computeIfAbsent(appId, _ => new AppSubscriber(appId))
  }

  def subscribeAppAttempt(appId: String, newAttemptId: Int): Boolean = {
    val subscriber = getOrCreate(appId)
    subscriber.safeSetAttemptId(newAttemptId)
  }

  def withSafeValidAttempt[T](appId: String, currAttempt: Int)(f: () => T): Option[T] = {
    val subscriber = getOrCreate(appId)
    subscriber.lock.synchronized {
      if (subscriber.unsafeSetAttemptId(currAttempt)) {
        Option(f())
      } else {
        None
      }
    }
  }

  def withUnsafeValidAttempt[T](appId: String, currAttempt: Int)(f: () => T): Option[T] = {
    val subscriber = getOrCreate(appId)
    if (subscriber.unsafeSetAttemptId(currAttempt)) {
      Option(f())
    } else {
      None
    }
  }
}
