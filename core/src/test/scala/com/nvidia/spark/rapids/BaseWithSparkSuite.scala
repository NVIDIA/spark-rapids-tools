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

package com.nvidia.spark.rapids

import org.apache.spark.sql.TrampolineUtil

/**
 * Base class containing cleanup resources used for Spark.
 */
class BaseWithSparkSuite extends BaseNoSparkSuite {
  // we don't need to cleanup spark session in the beforeEach because
  // this is already part of the trampoline helper
  override protected def afterEach(): Unit = {
    // close the spark session at the end of the test to avoid leaks
    TrampolineUtil.cleanupAnyExistingSession()
  }
}
