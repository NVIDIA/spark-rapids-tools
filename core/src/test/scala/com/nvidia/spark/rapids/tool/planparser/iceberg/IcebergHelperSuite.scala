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

package com.nvidia.spark.rapids.tool.planparser.iceberg

import com.nvidia.spark.rapids.BaseNoSparkSuite

/**
 * Pure unit tests for the runtime/config gates on `IcebergHelper`. These do not
 * spin up Spark and only exercise the helper logic that drives PR 2093's
 * AppendData / ReplaceData support flips.
 *
 * The behaviors locked in here mirror the assumptions documented in the PR:
 *   - Spark version regex restricted to 3.5.x and 4.0.x families.
 *   - Default-on RAPIDS Iceberg toggles fail only when explicitly disabled
 *     (case-insensitive "false").
 *   - Parquet field-ID write toggle treated the same way.
 *   - Catalog allowlist limited to "hadoop" today; missing catalog defaults true.
 */
class IcebergHelperSuite extends BaseNoSparkSuite {

  // ---------------- Spark version gate ----------------

  test("isSparkVersionSupported accepts Spark 3.5.x and 4.0.x families") {
    Seq("3.5.0", "3.5.1", "3.5.7", "3.5.10",
        "4.0.0", "4.0.2", "4.0.10").foreach { v =>
      assert(IcebergHelper.isSparkVersionSupported(v),
        s"expected $v to be a supported Spark version")
    }
  }

  test("isSparkVersionSupported accepts Spark builds with vendor suffix") {
    // Real eventlogs often carry suffixes like '3.5.0-amzn-1' or '3.5.1-databricks-1'.
    // The regex tolerates any trailing characters after the patch number.
    Seq("3.5.0-amzn-1", "3.5.6-databricks-1", "4.0.0-SNAPSHOT").foreach { v =>
      assert(IcebergHelper.isSparkVersionSupported(v),
        s"expected vendor-suffixed $v to be supported")
    }
  }

  test("isSparkVersionSupported rejects older and newer Spark families") {
    Seq("3.2.0", "3.3.4", "3.4.0", "3.4.5",
        "4.1.0", "4.1.1", "5.0.0").foreach { v =>
      assert(!IcebergHelper.isSparkVersionSupported(v),
        s"expected $v to be rejected as unsupported")
    }
  }

  test("isSparkVersionSupported is optimistic for empty or null versions") {
    // The unit test EventLogs sometimes lack a sparkVersion. Stay optimistic so
    // we don't regress existing test paths that don't set it.
    assert(IcebergHelper.isSparkVersionSupported(""),
      "empty version string should be treated as unknown / allowed")
    assert(IcebergHelper.isSparkVersionSupported(null.asInstanceOf[String]),
      "null version should be treated as unknown / allowed")
  }

  // ---------------- Default-on RAPIDS Iceberg toggles ----------------

  test("isIcebergFormatEnabled defaults to true when the key is absent") {
    assert(IcebergHelper.isIcebergFormatEnabled(Map.empty[String, String]))
  }

  test("isIcebergFormatEnabled returns false only when explicitly disabled") {
    val disabled = Map(IcebergHelper.CONF_ICEBERG_ENABLED -> "false")
    val disabledMixedCase = Map(IcebergHelper.CONF_ICEBERG_ENABLED -> "FALSE")
    val enabled = Map(IcebergHelper.CONF_ICEBERG_ENABLED -> "true")
    val unknownValue = Map(IcebergHelper.CONF_ICEBERG_ENABLED -> "maybe")
    assert(!IcebergHelper.isIcebergFormatEnabled(disabled))
    assert(!IcebergHelper.isIcebergFormatEnabled(disabledMixedCase))
    assert(IcebergHelper.isIcebergFormatEnabled(enabled))
    // Unknown / garbage values fall through to the "not explicitly false" branch.
    assert(IcebergHelper.isIcebergFormatEnabled(unknownValue))
  }

  test("isIcebergWriteEnabled mirrors the format toggle behavior") {
    assert(IcebergHelper.isIcebergWriteEnabled(Map.empty[String, String]))
    assert(!IcebergHelper.isIcebergWriteEnabled(
      Map(IcebergHelper.CONF_ICEBERG_WRITE_ENABLED -> "false")))
    assert(IcebergHelper.isIcebergWriteEnabled(
      Map(IcebergHelper.CONF_ICEBERG_WRITE_ENABLED -> "true")))
  }

  test("isParquetFieldIdWriteEnabled mirrors the toggle behavior") {
    // Spark's own default for this config is "false", but Iceberg forces it on
    // for its own writes. We deliberately only fail when the eventlog records
    // an explicit "false".
    assert(IcebergHelper.isParquetFieldIdWriteEnabled(Map.empty[String, String]))
    assert(!IcebergHelper.isParquetFieldIdWriteEnabled(
      Map(IcebergHelper.CONF_PARQUET_FIELD_ID_WRITE -> "false")))
    assert(IcebergHelper.isParquetFieldIdWriteEnabled(
      Map(IcebergHelper.CONF_PARQUET_FIELD_ID_WRITE -> "true")))
  }

  // ---------------- Catalog allowlist ----------------
  //
  // NOTE on the catalog gate:
  //
  // The shared `SPARK_CATALOG_REGEX` in `EventUtils` is currently
  //   """spark\\.sql\\.catalog\\.([A-Za-z][A-Za-z0-9_-]*)$"""
  // — i.e. the triple-quoted string contains literal `\\.` pairs, which the
  // regex engine reads as "backslash followed by any character". Real
  // `spark.sql.catalog.<name>` keys have no backslash, so the regex never
  // matches, `getCatalogType` always returns None, and
  // `isSparkCatalogSupported` defaults to true regardless of the configured
  // catalog `.type`. The "hadoop-only" comment on `SUPPORTED_CATALOGS` is
  // therefore aspirational today.
  //
  // The follow-up "broaden catalog allowlist" PR will replace this with a
  // class-name match (Iceberg `BaseCatalog` subclasses) and fix the regex.
  // The tests below pin down the *current* observable behavior so that fix is
  // a deliberate, reviewed change rather than an accidental drift.

  test("isSparkCatalogSupported is effectively always true with the current regex bug") {
    val hadoop = Map(
      "spark.sql.catalog.local" -> "org.apache.iceberg.spark.SparkCatalog",
      "spark.sql.catalog.local.type" -> "hadoop")
    val hive = Map(
      "spark.sql.catalog.local" -> "org.apache.iceberg.spark.SparkCatalog",
      "spark.sql.catalog.local.type" -> "hive")
    val rest = Map(
      "spark.sql.catalog.rest" -> "org.apache.iceberg.spark.SparkCatalog",
      "spark.sql.catalog.rest.type" -> "rest")
    // All three currently pass the gate because `SPARK_CATALOG_REGEX` never
    // matches real keys. When the catalog allowlist follow-up lands and fixes
    // the regex, this test will need to be split into the per-type assertions.
    assert(IcebergHelper.isSparkCatalogSupported(hadoop))
    assert(IcebergHelper.isSparkCatalogSupported(hive))
    assert(IcebergHelper.isSparkCatalogSupported(rest))
  }

  test("isSparkCatalogSupported defaults to true when no catalog.type is set") {
    // Independent of the regex bug, the explicit fallback path also defaults to
    // true so we don't reject eventlogs that lack a catalog.type subkey.
    val noTypeKey = Map("spark.sql.catalog.local" -> "org.apache.iceberg.spark.SparkCatalog")
    val emptyProps = Map.empty[String, String]
    assert(IcebergHelper.isSparkCatalogSupported(noTypeKey))
    assert(IcebergHelper.isSparkCatalogSupported(emptyProps))
  }
}
